"""Sports gate2 (book-only) and multi-team ladder sharding via custom_strike."""

from __future__ import annotations

from types import SimpleNamespace

from vol_surface_strategy.analysis import _passes_gate2_liquidity
from vol_surface_strategy.config import SPORTS_GATE2_MIN_BOOK_SZ
from vol_surface_strategy.sports_analysis import _filter_sports_surface_contracts
from vol_surface_strategy.sports_discovery import group_markets_into_surface_ladders, sport_ladder_shard_from_custom_strike


def _c(
    *,
    vol: float = 0,
    mid: float = 50.0,
    yb: float = 45.0,
    ya: float = 50.0,
    yb_fp: float = 100.0,
    ya_fp: float = 100.0,
    ticker: str = "T",
) -> SimpleNamespace:
    return SimpleNamespace(
        ticker=ticker,
        volume_fp=vol,
        mid_cents=mid,
        yes_bid_cents=yb,
        yes_ask_cents=ya,
        yes_bid_size_fp=yb_fp,
        yes_ask_size_fp=ya_fp,
    )


def test_gate2_sports_passes_on_book_only_low_volume() -> None:
    ok, mid_vol, vol_c, book_c = _passes_gate2_liquidity(
        [_c(vol=0, yb_fp=600, ya_fp=600)], is_weather=False, is_sports=True
    )
    assert ok
    assert vol_c == 0.0
    assert mid_vol == 0.0
    assert book_c == 1200.0


def test_gate2_sports_fails_below_book_threshold() -> None:
    ok, _, _, book_c = _passes_gate2_liquidity(
        [_c(vol=0, yb_fp=200, ya_fp=200)], is_weather=False, is_sports=True
    )
    assert not ok
    assert book_c == 400.0 < SPORTS_GATE2_MIN_BOOK_SZ


def test_gate2_crypto_still_needs_volume_or_book_5k() -> None:
    ok, _, _, _ = _passes_gate2_liquidity(
        [_c(vol=0, yb_fp=600, ya_fp=600)], is_weather=False, is_sports=False
    )
    assert not ok


def test_shard_from_baseball_team_uuid() -> None:
    m = SimpleNamespace(custom_strike={"baseball_team": "uuid-abc"})
    assert sport_ladder_shard_from_custom_strike(m) == "baseball_team:uuid-abc"


def test_shard_from_json_string() -> None:
    m = SimpleNamespace(custom_strike='{"basketball_team": "uuid-x"}')
    assert sport_ladder_shard_from_custom_strike(m) == "basketball_team:uuid-x"


def test_group_ladders_splits_two_teams_same_series() -> None:
    et, st = "KXMLBSPREAD-26APR172140TORAZ", "KXMLBSPREAD"
    m1 = SimpleNamespace(
        event_ticker=et,
        series_ticker=st,
        custom_strike={"baseball_team": "team-a"},
    )
    m2 = SimpleNamespace(
        event_ticker=et,
        series_ticker=st,
        custom_strike={"baseball_team": "team-b"},
    )
    g = group_markets_into_surface_ladders([m1, m2])
    assert len(g) == 2
    assert g[(et, st, "baseball_team:team-a")] == [m1]
    assert g[(et, st, "baseball_team:team-b")] == [m2]


def test_sports_surface_drops_wide_spread_after_mid_band() -> None:
    tight = _c(ticker="A", mid=50.0, yb=46.0, ya=50.0)  # 4¢
    wide = _c(ticker="B", mid=50.0, yb=40.0, ya=55.0)  # 15¢
    out = _filter_sports_surface_contracts([tight, wide])
    assert [c.ticker for c in out] == ["A"]


def test_group_ladders_single_bucket_when_no_custom_strike() -> None:
    et, st = "KXMVENBAGAME-26APR18LALDEN", "KXMVENBAGAME"
    m = SimpleNamespace(event_ticker=et, series_ticker=st, custom_strike=None)
    g = group_markets_into_surface_ladders([m])
    assert g == {(et, st, ""): [m]}
