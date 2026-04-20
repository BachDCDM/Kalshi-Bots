"""Tests for settlement ticker → strategy_id classification (sports multivariate, etc.)."""

from __future__ import annotations

import sys
from pathlib import Path

_CP = Path(__file__).resolve().parents[1] / "control-panel"
if str(_CP) not in sys.path:
    sys.path.insert(0, str(_CP))

from settlement_sync import classify_settlement_ticker


def test_sports_multivariate_nba_classifies_sports_vol() -> None:
    strategies: list[dict] = [{"id": "vol_surface", "settlement_prefixes": []}]
    t = "KXMVENBAGAME-25APR01NYKBOS-SOMELEG"
    assert classify_settlement_ticker(t, strategies) == "sports_vol"


def test_sports_multivariate_nhl_classifies_sports_vol() -> None:
    strategies: list[dict] = []
    assert classify_settlement_ticker("KXMVENHLGAME-FOO", strategies) == "sports_vol"


def test_catalog_mlb_game_prefix_classifies_sports_vol() -> None:
    strategies: list[dict] = []
    assert classify_settlement_ticker("KXMLBGAME-26APR18NYYBOS-SOMESTRIKE", strategies) == "sports_vol"


def test_non_game_series_stays_unassigned() -> None:
    strategies: list[dict] = []
    assert classify_settlement_ticker("KXNBADRAFT-26-ROUND1", strategies) == "unassigned"


def test_head_fallback_mlb_shape_classifies_sports_vol() -> None:
    """Series head maps to a league via ``sport_from_series_ticker`` + ``GAME`` in full ticker."""
    strategies: list[dict] = []
    assert classify_settlement_ticker("KXMLBCUSTOM-26APR18-GAME", strategies) == "sports_vol"


def test_vol_surface_weather_unchanged() -> None:
    strategies: list[dict] = []
    assert classify_settlement_ticker("KXHIGHNY-25APR01-T72", strategies) == "vol_surface"


def test_btc_hourly_unchanged() -> None:
    strategies: list[dict] = []
    assert classify_settlement_ticker("KXBTC-25APR0115-B39500", strategies) == "vol_surface"


def test_yaml_prefix_wins_over_default_sports() -> None:
    """First matching strategy in yaml order owns the ticker."""
    strategies = [
        {"id": "vol_surface", "settlement_prefixes": ["KXMVENBAGAME"]},
        {"id": "sports_vol", "settlement_prefixes": []},
    ]
    assert classify_settlement_ticker("KXMVENBAGAME-EVENT-LEG", strategies) == "vol_surface"


def test_kalshi_result_from_trade_note() -> None:
    _ROOT = Path(__file__).resolve().parents[1]
    if str(_ROOT) not in sys.path:
        sys.path.insert(0, str(_ROOT))
    from vol_surface_strategy.panel_snapshot import _kalshi_settlement_result_from_note

    note = "Kalshi settlement net_pnl_cents=42; market_result=no"
    assert _kalshi_settlement_result_from_note(note) == "no"
    assert _kalshi_settlement_result_from_note("no market_result here") is None

