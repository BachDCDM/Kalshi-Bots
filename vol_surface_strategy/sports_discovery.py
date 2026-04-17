"""Sports game market fetch + ladder grouping (one surface per Kalshi ``series_ticker`` per event)."""

from __future__ import annotations

import json
import logging
import os
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from vol_surface_strategy.discovery import parse_market_resolve_utc
from vol_surface_strategy.kalshi_io import get_markets_page_raw, get_multivariate_events_page_raw
from vol_surface_strategy.sports_model import (
    SportCode,
    parse_sport_code,
    sport_from_market,
    sport_from_series_ticker,
)
from vol_surface_strategy.sports_windows import earliest_sports_game_start_utc

LOG = logging.getLogger("vol_surface")

# Kalshi ``custom_strike`` team discriminator (spread / team totals share strikes across teams).
_CUSTOM_STRIKE_TEAM_KEYS: tuple[str, ...] = (
    "baseball_team",
    "basketball_team",
    "hockey_team",
    "soccer_team",
    "football_team",
)


def sport_ladder_shard_from_custom_strike(m: Any) -> str:
    """
    Stable shard id for multi-team ladders. Kalshi encodes the team in ``custom_strike``
    (e.g. ``{"baseball_team": "<uuid>"}``). Returns ``""`` when absent — single ladder.
    """
    cs = getattr(m, "custom_strike", None)
    d: dict[str, Any]
    if isinstance(cs, dict):
        d = cs
    elif isinstance(cs, str) and cs.strip():
        try:
            parsed = json.loads(cs)
            d = parsed if isinstance(parsed, dict) else {}
        except (json.JSONDecodeError, TypeError):
            d = {}
    else:
        inner = getattr(cs, "__dict__", None)
        d = inner if isinstance(inner, dict) else {}
    for k in _CUSTOM_STRIKE_TEAM_KEYS:
        v = d.get(k)
        if v is not None and str(v).strip():
            return f"{k}:{str(v).strip()}"
    for k, v in d.items():
        if isinstance(k, str) and k.endswith("_team") and v is not None and str(v).strip():
            return f"{k}:{str(v).strip()}"
    return ""


def _env_scan_debug() -> bool:
    return (os.environ.get("VOL_SPORTS_SCAN_DEBUG") or "").strip().lower() in ("1", "true", "yes", "on")


def autoscan_multivariate_series_tickers() -> list[str]:
    """
    Series tickers to scan via ``GET /events/multivariate`` when ``VOL_SPORTS_EVENTS`` is unset.

    Override with comma-separated ``VOL_SPORTS_AUTOSCAN_SERIES`` (discover game events only;
    Kalshi excludes multivariate events from plain ``get_events``).
    """
    raw = (os.environ.get("VOL_SPORTS_AUTOSCAN_SERIES") or "").strip()
    if raw:
        return [x.strip() for x in raw.split(",") if x.strip()]
    # Defaults: Kalshi multivariate game ladders (tune if catalog changes).
    return [
        "KXMVENBAGAME",
        "KXMVENHLGAME",
        "KXMVEMLBGAME",
        "KXMVEMLSGAME",
    ]


def _emit_scan_diag(lines: list[str]) -> None:
    """Mirror to stdout + vol_surface log so terminal scans stay readable."""
    for ln in lines:
        print(ln, flush=True)
        LOG.info("%s", ln)


def discover_sports_games_via_open_markets_catalog(
    client: Any, *, scan_debug: bool = False
) -> list[tuple[str, SportCode, list[Any]]]:
    """
    Fallback when multivariate series return no rows: page **open** markets in a close-time window,
    keep markets whose ``series_ticker`` maps to NBA/NHL/MLB/MLS, group by ``event_ticker``.

    Bounded by ``VOL_SPORTS_CATALOG_MAX_PAGES`` (default 50), ``VOL_SPORTS_CATALOG_HOURS`` (default 168),
    and ``VOL_SPORTS_CATALOG_MAX_GAMES`` (default 120) after sorting by earliest market resolve.
    """
    try:
        max_pages = int((os.environ.get("VOL_SPORTS_CATALOG_MAX_PAGES") or "50").strip() or "50")
    except ValueError:
        max_pages = 50
    max_pages = max(1, min(max_pages, 200))
    try:
        horizon_h = int((os.environ.get("VOL_SPORTS_CATALOG_HOURS") or "168").strip() or "168")
    except ValueError:
        horizon_h = 168
    try:
        max_games = int((os.environ.get("VOL_SPORTS_CATALOG_MAX_GAMES") or "120").strip() or "120")
    except ValueError:
        max_games = 120
    max_games = max(1, min(max_games, 500))

    now = datetime.now(timezone.utc)
    min_ts = int(now.timestamp())
    max_ts = int((now + timedelta(hours=horizon_h)).timestamp())

    by_event: dict[str, list[Any]] = defaultdict(list)
    event_sport: dict[str, SportCode] = {}
    tick_seen: dict[str, set[str]] = defaultdict(set)

    # Diagnostics (why 0 sport events?)
    n_rows = 0
    n_missing_et_or_st = 0
    n_has_et_st = 0
    n_hit_series = 0
    n_hit_event = 0
    n_hit_blob = 0
    n_nfl = 0
    n_none = 0
    series_freq_all = Counter()
    series_freq_no_match = Counter()
    sample_no_match: list[str] = []
    sample_nfl: list[str] = []

    cursor: Optional[str] = None
    total_mk = 0
    pages_scanned = 0
    for page_i in range(max_pages):
        pages_scanned = page_i + 1
        try:
            mk, cursor = get_markets_page_raw(
                client,
                limit=200,
                cursor=cursor,
                min_close_ts=min_ts,
                max_close_ts=max_ts,
            )
        except Exception as e:
            LOG.warning("autoscan(catalog): get_markets page failed: %s", e)
            break
        total_mk += len(mk)
        for m in mk:
            n_rows += 1
            et = str(getattr(m, "event_ticker", "") or "").strip()
            st = str(getattr(m, "series_ticker", "") or "").strip()
            tk = str(getattr(m, "ticker", "") or "").strip()
            if st:
                series_freq_all[st] += 1
            if not et or not st:
                n_missing_et_or_st += 1
                continue
            n_has_et_st += 1
            sp_s = sport_from_series_ticker(st)
            sp_e = sport_from_series_ticker(et) if not sp_s else None
            sp_b = sport_from_market(m) if not (sp_s or sp_e) else None
            if sp_s:
                n_hit_series += 1
            elif sp_e:
                n_hit_event += 1
            elif sp_b:
                n_hit_blob += 1
            sp = sp_s or sp_e or sp_b
            if sp == "NFL":
                n_nfl += 1
                if len(sample_nfl) < (25 if scan_debug else 8):
                    sample_nfl.append(
                        f"  nfl  ticker={tk!r} series={st!r} ev={et!r} "
                        f"title={str(getattr(m, 'title', ''))[:90]!r}"
                    )
                continue
            if sp is None:
                n_none += 1
                series_freq_no_match[st] += 1
                lim = 60 if scan_debug else 20
                if len(sample_no_match) < lim:
                    tit = str(getattr(m, "title", "") or "")[:100]
                    sub = str(getattr(m, "subtitle", "") or "")[:100]
                    cat = str(getattr(m, "category", "") or "")[:60]
                    sample_no_match.append(
                        f"  none ticker={tk!r} series={st!r} ev={et!r} cat={cat!r}\n"
                        f"       title={tit!r}\n"
                        f"       sub={sub!r}"
                    )
                continue
            if tk and tk in tick_seen[et]:
                continue
            if tk:
                tick_seen[et].add(tk)
            if et not in event_sport:
                event_sport[et] = sp
            by_event[et].append(m)
        if not cursor:
            break

    def earliest_rank_time(et: str, mk_list: list[Any]) -> datetime:
        sp = event_sport.get(et)
        if sp:
            u0 = earliest_sports_game_start_utc(mk_list, sp)
            if u0 is not None:
                return u0
        best: Optional[datetime] = None
        for m in mk_list:
            u = parse_market_resolve_utc(m)
            if u is None:
                continue
            if best is None or u < best:
                best = u
        return best if best is not None else now + timedelta(days=365)

    ranked = sorted(by_event.items(), key=lambda kv: earliest_rank_time(kv[0], kv[1]))
    out: list[tuple[str, SportCode, list[Any]]] = []
    for et, mk_list in ranked[:max_games]:
        sp = event_sport.get(et)
        if sp is None or not mk_list:
            continue
        out.append((et, sp, mk_list))

    LOG.info(
        "autoscan(catalog): scanned %d market rows across %d page(s), %d sport event(s) (cap %d)",
        total_mk,
        pages_scanned,
        len(out),
        max_games,
    )

    diag_always = not out
    if scan_debug or diag_always:
        lines: list[str] = [
            "",
            "=" * 88,
            "CATALOG SCAN DIAGNOSTICS (open markets in close-time window)",
            "=" * 88,
            f"  min_close_ts={min_ts} max_close_ts={max_ts}  horizon_h={horizon_h}  max_pages={max_pages}",
            f"  market_rows_read={total_mk}  pages_fetched={pages_scanned}",
            f"  rows_total={n_rows}  rows_missing_event_or_series={n_missing_et_or_st}  "
            f"rows_with_event_and_series={n_has_et_st}",
            f"  classified_via_series_ticker={n_hit_series}  via_event_ticker={n_hit_event}  "
            f"via_title_blob={n_hit_blob}",
            f"  rows_no_league_match={n_none}  rows_NFL_excluded={n_nfl}",
            f"  distinct_event_tickers_matched={len(by_event)}  games_returned={len(out)}  "
            f"max_games_cap={max_games}",
            f"  unique_series_ticker_values_seen={len(series_freq_all)}",
        ]
        top_n = 50 if scan_debug else 20
        lines.append(f"  top {top_n} series_ticker (all rows with that series):")
        for stc, cnt in series_freq_all.most_common(top_n):
            lines.append(f"    {cnt:5d}  {stc!r}")
        if series_freq_no_match:
            lines.append(f"  top {top_n} series_ticker among NO-league-match rows:")
            for stc, cnt in series_freq_no_match.most_common(top_n):
                lines.append(f"    {cnt:5d}  {stc!r}")
        if sample_no_match:
            cap = len(sample_no_match) if scan_debug else min(12, len(sample_no_match))
            lines.append(f"  sample rows with event+series but no NBA/NHL/MLB/MLS match (n={cap}):")
            lines.extend(sample_no_match[:cap])
        if sample_nfl:
            lines.append("  sample NFL-excluded rows:")
            lines.extend(sample_nfl[: (15 if scan_debug else 6)])
        lines.append("=" * 88)
        _emit_scan_diag(lines)

    return out


def discover_sports_games_with_markets(
    client: Any, *, scan_debug: bool = False
) -> list[tuple[str, SportCode, list[Any]]]:
    """
    Paginate open multivariate events per configured series; return one row per game with market list.

    Skips NFL. Uses nested markets when present, else ``GET /markets?event_ticker=…``.
    """
    if (os.environ.get("VOL_SPORTS_NO_AUTOSCAN") or "").strip().lower() in ("1", "true", "yes", "on"):
        LOG.info("autoscan disabled (VOL_SPORTS_NO_AUTOSCAN)")
        return []

    try:
        max_pages = int((os.environ.get("VOL_SPORTS_AUTOSCAN_MAX_PAGES") or "25").strip() or "25")
    except ValueError:
        max_pages = 25
    max_pages = max(1, min(max_pages, 200))

    seen: set[str] = set()
    out: list[tuple[str, SportCode, list[Any]]] = []
    scan_debug = bool(scan_debug) or _env_scan_debug()
    mv_sample_emitted = False

    for series in autoscan_multivariate_series_tickers():
        sport_hint = sport_from_series_ticker(series)
        if sport_hint == "NFL":
            LOG.debug("autoscan: skip NFL series %r", series)
            continue
        cursor: Optional[str] = None
        pages = 0
        while pages < max_pages:
            pages += 1
            try:
                rows, cursor = get_multivariate_events_page_raw(
                    client,
                    limit=200,
                    cursor=cursor or None,
                    series_ticker=series,
                    with_nested_markets=True,
                )
            except Exception as e:
                LOG.warning("autoscan: multivariate fetch series=%s failed: %s", series, e)
                break
            LOG.info(
                "autoscan: series=%r page=%d raw_events=%d cursor=%s",
                series,
                pages,
                len(rows),
                "yes" if cursor else "no",
            )
            if scan_debug and rows and not mv_sample_emitted:
                mv_sample_emitted = True
                ev0 = rows[0]
                keys = sorted(k for k in vars(ev0).keys() if not str(k).startswith("_"))[:35]
                tit = str(getattr(ev0, "title", "") or "")[:120]
                _emit_scan_diag(
                    [
                        "",
                        "[multivariate] first raw event sample (scan-debug):",
                        f"  keys={keys}",
                        f"  event_ticker={getattr(ev0, 'event_ticker', '')!r}  series_ticker={getattr(ev0, 'series_ticker', '')!r}",
                        f"  title={tit!r}",
                    ]
                )
            for ev in rows:
                et = str(getattr(ev, "event_ticker", "") or "").strip()
                if not et or et in seen:
                    continue
                st_ev = str(getattr(ev, "series_ticker", "") or series).strip()
                mk0 = list(getattr(ev, "markets", None) or [])
                sp = (
                    sport_from_series_ticker(st_ev)
                    or sport_hint
                    or sport_from_series_ticker(et)
                    or sport_from_market(ev)
                    or (sport_from_market(mk0[0]) if mk0 else None)
                )
                if sp is None or sp == "NFL":
                    continue
                mk = mk0
                if not mk:
                    mk = fetch_markets_for_event(client, et)
                if not mk:
                    continue
                seen.add(et)
                out.append((et, sp, mk))
            if not cursor:
                break

    LOG.info("autoscan: multivariate path produced %d game(s)", len(out))
    if not out:
        LOG.info("autoscan: multivariate empty — falling back to open-markets catalog scan")
        out = discover_sports_games_via_open_markets_catalog(client, scan_debug=scan_debug)
    return out


def iter_sports_game_targets(
    client: Any, *, scan_debug: bool = False
) -> list[tuple[str, SportCode, list[Any]]]:
    """
    Games to process: explicit ``VOL_SPORTS_EVENTS`` (with fetch), else multivariate autoscan.

    Returns ``(event_ticker, sport, markets)`` for each game.
    """
    explicit = parse_vol_sports_events_env()
    if explicit:
        return [(et, sp, fetch_markets_for_event(client, et)) for et, sp in explicit]
    return discover_sports_games_with_markets(client, scan_debug=scan_debug)


def fetch_markets_for_event(client: Any, event_ticker: str) -> list[Any]:
    """All open markets for a single ``event_ticker`` (paginated)."""
    et = (event_ticker or "").strip()
    if not et:
        return []
    out: list[Any] = []
    cursor: Optional[str] = None
    try:
        while True:
            page, cursor = get_markets_page_raw(
                client,
                limit=200,
                cursor=cursor,
                event_ticker=et,
            )
            out.extend(page)
            if not cursor:
                break
    except Exception as e:
        LOG.warning("get_markets event_ticker=%s failed: %s", et, e)
        return []
    return out


def group_markets_into_surface_ladders(markets: list[Any]) -> dict[tuple[str, str, str], list[Any]]:
    """
    Each ``(event_ticker, series_ticker, ladder_shard)`` is an independent vol surface.

    Kalshi uses distinct ``series_ticker`` values for different product types (game total vs
    team total, etc.). Within one series, spread / team-total events may repeat the same
    numeric strikes for **both** teams; ``ladder_shard`` comes from ``custom_strike.*_team``
    so each team's rungs form their own monotone ladder.
    """
    preliminary: dict[tuple[str, str], list[Any]] = defaultdict(list)
    for m in markets:
        et = str(getattr(m, "event_ticker", "") or "").strip()
        st = str(getattr(m, "series_ticker", "") or "").strip()
        if not et or not st:
            continue
        preliminary[(et, st)].append(m)

    out: dict[tuple[str, str, str], list[Any]] = {}
    for (et, st), mlist in preliminary.items():
        by_shard: dict[str, list[Any]] = defaultdict(list)
        for m in mlist:
            by_shard[sport_ladder_shard_from_custom_strike(m)].append(m)
        for shard, sub in by_shard.items():
            if sub:
                out[(et, st, shard)] = sub
    return out


def parse_vol_sports_events_env() -> list[tuple[str, SportCode]]:
    """
    ``VOL_SPORTS_EVENTS`` — comma-separated ``EVENT_TICKER:LEAGUE`` (league MLB|NBA|NHL|MLS).

    NFL lines are ignored if listed. Optional ``|ISO8601`` game start override per row. Example::

        VOL_SPORTS_EVENTS=KXMVENBAGAME-26APR18LALDEN:NBA,KXMLBGAME-26APR18NYYBOS:MLB
    """
    raw = (os.environ.get("VOL_SPORTS_EVENTS") or "").strip()
    if not raw:
        return []
    out: list[tuple[str, SportCode]] = []
    for chunk in raw.split(","):
        part = chunk.strip()
        if not part:
            continue
        main = part.split("|", 1)[0].strip()
        if ":" not in main:
            continue
        ev, sp = main.rsplit(":", 1)
        ev = ev.strip()
        code = parse_sport_code(sp.strip())
        if not ev or code is None:
            continue
        if code == "NFL":
            continue
        out.append((ev, code))
    return out


def parse_event_overrides_with_start() -> dict[str, str]:
    """
    Optional per-event scheduled start ISO after ``|``::

        KXMV-...:NBA|2026-04-18T19:30:00-04:00
    """
    raw = (os.environ.get("VOL_SPORTS_EVENTS") or "").strip()
    out: dict[str, str] = {}
    for chunk in raw.split(","):
        part = chunk.strip()
        if "|" not in part:
            continue
        main, iso = part.split("|", 1)
        main = main.strip()
        iso = iso.strip()
        if ":" not in main or not iso:
            continue
        ev, sp = main.rsplit(":", 1)
        ev = ev.strip()
        if parse_sport_code(sp.strip()) is None:
            continue
        out[ev] = iso
    return out
