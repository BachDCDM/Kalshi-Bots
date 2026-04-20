"""Aggregate vol-surface bot state for the control panel (read-only)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional, cast

from zoneinfo import ZoneInfo

from vol_surface_strategy.config import CITIES
from vol_surface_strategy.discovery import resolution_date_for_high_scan, resolution_date_for_low_scan
from vol_surface_strategy.panel_state import PANEL_DB_PATH, init_panel_db, sum_pnl_by_market_type, sum_realized_pnl_cents
from vol_surface_strategy.sports_model import SportCode, sport_from_series_ticker
from vol_surface_strategy.tracker import btc_key, get_row, list_all_rows, weather_key
from vol_surface_strategy.trading_windows import (
    btc_in_trading_window,
    btc_order_expiration_ts,
    btc_should_monitor,
    minutes_until_btc_hour_end,
    minutes_until_weather_high_end,
    minutes_until_weather_low_end,
    normalize_tracker_status,
    resting_order_expiration_ts,
    weather_high_in_window,
    weather_high_should_monitor,
    weather_low_in_window,
    weather_low_should_monitor,
    WEATHER_HIGH_WINDOWS,
    WEATHER_LOW_WINDOW,
)

_REPO_ROOT = Path(__file__).resolve().parents[1]


def _parse_key_parts(key: str) -> tuple[str, Optional[str], Optional[str]]:
    """Returns (coarse_type, city_id, HIGH|LOW)."""
    if key.startswith("btc:"):
        return "btc_hourly", None, None
    if key.startswith("w:"):
        parts = key.split(":")
        if len(parts) >= 4:
            hil = parts[2].upper()
            return (f"weather_{hil.lower()}", parts[1], hil)
    return "unknown", None, None


def _weather_window_bounds(city_id: str, kind: str) -> tuple[tuple[int, int], tuple[int, int]]:
    if kind == "HIGH":
        w = WEATHER_HIGH_WINDOWS[city_id]
        return (w[0].hour, w[0].minute), (w[1].hour, w[1].minute)
    lo = WEATHER_LOW_WINDOW[0]
    hi = WEATHER_LOW_WINDOW[1]
    return (lo.hour, lo.minute), (hi.hour, hi.minute)


def _describe_market_window(
    key: str,
    market_type: str,
    city_id: Optional[str],
    hi_lo: Optional[str],
    now_utc: datetime,
) -> dict[str, Any]:
    utc = now_utc.astimezone(timezone.utc)
    if market_type == "btc_hourly":
        in_w = btc_in_trading_window(utc)
        t_rem = minutes_until_btc_hour_end(utc)
        exp = resting_order_expiration_ts(
            utc, latest_allowed_ts=btc_order_expiration_ts(utc)
        )
        next_open = None
        if not in_w and utc.minute < 5:
            next_open = "Opens :05 UTC this hour"
        elif not in_w:
            nhr = utc.replace(minute=5, second=0, microsecond=0) + timedelta(hours=1)
            if utc.minute >= 45:
                nhr = utc.replace(minute=5, second=0, microsecond=0) + timedelta(hours=1)
            next_open = f"Next window starts {nhr.isoformat()} UTC (approx.)"
        return {
            "kind": "btc_hourly",
            "in_trading_window": in_w,
            "monitor_tick": btc_should_monitor(utc),
            "minutes_to_hour_end": round(t_rem, 1),
            "order_expiration_ts": exp,
            "window_hint": "UTC :05–:40 scan; limits ~4m TTL (or :45 if sooner)"
            + (" · active now" if in_w else ""),
            "next_window_hint": next_open or ("Active" if in_w else ""),
        }

    if city_id and city_id in CITIES and hi_lo in ("HIGH", "LOW"):
        tz = ZoneInfo(CITIES[city_id].tz_name)
        loc = now_utc.astimezone(tz)
        start, end = _weather_window_bounds(city_id, hi_lo)
        if hi_lo == "HIGH":
            in_w = weather_high_in_window(city_id, loc)
            t_rem = minutes_until_weather_high_end(city_id, loc)
        else:
            in_w = weather_low_in_window(loc)
            t_rem = minutes_until_weather_low_end(loc)
        sh, sm = start
        eh, em = end
        if hi_lo == "HIGH":
            mon = weather_high_should_monitor(city_id, loc)
        else:
            mon = weather_low_should_monitor(loc)
        return {
            "kind": "weather",
            "city_id": city_id,
            "high_low": hi_lo,
            "timezone": CITIES[city_id].tz_name,
            "in_trading_window": in_w,
            "monitor_tick": mon,
            "minutes_to_window_end": round(t_rem, 1),
            "local_window": f"{sh:02d}:{sm:02d}–{eh:02d}:{em:02d} local",
            "window_hint": ("Inside trading window" if in_w else "Outside trading window"),
        }

    return {"kind": "unknown", "in_trading_window": False, "monitor_tick": False, "window_hint": ""}


def _read_last_scan_row(market_key: str) -> Optional[dict[str, Any]]:
    init_panel_db()
    import sqlite3

    if not PANEL_DB_PATH.is_file():
        return None
    try:
        conn = sqlite3.connect(str(PANEL_DB_PATH), timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            r = conn.execute("SELECT * FROM last_scan WHERE market_key = ?", (market_key,)).fetchone()
            if not r:
                return None
            return {k: r[k] for k in r.keys()}
        finally:
            conn.close()
    except sqlite3.Error:
        return None


def _read_recent_orders(limit: int = 80) -> list[dict[str, Any]]:
    init_panel_db()
    import sqlite3

    if not PANEL_DB_PATH.is_file():
        return []
    try:
        conn = sqlite3.connect(str(PANEL_DB_PATH), timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.execute(
                f"SELECT * FROM order_events ORDER BY id DESC LIMIT {max(1, min(200, limit))}"
            )
            out = []
            for r in cur.fetchall():
                out.append({k: r[k] for k in r.keys()})
            return out
        finally:
            conn.close()
    except sqlite3.Error:
        return []


def _ledger_db_path(repo_root: Any) -> Path:
    base = Path(repo_root).resolve() if repo_root is not None else _REPO_ROOT
    return (base / "control-panel" / "data" / "settlement_ledger.db").resolve()


def _enrich_trade_outcomes_with_settlement_ledger(
    rows: list[dict[str, Any]], *, repo_root: Any = None
) -> list[dict[str, Any]]:
    """
    Attach latest ``kalshi_settlements`` row per ticker so sports rows show net P&amp;L / outcome
    even when ``trade_outcomes`` was not updated (ticker mismatch or sync order).
    """
    ledger_p = _ledger_db_path(repo_root)
    if not ledger_p.is_file():
        return rows
    import sqlite3

    try:
        conn = sqlite3.connect(str(ledger_p), timeout=10)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error:
        return rows
    try:
        for d in rows:
            if str(d.get("market_type") or "") != "sports_vol_surface":
                continue
            tk = str(d.get("ticker") or "").strip()
            if not tk:
                continue
            r = conn.execute(
                """
                SELECT net_pnl_cents, revenue_cents, market_result, settled_time_iso, strategy_id
                FROM kalshi_settlements
                WHERE UPPER(TRIM(ticker)) = UPPER(TRIM(?))
                ORDER BY settled_time_iso DESC
                LIMIT 1
                """,
                (tk,),
            ).fetchone()
            if not r:
                continue
            d["ledger_net_pnl_cents"] = r["net_pnl_cents"]
            d["ledger_revenue_cents"] = r["revenue_cents"]
            d["ledger_market_result"] = r["market_result"]
            d["ledger_settled_time_iso"] = r["settled_time_iso"]
            d["ledger_strategy_id"] = r["strategy_id"]
    finally:
        conn.close()
    return rows


def _kalshi_settlement_result_from_note(note: Any) -> Optional[str]:
    """Parse ``market_result=`` from ``trade_outcomes.note`` (written at Kalshi settlement sync)."""
    if not note or not isinstance(note, str):
        return None
    key = "market_result="
    if key not in note:
        return None
    fragment = note.split(key, 1)[1].strip()
    if ";" in fragment:
        fragment = fragment.split(";", 1)[0].strip()
    return fragment or None


def _read_trade_outcomes(limit: int = 100) -> list[dict[str, Any]]:
    init_panel_db()
    import sqlite3

    if not PANEL_DB_PATH.is_file():
        return []
    try:
        conn = sqlite3.connect(str(PANEL_DB_PATH), timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.execute(
                f"SELECT * FROM trade_outcomes ORDER BY id DESC LIMIT {max(1, min(300, limit))}"
            )
            out: list[dict[str, Any]] = []
            for r in cur.fetchall():
                d = {k: r[k] for k in r.keys()}
                res = _kalshi_settlement_result_from_note(d.get("note"))
                if res is not None:
                    d["settlement_market_result"] = res
                out.append(d)
            return out
        finally:
            conn.close()
    except sqlite3.Error:
        return []


def _last_scan_summary(scan: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if not scan:
        return None
    gl = scan.get("gate_log_json") or "[]"
    try:
        glp = json.loads(gl) if isinstance(gl, str) else gl
    except json.JSONDecodeError:
        glp = []
    return {
        "updated_utc": scan.get("updated_utc"),
        "action": scan.get("action"),
        "reason": scan.get("reason"),
        "edge_cents": scan.get("edge_cents"),
        "trade_recommended": scan.get("action") == "trade",
        "outlier_ticker": scan.get("outlier_ticker"),
        "side": scan.get("side"),
        "entry_cents": scan.get("entry_cents"),
        "contracts": scan.get("contracts"),
        "sigma_star": scan.get("sigma_star"),
        "underlying": scan.get("underlying"),
        "gate_log_tail": (glp[-5:] if isinstance(glp, list) else []),
    }


def _order_status_from_tracker(row: Optional[Any]) -> str:
    """UI bucket: none | open | active_position."""
    if row is None:
        return "none"
    st = normalize_tracker_status(row.status)
    if st == "order_resting":
        return "open"
    if st == "position_active":
        return "active_position"
    return "none"


def _enumerate_all_market_specs(now_utc: datetime) -> list[tuple[str, str, str]]:
    """Current-hour BTC plus every city HIGH/LOW for today's resolution dates (local per city)."""
    out: list[tuple[str, str, str]] = []
    u = now_utc.astimezone(timezone.utc)
    hour_start = u.replace(minute=0, second=0, microsecond=0)
    out.append(
        (
            btc_key(hour_start),
            f"BTC hourly · {hour_start.strftime('%Y-%m-%d %H:00')} UTC",
            "btc_hourly",
        )
    )
    for city_id in sorted(CITIES.keys()):
        tz = ZoneInfo(CITIES[city_id].tz_name)
        loc = now_utc.astimezone(tz)
        dh = resolution_date_for_high_scan(loc)
        dl = resolution_date_for_low_scan(loc)
        kh = weather_key(city_id, "HIGH", dh)
        kl = weather_key(city_id, "LOW", dl)
        out.append((kh, f"{city_id} HIGH · {dh.isoformat()}", "weather_high"))
        out.append((kl, f"{city_id} LOW · {dl.isoformat()}", "weather_low"))
    return out


def _build_market_row(
    key: str,
    label: str,
    default_market_type: str,
    now_utc: datetime,
) -> dict[str, Any]:
    row = get_row(key)
    mt, cid, hil = _parse_key_parts(key)
    mt_eff = (row.market_type if row and row.market_type else None) or default_market_type
    st = normalize_tracker_status(row.status) if row else None
    win = _describe_market_window(key, mt_eff, cid, hil, now_utc)
    scan = _read_last_scan_row(key)
    return {
        "key": key,
        "label": label,
        "order_status": _order_status_from_tracker(row),
        "status": st,
        "market_type": row.market_type if row else default_market_type,
        "raw_status": row.status if row else None,
        "ticker": row.ticker if row else None,
        "side": row.side if row else None,
        "contracts": row.contracts if row else None,
        "entry_cents": row.entry_cents if row else None,
        "order_id": row.order_id if row else None,
        "city_id": row.city_id if row else cid,
        "resolution_date": row.resolution_date if row else None,
        "hour_start_utc": row.hour_start_utc if row else None,
        "window": win,
        "last_scan": _last_scan_summary(scan),
    }


def _parse_sports_tracker_key(key: str) -> tuple[str, str, str]:
    """``s:{event_ticker}:{series_ticker}`` or ``…#{ladder_shard}``."""
    if not key.startswith("s:"):
        return "", "", ""
    tail = key[2:]
    idx = tail.find(":")
    if idx < 0:
        return "", "", ""
    et = tail[:idx]
    rest = tail[idx + 1 :]
    if "#" in rest:
        st, shard = rest.split("#", 1)
    else:
        st, shard = rest, ""
    return et, st, shard


def _try_sports_dashboard_data(now_utc: datetime) -> dict[str, Any]:
    """
    Kalshi-backed sports schedule + tracker ladders for the control panel (best-effort;
    failures are non-fatal and returned in ``error``).
    """
    out: dict[str, Any] = {"schedule": [], "ladders": [], "error": None}
    try:
        from vol_surface_strategy.kalshi_io import load_client
        from vol_surface_strategy.sports_discovery import (
            iter_sports_game_targets,
            parse_event_overrides_with_start,
        )
        from vol_surface_strategy.sports_windows import (
            ET,
            describe_sports_trading_window,
            game_start_et_from_markets,
            in_pre_game_order_window,
            sports_trading_window_open_et,
        )

        client = load_client()
        games = iter_sports_game_targets(client, scan_debug=False)
        so = parse_event_overrides_with_start()
        now_et = now_utc.astimezone(ET)
        horizon = now_utc + timedelta(hours=48)

        event_gs: dict[str, tuple[SportCode, datetime]] = {}
        event_mk: dict[str, list[Any]] = {}
        for et, sport, mk in games:
            if not mk:
                continue
            event_mk[et] = mk
            gs_et: Optional[datetime] = None
            if et in so:
                try:
                    dt = datetime.fromisoformat(so[et].replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=ET)
                    gs_et = dt.astimezone(ET)
                except ValueError:
                    pass
            if gs_et is None:
                gs_et = game_start_et_from_markets(mk, sport=sport, fallback_utc=None)
            if gs_et is None:
                continue
            prev = event_gs.get(et)
            if prev is None or gs_et < prev[1]:
                event_gs[et] = (sport, gs_et)

        sched: list[dict[str, Any]] = []
        for et, (sport, gs_et) in sorted(
            event_gs.items(), key=lambda kv: kv[1][1].astimezone(timezone.utc)
        ):
            gs_utc = gs_et.astimezone(timezone.utc)
            if gs_utc <= now_utc or gs_utc > horizon:
                continue
            open_et, close_et = sports_trading_window_open_et(gs_et)
            in_w, mins_to = in_pre_game_order_window(sport, now_et, gs_et)
            mins_until_open = max(0.0, (open_et - now_et).total_seconds() / 60.0)
            sched.append(
                {
                    "event_ticker": et,
                    "sport": sport,
                    "game_start_et": gs_et.isoformat(),
                    "window_open_et": open_et.isoformat(),
                    "window_close_et": close_et.isoformat(),
                    "in_trading_window": in_w,
                    "minutes_until_game_start": round(mins_to, 1),
                    "minutes_until_trading_window_opens": round(mins_until_open, 1),
                }
            )
        out["schedule"] = sched

        ladders: list[dict[str, Any]] = []
        for row in list_all_rows(120):
            key = row.key or ""
            if not (row.market_type == "sports_vol_surface" or key.startswith("s:")):
                continue
            et, st, shard = _parse_sports_tracker_key(key)
            if not et:
                continue
            tpl = event_gs.get(et)
            sport_eff: Optional[SportCode] = tpl[0] if tpl else sport_from_series_ticker(st)
            if sport_eff is None:
                continue
            mk = event_mk.get(et) or []
            gs_et = tpl[1] if tpl else (
                game_start_et_from_markets(mk, sport=sport_eff, fallback_utc=None) if mk else None
            )
            win = describe_sports_trading_window(cast(SportCode, sport_eff), now_utc, gs_et)
            scan = _read_last_scan_row(key)
            st_norm = normalize_tracker_status(row.status)
            ladders.append(
                {
                    "key": key,
                    "label": f"{sport_eff} {et}" + (f" · {st}" if st else "") + (f" [{shard}]" if shard else ""),
                    "event_ticker": et,
                    "series_ticker": st,
                    "ladder_shard": shard or None,
                    "sport": sport_eff,
                    "order_status": _order_status_from_tracker(row),
                    "status": st_norm,
                    "raw_status": row.status,
                    "ticker": row.ticker,
                    "side": row.side,
                    "contracts": row.contracts,
                    "entry_cents": row.entry_cents,
                    "order_id": row.order_id,
                    "window": win,
                    "last_scan": _last_scan_summary(scan),
                }
            )
        out["ladders"] = ladders
    except Exception as e:
        out["error"] = str(e)
    return out


def build_dashboard_payload(repo_root: Any = None) -> dict[str, Any]:
    """Full JSON for GET /api/strategies/vol-surface/dashboard."""
    now = datetime.now(timezone.utc)
    specs = _enumerate_all_market_specs(now)
    markets_full = [_build_market_row(k, lab, dmt, now) for k, lab, dmt in specs]

    pnl_by = sum_pnl_by_market_type()
    sports_extras = _try_sports_dashboard_data(now)
    trade_rows = _read_trade_outcomes(120)
    trade_rows = _enrich_trade_outcomes_with_settlement_ledger(trade_rows, repo_root=repo_root)
    return {
        "generated_at_utc": now.isoformat(),
        "cumulative_pnl_cents": sum_realized_pnl_cents(),
        "pnl_by_market_type": pnl_by,
        "markets": markets_full,
        "tracker_rows": markets_full,
        "recent_order_events": _read_recent_orders(100),
        "trade_outcomes": trade_rows,
        "panel_db": str(PANEL_DB_PATH),
        "sports_schedule": sports_extras.get("schedule") or [],
        "sports_ladders": sports_extras.get("ladders") or [],
        "sports_panel_error": sports_extras.get("error"),
    }
