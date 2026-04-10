"""Aggregate vol-surface bot state for the control panel (read-only)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from zoneinfo import ZoneInfo

from vol_surface_strategy.config import CITIES
from vol_surface_strategy.discovery import resolution_date_for_high_scan, resolution_date_for_low_scan
from vol_surface_strategy.panel_state import PANEL_DB_PATH, init_panel_db, sum_pnl_by_market_type, sum_realized_pnl_cents
from vol_surface_strategy.tracker import btc_key, get_row, weather_key
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
            return [{k: r[k] for k in r.keys()} for r in cur.fetchall()]
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


def build_dashboard_payload(repo_root: Any = None) -> dict[str, Any]:
    """Full JSON for GET /api/strategies/vol-surface/dashboard."""
    now = datetime.now(timezone.utc)
    specs = _enumerate_all_market_specs(now)
    markets_full = [_build_market_row(k, lab, dmt, now) for k, lab, dmt in specs]

    pnl_by = sum_pnl_by_market_type()
    return {
        "generated_at_utc": now.isoformat(),
        "cumulative_pnl_cents": sum_realized_pnl_cents(),
        "pnl_by_market_type": pnl_by,
        "markets": markets_full,
        "tracker_rows": markets_full,
        "recent_order_events": _read_recent_orders(100),
        "trade_outcomes": _read_trade_outcomes(120),
        "panel_db": str(PANEL_DB_PATH),
    }
