"""Aggregate vol-surface bot state for the control panel (read-only)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from zoneinfo import ZoneInfo

from vol_surface_strategy.config import CITIES
from vol_surface_strategy.panel_state import PANEL_DB_PATH, init_panel_db, sum_pnl_by_market_type, sum_realized_pnl_cents
from vol_surface_strategy.tracker import list_all_rows
from vol_surface_strategy.trading_windows import (
    btc_in_trading_window,
    btc_order_expiration_ts,
    minutes_until_btc_hour_end,
    minutes_until_weather_high_end,
    minutes_until_weather_low_end,
    normalize_tracker_status,
    weather_high_in_window,
    weather_low_in_window,
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
        exp = btc_order_expiration_ts(utc)
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
            "minutes_to_hour_end": round(t_rem, 1),
            "order_expiration_ts": exp,
            "window_hint": "UTC :05–:40 scan; :45 cancel" + (" · active now" if in_w else ""),
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
        return {
            "kind": "weather",
            "city_id": city_id,
            "high_low": hi_lo,
            "timezone": CITIES[city_id].tz_name,
            "in_trading_window": in_w,
            "minutes_to_window_end": round(t_rem, 1),
            "local_window": f"{sh:02d}:{sm:02d}–{eh:02d}:{em:02d} local",
            "window_hint": ("Inside trading window" if in_w else "Outside trading window"),
        }

    return {"kind": "unknown", "in_trading_window": False, "window_hint": ""}


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


def build_dashboard_payload(repo_root: Any = None) -> dict[str, Any]:
    """Full JSON for GET /api/strategies/vol-surface/dashboard."""
    now = datetime.now(timezone.utc)
    rows = list_all_rows(250)
    markets: list[dict[str, Any]] = []

    for row in rows:
        mt, cid, hil = _parse_key_parts(row.key)
        st = normalize_tracker_status(row.status)
        win = _describe_market_window(row.key, row.market_type or mt, cid, hil, now)
        scan = _read_last_scan_row(row.key)
        scan_summary: Optional[dict[str, Any]] = None
        if scan:
            gl = scan.get("gate_log_json") or "[]"
            try:
                glp = json.loads(gl) if isinstance(gl, str) else gl
            except json.JSONDecodeError:
                glp = []
            scan_summary = {
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
        markets.append(
            {
                "key": row.key,
                "status": st,
                "market_type": row.market_type,
                "raw_status": row.status,
                "ticker": row.ticker,
                "side": row.side,
                "contracts": row.contracts,
                "entry_cents": row.entry_cents,
                "order_id": row.order_id,
                "city_id": row.city_id,
                "resolution_date": row.resolution_date,
                "hour_start_utc": row.hour_start_utc,
                "window": win,
                "last_scan": scan_summary,
            }
        )

    pnl_by = sum_pnl_by_market_type()
    return {
        "generated_at_utc": now.isoformat(),
        "cumulative_pnl_cents": sum_realized_pnl_cents(),
        "pnl_by_market_type": pnl_by,
        "tracker_rows": markets,
        "recent_order_events": _read_recent_orders(100),
        "trade_outcomes": _read_trade_outcomes(120),
        "panel_db": str(PANEL_DB_PATH),
    }
