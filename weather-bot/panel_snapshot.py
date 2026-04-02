"""Read-only snapshot of forecast vs market edges for the control panel."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import pytz

from config import (
    CITIES,
    ENTRY_MAX_HOURS_TO_CLOSE,
    ENTRY_MIN_HOURS_TO_CLOSE,
    MAX_SPREAD_CENTS,
    MIN_DISTANCE_FROM_BUCKET_EDGE,
    MIN_EDGE,
    TRADE_WINDOW_END_HOUR,
    TRADE_WINDOW_START_HOUR,
)
from kalshi import (
    fetch_open_temp_markets_for_city,
    get_orderbook,
    load_client,
    market_range_label,
    parse_bucket_range,
)
from nws import get_grid_point, get_projected_high, has_severe_weather_blackout
from edge_model import (
    _bucket_sort_key,
    _distance_from_bucket_edges,
    _find_target_bucket,
    evaluate_trades,
    projected_high_to_bucket_probabilities,
)


def _bucket_display_label(title: str, low: Optional[int], high: Optional[int]) -> str:
    if low is None and high is not None:
        return f"≤{high}°F"
    if high is None and low is not None:
        return f"≥{low}°F"
    if low is not None and high is not None:
        return f"{low}–{high}°F"
    return (title or "")[:48]


def _close_dt_from_market(m: Any, tz: Any) -> Optional[datetime]:
    ct = getattr(m, "close_time", None)
    if ct is None:
        return None
    if isinstance(ct, datetime):
        close_dt = ct
    else:
        s = str(ct)
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        close_dt = datetime.fromisoformat(s)
    if close_dt.tzinfo is None:
        close_dt = close_dt.replace(tzinfo=timezone.utc)
    return close_dt.astimezone(tz)


def _next_trade_window_hint(now_local: datetime) -> str:
    h = now_local.hour
    if TRADE_WINDOW_START_HOUR <= h < TRADE_WINDOW_END_HOUR:
        end = now_local.replace(
            hour=TRADE_WINDOW_END_HOUR, minute=0, second=0, microsecond=0
        )
        return f"In trading window until {end.strftime('%H:%M')} local."
    if h < TRADE_WINDOW_START_HOUR:
        start = now_local.replace(
            hour=TRADE_WINDOW_START_HOUR, minute=0, second=0, microsecond=0
        )
        return f"Trading window opens at {start.strftime('%H:%M')} local today."
    tomorrow = now_local.date() + timedelta(days=1)
    start = now_local.replace(
        year=tomorrow.year,
        month=tomorrow.month,
        day=tomorrow.day,
        hour=TRADE_WINDOW_START_HOUR,
        minute=0,
        second=0,
        microsecond=0,
    )
    return (
        f"Past today’s window; next opens {start.strftime('%Y-%m-%d %H:%M')} local."
    )


def _entry_hours_hint(hours: Optional[float]) -> str:
    if hours is None:
        return "No market close time yet."
    if hours < ENTRY_MIN_HOURS_TO_CLOSE:
        return f"{hours:.1f}h to close — too soon for new entries (need >{ENTRY_MIN_HOURS_TO_CLOSE}h)."
    if hours > ENTRY_MAX_HOURS_TO_CLOSE:
        return f"{hours:.1f}h to close — too early for new entries (need <{ENTRY_MAX_HOURS_TO_CLOSE}h)."
    return f"{hours:.1f}h to close — inside the {ENTRY_MIN_HOURS_TO_CLOSE}–{ENTRY_MAX_HOURS_TO_CLOSE}h window where entries are allowed."


def analyze_city(
    client: Any, city_key: str, cfg: dict[str, Any]
) -> dict[str, Any]:
    tz_name = None
    out: dict[str, Any] = {
        "city": city_key,
        "timezone": None,
        "now_local": None,
        "in_trade_window": False,
        "trade_window_hint": "",
        "severe_blackout": False,
        "projected": None,
        "market_close_local": None,
        "hours_to_close": None,
        "target_bucket_ok": None,
        "target_distance_f": None,
        "bucket_rows": [],
        "best_edge_pct": None,
        "actionable_signals": [],
        "entry_hours_hint": None,
        "error": None,
    }
    try:
        grid = get_grid_point(cfg["lat"], cfg["lon"])
    except Exception as e:
        out["error"] = f"grid: {e}"
        return out

    tz = pytz.timezone(grid["timeZone"])
    out["timezone"] = grid["timeZone"]
    now_local = datetime.now(tz)
    out["now_local"] = now_local.isoformat()
    out["trade_window_hint"] = _next_trade_window_hint(now_local)
    out["in_trade_window"] = TRADE_WINDOW_START_HOUR <= now_local.hour < TRADE_WINDOW_END_HOUR

    try:
        out["severe_blackout"] = has_severe_weather_blackout(cfg["alert_area"])
    except Exception:
        out["severe_blackout"] = False

    projected = get_projected_high(city_key, grid, cfg["metar_station"])
    out["projected"] = projected
    if not projected:
        out["error"] = "No projected high (forecast/METAR)"
        return out

    try:
        raw_markets = fetch_open_temp_markets_for_city(
            client, cfg["kalshi_name_variants"], tz
        )
    except Exception as e:
        out["error"] = f"markets: {e}"
        return out

    if not raw_markets:
        out["error"] = "No matching open Kalshi markets for today"
        return out

    buckets: list[dict[str, Any]] = []
    for m in raw_markets:
        ticker = getattr(m, "ticker", None)
        if not ticker:
            continue
        label = market_range_label(m)
        low, high = parse_bucket_range(label)
        if low is None and high is None:
            continue
        try:
            ob = get_orderbook(client, ticker)
        except Exception:
            continue
        buckets.append(
            {
                "ticker": ticker,
                "title": getattr(m, "title", None) or "",
                "low": low,
                "high": high,
                "yes_bid": ob["yes_bid"],
                "yes_ask": ob["yes_ask"],
                "spread": ob["spread"],
                "midpoint": ob["midpoint"],
            }
        )

    if not buckets:
        out["error"] = "No buckets with orderbooks"
        return out

    close_time_local = _close_dt_from_market(raw_markets[0], tz)
    if close_time_local:
        out["market_close_local"] = close_time_local.isoformat()
        htc = round(
            (close_time_local - now_local).total_seconds() / 3600.0, 2
        )
        out["hours_to_close"] = htc
        out["entry_hours_hint"] = _entry_hours_hint(htc)

    ph = float(projected["projected_high"])
    ordered = sorted(buckets, key=_bucket_sort_key)
    target = _find_target_bucket(ph, ordered)
    if target:
        dist = _distance_from_bucket_edges(ph, target)
        out["target_bucket_ok"] = dist >= MIN_DISTANCE_FROM_BUCKET_EDGE
        out["target_distance_f"] = round(dist, 2)
    else:
        out["target_bucket_ok"] = False
        out["target_distance_f"] = None

    model_probs = projected_high_to_bucket_probabilities(ph, ordered)
    rows: list[dict[str, Any]] = []
    for b in buckets:
        mid = b.get("midpoint")
        if mid is None:
            continue
        market_prob = float(mid) / 100.0
        mp = model_probs.get(b["ticker"], 0.0)
        edge = mp - market_prob
        sp = b.get("spread")
        rows.append(
            {
                "ticker": b["ticker"],
                "bucket": _bucket_display_label(b.get("title", ""), b["low"], b["high"]),
                "model_pct": round(mp * 100, 1),
                "market_pct": round(float(mid), 1),
                "edge_pct": round(edge * 100, 1),
                "spread": sp,
                "spread_ok": sp is not None and sp <= MAX_SPREAD_CENTS,
            }
        )

    rows.sort(key=lambda r: abs(r["edge_pct"]), reverse=True)
    out["bucket_rows"] = rows
    if rows:
        out["best_edge_pct"] = rows[0]["edge_pct"]

    try:
        out["actionable_signals"] = evaluate_trades(
            projected, buckets, now_local, close_time_local or now_local
        )
    except Exception:
        out["actionable_signals"] = []

    return out


def build_snapshot() -> dict[str, Any]:
    """Full snapshot for API JSON."""
    try:
        client = load_client()
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
            "cities": [],
            "next_steps": [],
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        }

    cities_out: list[dict[str, Any]] = []
    for city_key, cfg in CITIES.items():
        cities_out.append(analyze_city(client, city_key, cfg))

    now_utc = datetime.now(timezone.utc)
    next_half = now_utc.replace(second=0, microsecond=0)
    if next_half.minute < 30:
        next_half = next_half.replace(minute=30)
    else:
        next_half = next_half + timedelta(hours=1)
        next_half = next_half.replace(minute=0)

    next_steps = [
        "The bot runs a full sweep every 30 minutes (and once at startup). "
        "Use Restart on the Weather bot page if you want an immediate sweep.",
        f"Rough next half-hour tick (UTC): {next_half.strftime('%H:%M')} — "
        "align checks with :00 and :30 past the hour.",
        f"Per city, new entries only during local hours "
        f"{TRADE_WINDOW_START_HOUR}:00–{TRADE_WINDOW_END_HOUR}:00.",
        "New limit orders only when roughly 1–6 hours remain before the Kalshi market close.",
        f"Signals need model edge ≥ {MIN_EDGE:.0%} vs mid, spread ≤ {MAX_SPREAD_CENTS}¢, "
        f"and projected high ≥ {MIN_DISTANCE_FROM_BUCKET_EDGE}°F from bucket edges.",
        "Refresh this tab after weather or order books move; NWS updates can shift the model.",
    ]

    return {
        "ok": True,
        "error": None,
        "cities": cities_out,
        "next_steps": next_steps,
        "constants": {
            "min_edge": MIN_EDGE,
            "max_spread_cents": MAX_SPREAD_CENTS,
            "min_bucket_edge_distance_f": MIN_DISTANCE_FROM_BUCKET_EDGE,
            "trade_window_start_hour": TRADE_WINDOW_START_HOUR,
            "trade_window_end_hour": TRADE_WINDOW_END_HOUR,
            "entry_hours_min": ENTRY_MIN_HOURS_TO_CLOSE,
            "entry_hours_max": ENTRY_MAX_HOURS_TO_CLOSE,
        },
        "generated_at_utc": now_utc.isoformat(),
    }
