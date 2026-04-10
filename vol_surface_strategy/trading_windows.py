"""Local trading windows for BTC (UTC) and weather HIGH/LOW per city."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from zoneinfo import ZoneInfo

from vol_surface_strategy.config import CITIES

# --- BTC hourly (UTC): monitor at minutes 5..40; cancel at :45; no activity before :05 or after :45 ---
BTC_MONITOR_MINUTES_UTC = frozenset({5, 10, 15, 20, 25, 30, 35, 40})

# Resting limit orders: expire soon so stale edges do not fill after conditions move.
ORDER_RESTING_TTL_MINUTES = 4


@dataclass(frozen=True)
class LocalHM:
    hour: int
    minute: int


# Weather HIGH: local (start inclusive, end exclusive) — at `end`, cancel and stop.
WEATHER_HIGH_WINDOWS: dict[str, tuple[LocalHM, LocalHM]] = {
    "LAX": (LocalHM(7, 0), LocalHM(9, 0)),
    "NYC": (LocalHM(9, 0), LocalHM(13, 0)),
    "MIA": (LocalHM(9, 0), LocalHM(13, 0)),
    "PHL": (LocalHM(9, 0), LocalHM(13, 0)),
    "TPA": (LocalHM(9, 0), LocalHM(13, 0)),
    "CHI": (LocalHM(9, 0), LocalHM(13, 0)),
    "MSP": (LocalHM(9, 0), LocalHM(13, 0)),
    "BNA": (LocalHM(9, 0), LocalHM(13, 0)),
    "MSY": (LocalHM(9, 0), LocalHM(13, 0)),
    "AUS": (LocalHM(10, 0), LocalHM(14, 0)),
    "OKC": (LocalHM(10, 0), LocalHM(14, 0)),
    "SAT": (LocalHM(10, 0), LocalHM(14, 0)),
    "DEN": (LocalHM(10, 0), LocalHM(14, 0)),
    "PHX": (LocalHM(10, 0), LocalHM(15, 0)),
}

# Weather LOW: 19:00–23:00 local (end at 23:00 cancel).
WEATHER_LOW_WINDOW: tuple[LocalHM, LocalHM] = (LocalHM(19, 0), LocalHM(23, 0))


def _hm_to_minutes(h: int, m: int) -> int:
    return h * 60 + m


def local_minutes_of_day(local_dt: datetime) -> int:
    return _hm_to_minutes(local_dt.hour, local_dt.minute)


def in_half_open_window(local_dt: datetime, start: LocalHM, end: LocalHM) -> bool:
    """True iff local clock time is in [start, end)."""
    t = local_minutes_of_day(local_dt)
    ts = _hm_to_minutes(start.hour, start.minute)
    te = _hm_to_minutes(end.hour, end.minute)
    return ts <= t < te


def is_close_minute(local_dt: datetime, end: LocalHM) -> bool:
    """First minute of window end — cancel resting orders."""
    return local_dt.hour == end.hour and local_dt.minute == end.minute and local_dt.second < 28


def _five_min_tick(local_dt: datetime) -> bool:
    return local_dt.minute % 5 == 0 and local_dt.second < 28


def weather_high_in_window(city_id: str, local_dt: datetime) -> bool:
    w = WEATHER_HIGH_WINDOWS.get(city_id)
    if not w:
        return False
    start, end = w
    return in_half_open_window(local_dt, start, end)


def weather_low_in_window(local_dt: datetime) -> bool:
    start, end = WEATHER_LOW_WINDOW
    return in_half_open_window(local_dt, start, end)


def weather_high_should_monitor(city_id: str, local_dt: datetime) -> bool:
    return weather_high_in_window(city_id, local_dt) and _five_min_tick(local_dt)


def weather_low_should_monitor(local_dt: datetime) -> bool:
    return weather_low_in_window(local_dt) and _five_min_tick(local_dt)


def weather_high_close_tick(city_id: str, local_dt: datetime) -> bool:
    w = WEATHER_HIGH_WINDOWS.get(city_id)
    if not w:
        return False
    return is_close_minute(local_dt, w[1])


def weather_low_close_tick(local_dt: datetime) -> bool:
    return is_close_minute(local_dt, WEATHER_LOW_WINDOW[1])


def btc_in_trading_window(utc_dt: datetime) -> bool:
    return 5 <= utc_dt.minute < 45


def btc_should_monitor(utc_dt: datetime) -> bool:
    return utc_dt.minute in BTC_MONITOR_MINUTES_UTC and utc_dt.second < 28


def btc_close_tick(utc_dt: datetime) -> bool:
    return utc_dt.minute == 45 and utc_dt.second < 28


def btc_order_expiration_ts(utc_dt: datetime) -> int:
    """Latest allowed order expiry: minute 45 of the current hour (UTC)."""
    base = utc_dt.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    exp = base.replace(minute=45)
    return int(exp.timestamp())


def resting_order_expiration_ts(now_utc: datetime, *, latest_allowed_ts: int) -> int:
    """
    Unix timestamp when a posted limit order should expire.

    Uses the earlier of (now + ORDER_RESTING_TTL_MINUTES) and ``latest_allowed_ts``
    (structural cap: BTC :45 UTC or end of weather window).
    """
    now = now_utc.astimezone(timezone.utc)
    short = int((now + timedelta(minutes=ORDER_RESTING_TTL_MINUTES)).timestamp())
    return min(short, int(latest_allowed_ts))


def weather_high_order_expiration_ts(res_date: date, city_id: str) -> int:
    """Latest allowed order expiry: end of city's HIGH window (local)."""
    w = WEATHER_HIGH_WINDOWS[city_id]
    end = w[1]
    tz = ZoneInfo(CITIES[city_id].tz_name)
    dt = datetime(res_date.year, res_date.month, res_date.day, end.hour, end.minute, tzinfo=tz)
    return int(dt.timestamp())


def weather_low_order_expiration_ts(res_date: date, city_id: str) -> int:
    """Latest allowed order expiry: 23:00 local on resolution day."""
    tz = ZoneInfo(CITIES[city_id].tz_name)
    dt = datetime(res_date.year, res_date.month, res_date.day, 23, 0, tzinfo=tz)
    return int(dt.timestamp())


def minutes_until_weather_high_end(city_id: str, local_dt: datetime) -> float:
    w = WEATHER_HIGH_WINDOWS.get(city_id)
    if not w:
        return 0.0
    end = w[1]
    end_of_day = local_dt.replace(hour=end.hour, minute=end.minute, second=0, microsecond=0)
    if local_dt >= end_of_day:
        return 0.0
    return max(0.0, (end_of_day - local_dt).total_seconds() / 60.0)


def minutes_until_weather_low_end(local_dt: datetime) -> float:
    end = WEATHER_LOW_WINDOW[1]
    end_of_day = local_dt.replace(hour=end.hour, minute=end.minute, second=0, microsecond=0)
    if local_dt >= end_of_day:
        return 0.0
    return max(0.0, (end_of_day - local_dt).total_seconds() / 60.0)


def minutes_until_btc_hour_end(utc_dt: datetime) -> float:
    """Top of next hour from current hour start (resolution)."""
    u = utc_dt.astimezone(timezone.utc)
    nxt = u.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    return max(0.0, (nxt - u).total_seconds() / 60.0)


def normalize_tracker_status(status: str) -> str:
    """Map legacy DB statuses to the current state machine."""
    m = {
        "pending_first_scan": "window_open",
        "pending_second_scan": "window_open",
        "filled": "position_active",
        "expired": "window_closed",
    }
    return m.get(status, status)
