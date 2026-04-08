"""Discover BTC hourly and weather markets from the Kalshi API."""

from __future__ import annotations

import os
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Literal, Optional

from vol_surface_strategy.config import CitySchedule, DEFAULT_BTC_HOURLY_SERIES
from vol_surface_strategy.kalshi_io import get_markets_page_raw

WeatherKind = Literal["HIGH", "LOW"]


_TEMP_RE = re.compile(r"(TEMP|HIGHTEMP|KXTEMP|KXHIGH|KXLOW)", re.IGNORECASE)
# KXLOWTAUS-26APR09-T54 → calendar day encoded in ticker (avoids mixing two open events).
_KX_EVENT_DAY_RE = re.compile(r"-(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d{2})-", re.I)
_MONTH_NUM = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


def _event_day_from_ticker(ticker: str) -> Optional[date]:
    m = _KX_EVENT_DAY_RE.search(ticker.upper())
    if not m:
        return None
    yy, mon, dd = int(m.group(1)), m.group(2).upper(), int(m.group(3))
    mo = _MONTH_NUM.get(mon)
    if mo is None:
        return None
    year = 2000 + yy
    try:
        return date(year, mo, dd)
    except ValueError:
        return None


def _city_match(m: Any, city: CitySchedule) -> bool:
    blob = " ".join(
        str(getattr(m, a, "") or "")
        for a in ("title", "subtitle", "ticker", "event_ticker", "series_ticker", "yes_sub_title")
    ).lower()
    if any(v.lower() in blob for v in city.kalshi_name_variants):
        return True
    return any(t in blob for t in city.discovery_blob_tokens)


def _is_high_title(m: Any) -> bool:
    t = (str(getattr(m, "title", "") or "") + str(getattr(m, "subtitle", "") or "")).lower()
    if any(x in t for x in ("low", "minimum", "min temp", "overnight low")):
        return False
    return any(x in t for x in ("high", "highest", "hi ", "max")) or "high" in t


def _is_low_title(m: Any) -> bool:
    t = (str(getattr(m, "title", "") or "") + str(getattr(m, "subtitle", "") or "")).lower()
    return any(x in t for x in ("low", "minimum", "min temp", "overnight low", "daily low"))


def _is_temp_surface_market(m: Any) -> bool:
    """True if this looks like a daily city temperature line (threshold or range buckets)."""
    tk = str(getattr(m, "ticker", "") or "")
    title = str(getattr(m, "title", "") or "")
    sub = str(getattr(m, "subtitle", "") or "")
    blob_l = (title + " " + sub).lower()
    if _TEMP_RE.search(tk):
        return True
    if "temp" in blob_l or "temperature" in blob_l:
        return True
    if "°" in title or "°" in sub or "℉" in title or "℉" in sub:
        return True
    return bool(re.search(r"KXHIGH[A-Z]{2,8}-|KXLOW[A-Z]{2,8}-", tk))


def _close_ts_window_for_resolution(
    resolution_date: date,
    local_tz: Any,
    *,
    pad_days_after: int = 7,
) -> tuple[int, int]:
    """UTC unix bounds for catalog fallback (wider window; Kalshi close times vary by product)."""
    start_local = datetime.combine(resolution_date, datetime.min.time()).replace(tzinfo=local_tz)
    end_local = start_local + timedelta(days=pad_days_after)
    start_utc = start_local.astimezone(timezone.utc) - timedelta(days=1)
    end_utc = end_local.astimezone(timezone.utc) + timedelta(days=1)
    return int(start_utc.timestamp()), int(end_utc.timestamp())


def _weather_series_ticker(city: CitySchedule, kind: WeatherKind) -> Optional[str]:
    if kind == "HIGH":
        return city.kalshi_high_series
    return city.kalshi_low_series


def _parse_close_local(m: Any, local_tz: Any) -> Optional[datetime]:
    ct = getattr(m, "close_time", None)
    if not ct:
        return None
    if isinstance(ct, str):
        if ct.endswith("Z"):
            ct = ct[:-1] + "+00:00"
        cdt = datetime.fromisoformat(ct)
    else:
        cdt = ct
    if cdt.tzinfo is None:
        cdt = cdt.replace(tzinfo=timezone.utc)
    return cdt.astimezone(local_tz)


def _market_matches_resolution(
    m: Any,
    kind: WeatherKind,
    resolution_date: date,
    local_tz: Any,
    city: CitySchedule,
    *,
    from_series: bool,
) -> bool:
    cld = _parse_close_local(m, local_tz)
    if cld is None:
        return False
    tk = str(getattr(m, "ticker", "") or "")
    ev_day = _event_day_from_ticker(tk)
    if ev_day is not None:
        if ev_day != resolution_date:
            return False
    else:
        cd = cld.date()
        if not (resolution_date <= cd <= resolution_date + timedelta(days=1)):
            return False
    if from_series:
        return True
    if not _is_temp_surface_market(m):
        return False
    if not _city_match(m, city):
        return False
    if kind == "HIGH" and not _is_high_title(m):
        return False
    if kind == "LOW" and not _is_low_title(m):
        return False
    return True


def _discover_weather_via_series(
    client: Any,
    city: CitySchedule,
    kind: WeatherKind,
    resolution_date: date,
    local_tz: Any,
    series_ticker: str,
) -> list[Any]:
    """Small result set: paginate GET /markets?series_ticker=… only."""
    out: list[Any] = []
    cursor = None
    while True:
        markets, cursor = get_markets_page_raw(
            client,
            limit=200,
            cursor=cursor or None,
            series_ticker=series_ticker,
        )
        for m in markets:
            if _market_matches_resolution(m, kind, resolution_date, local_tz, city, from_series=True):
                out.append(m)
        if not cursor:
            break
    return out


def _discover_weather_via_close_window(
    client: Any,
    city: CitySchedule,
    kind: WeatherKind,
    resolution_date: date,
    local_tz: Any,
    *,
    max_pages: Optional[int] = None,
) -> list[Any]:
    """Fallback: paginate open markets in a UTC close-time band + keyword filters."""
    out: list[Any] = []
    cursor = None
    page_ct = 0
    min_ts, max_ts = _close_ts_window_for_resolution(resolution_date, local_tz)
    while True:
        markets, cursor = get_markets_page_raw(
            client,
            limit=200,
            cursor=cursor or None,
            min_close_ts=min_ts,
            max_close_ts=max_ts,
        )
        page_ct += 1
        for m in markets:
            if not _market_matches_resolution(
                m, kind, resolution_date, local_tz, city, from_series=False
            ):
                continue
            out.append(m)
        if not cursor:
            break
        if max_pages is not None and page_ct >= max_pages:
            break
    return out


def discover_weather_markets(
    client: Any,
    city: CitySchedule,
    kind: WeatherKind,
    resolution_date: date,
    local_tz: Any,
    *,
    max_pages: Optional[int] = None,
) -> list[Any]:
    """Resolve daily temp markets for ``resolution_date`` (local).

    Prefer Kalshi ``series_ticker`` (per-city strings in :class:`CitySchedule`) so we do not
    rely on paging the global open catalog. If no series is configured or the series query
    returns nothing, fall back to a close-time--window catalog scan.
    """
    st = _weather_series_ticker(city, kind)
    if st:
        found = _discover_weather_via_series(
            client, city, kind, resolution_date, local_tz, st
        )
        if found:
            return found
    return _discover_weather_via_close_window(
        client, city, kind, resolution_date, local_tz, max_pages=max_pages
    )


def discover_btc_hourly_markets(client: Any, *, hour_end_utc: datetime) -> list[Any]:
    """
    Markets for the BTC hourly event that resolves at hour_end_utc (:00).
    Uses series ticker from env VOL_BTC_HOURLY_SERIES (default KXBTC).
    Filters by ``close_time``; if multiple events share the window, keep one event (closest close).
    """
    series = os.environ.get("VOL_BTC_HOURLY_SERIES", DEFAULT_BTC_HOURLY_SERIES).strip()
    raw: list[Any] = []
    cursor = None
    he = hour_end_utc.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    while True:
        markets, cursor = get_markets_page_raw(
            client,
            limit=200,
            cursor=cursor or None,
            series_ticker=series,
        )
        for m in markets:
            ct = getattr(m, "close_time", None)
            if not ct:
                continue
            if isinstance(ct, str):
                if ct.endswith("Z"):
                    ct = ct[:-1] + "+00:00"
                cdt = datetime.fromisoformat(ct)
            else:
                cdt = ct
            if cdt.tzinfo is None:
                cdt = cdt.replace(tzinfo=timezone.utc)
            if abs((cdt - he).total_seconds()) > 120:
                continue
            raw.append(m)
        if not cursor:
            break
    if not raw:
        return []
    by_event: dict[str, list[Any]] = {}
    for m in raw:
        et = str(getattr(m, "event_ticker", "") or "") or "_"
        by_event.setdefault(et, []).append(m)
    # Prefer the event with the most strikes (full ladder) if the API returns multiple.
    best = max(by_event.keys(), key=lambda k: len(by_event[k]))
    return by_event[best]


def resolution_date_for_high_scan(local_now: datetime) -> date:
    """HIGH scans run on the resolution calendar day."""
    return local_now.date()


def resolution_date_for_low_scan(local_now: datetime) -> date:
    """LOW evening scans target the next calendar day's low."""
    return (local_now + timedelta(days=1)).date()
