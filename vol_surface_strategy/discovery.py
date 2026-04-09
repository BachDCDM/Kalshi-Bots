"""Discover BTC hourly and weather markets from the Kalshi API."""

from __future__ import annotations

import logging
import os
import re
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from typing import Any, Literal, Optional
from zoneinfo import ZoneInfo

from vol_surface_strategy.config import CitySchedule, DEFAULT_BTC_HOURLY_SERIES
from vol_surface_strategy.kalshi_io import get_markets_page_raw

WeatherKind = Literal["HIGH", "LOW"]

LOG = logging.getLogger("vol_surface")

# KXBTC-…-T71200 = cumulative "above $71,200"; …-B71250 = $100 range bucket.
_BTC_TICKER_THRESHOLD_TAIL_RE = re.compile(r"-T[\d.]+", re.I)
_BTC_TICKER_BUCKET_TAIL_RE = re.compile(r"-B[\d.]+", re.I)


def _btc_market_title_blob(m: Any) -> str:
    parts = [
        str(getattr(m, "title", "") or ""),
        str(getattr(m, "subtitle", "") or ""),
        str(getattr(m, "yes_sub_title", "") or ""),
        str(getattr(m, "no_sub_title", "") or ""),
    ]
    return " ".join(parts).lower()


def btc_hourly_market_is_threshold_style(m: Any) -> bool:
    """
    Cumulative threshold line: ticker ``-T71200`` / ``-T77799.99``, or subtitles like \"or above\".

    ``-B71250`` range-band tickers are never treated as cumulative, even if copy mentions \"above\"
    (avoids mixing $100 bucket lines with true threshold instruments).
    """
    tk = str(getattr(m, "ticker", "") or "")
    if _BTC_TICKER_BUCKET_TAIL_RE.search(tk):
        return False
    if _BTC_TICKER_THRESHOLD_TAIL_RE.search(tk):
        return True
    blob = _btc_market_title_blob(m)
    if "or above" in blob or "or higher" in blob:
        return True
    return False


def btc_hourly_market_is_range_bucket_style(m: Any) -> bool:
    """$100 band contract, typically ``-B71250`` in the ticker."""
    tk = str(getattr(m, "ticker", "") or "")
    return bool(_BTC_TICKER_BUCKET_TAIL_RE.search(tk))


def _prefer_btc_threshold_contracts(markets: list[Any], *, min_count: int = 3) -> list[Any]:
    """
    Same Kalshi hourly event often lists both range buckets (-B…) and cumulative thresholds (-T…).
    The vol surface expects monotone cumulative mids — use threshold lines when enough exist.
    """
    if os.environ.get("VOL_BTC_NO_THRESHOLD_PREF", "").strip() in ("1", "true", "yes"):
        return markets
    th = [m for m in markets if btc_hourly_market_is_threshold_style(m)]
    if len(th) < min_count:
        n_t = sum(
            1 for m in markets if _BTC_TICKER_THRESHOLD_TAIL_RE.search(str(getattr(m, "ticker", "") or ""))
        )
        n_b = sum(
            1 for m in markets if _BTC_TICKER_BUCKET_TAIL_RE.search(str(getattr(m, "ticker", "") or ""))
        )
        if n_b > 0 and 0 < n_t < min_count:
            LOG.info(
                "BTC discovery: series mixes %d -B range bands with only %d -T cumulative lines "
                "(need ≥%d -T lines to prefer threshold subset). Using full ladder.",
                n_b,
                n_t,
                min_count,
            )
        return markets
    if len(th) == len(markets):
        LOG.info(
            "BTC discovery: %d markets all threshold-style (cumulative); using full set.",
            len(th),
        )
        return markets
    n_bucket_tickers = sum(1 for m in markets if btc_hourly_market_is_range_bucket_style(m))
    LOG.info(
        "BTC discovery: using threshold (cumulative) subset n=%d (dropped %d non-threshold; "
        "~%d had -B bucket tickers).",
        len(th),
        len(markets) - len(th),
        n_bucket_tickers,
    )
    return th


def parse_market_resolve_utc(m: Any) -> Optional[datetime]:
    """
    Resolution / close instant for a market object from GET /markets.
    Tries Kalshi field variants (ISO strings or Unix seconds).
    """
    for attr in (
        "close_time",
        "expiration_time",
        "expected_expiration_time",
        "expiration_ts",
    ):
        val = getattr(m, attr, None)
        if val is None:
            continue
        if isinstance(val, bool):
            continue
        if isinstance(val, (int, float)):
            try:
                ts = float(val)
                if ts > 1e12:
                    ts /= 1000.0
                return datetime.fromtimestamp(ts, tz=timezone.utc).replace(microsecond=0)
            except (OSError, ValueError, OverflowError):
                continue
        if isinstance(val, str):
            s = val.strip()
            if s.isdigit():
                try:
                    ts = int(s)
                    if ts > 1e12:
                        ts //= 1000
                    return datetime.fromtimestamp(ts, tz=timezone.utc).replace(microsecond=0)
                except (OSError, ValueError, OverflowError):
                    continue
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            try:
                cdt = datetime.fromisoformat(s)
            except ValueError:
                continue
            if cdt.tzinfo is None:
                cdt = cdt.replace(tzinfo=timezone.utc)
            return cdt.astimezone(timezone.utc).replace(microsecond=0)
        if isinstance(val, datetime):
            cdt = val
            if cdt.tzinfo is None:
                cdt = cdt.replace(tzinfo=timezone.utc)
            return cdt.astimezone(timezone.utc).replace(microsecond=0)
    return None


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
    st = (city.station or "").strip().lower()
    if st and len(st) >= 3 and st in blob.replace(" ", ""):
        return True
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


def _weather_series_env_override(city_id: str, kind: WeatherKind) -> Optional[str]:
    """Optional: VOL_WEATHER_SERIES_NYC_HIGH=KXHIGHNY (same pattern for any city_id)."""
    key = f"VOL_WEATHER_SERIES_{city_id}_{kind}"
    v = os.environ.get(key, "").strip()
    return v or None


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
    st = _weather_series_ticker(city, kind) or _weather_series_env_override(city.city_id, kind)
    if st:
        found = _discover_weather_via_series(
            client, city, kind, resolution_date, local_tz, st
        )
        if found:
            return found
    return _discover_weather_via_close_window(
        client, city, kind, resolution_date, local_tz, max_pages=max_pages
    )


def _btc_event_ticker_prefix(hour_end_utc: datetime) -> str:
    """Kalshi encodes the event in America/New_York wall time, e.g. KXBTC-26APR0812 = noon Eastern."""
    he = hour_end_utc.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    et = he.astimezone(ZoneInfo("America/New_York"))
    yy = et.year % 100
    mon = (
        "JAN",
        "FEB",
        "MAR",
        "APR",
        "MAY",
        "JUN",
        "JUL",
        "AUG",
        "SEP",
        "OCT",
        "NOV",
        "DEC",
    )[et.month - 1]
    return f"KXBTC-{yy:02d}{mon}{et.day:02d}{et.hour:02d}"


def _group_by_event_ticker(markets: list[Any]) -> dict[str, list[Any]]:
    out: dict[str, list[Any]] = {}
    for m in markets:
        et = str(getattr(m, "event_ticker", "") or "") or "_"
        out.setdefault(et, []).append(m)
    return out


def _btc_choose_event_group(raw: list[Any], prefix: str, he: datetime) -> list[Any]:
    matched = [m for m in raw if str(getattr(m, "event_ticker", "") or "").startswith(prefix)]
    pool = matched if matched else raw
    groups = _group_by_event_ticker(pool)
    if len(groups) == 1:
        return next(iter(groups.values()))
    best_ms: Optional[list[Any]] = None
    best_d = float("inf")
    for ms in groups.values():
        deltas: list[float] = []
        for m in ms:
            u = parse_market_resolve_utc(m)
            if u is not None:
                deltas.append(abs((u - he).total_seconds()))
        if not deltas:
            continue
        dmin = min(deltas)
        if dmin < best_d or (
            abs(dmin - best_d) < 1e-6 and best_ms is not None and len(ms) > len(best_ms)
        ):
            best_d = dmin
            best_ms = ms
    return best_ms if best_ms is not None else max(groups.values(), key=len)


def _btc_discovery_stats(markets: list[Any]) -> tuple[int, list[tuple[str, int]], int, list[str]]:
    ticker_counts = Counter(str(getattr(m, "event_ticker", "") or "") or "_" for m in markets)
    close_vals: list[str] = []
    for m in markets:
        raw_ct = getattr(m, "close_time", None)
        if raw_ct is not None:
            close_vals.append(str(raw_ct)[:40])
    uniq_close = sorted(set(close_vals))
    preview = ticker_counts.most_common(12)
    return len(ticker_counts), preview, len(uniq_close), uniq_close[:8]


def _log_btc_discovery_line(
    *,
    pre: list[Any],
    post: list[Any],
    he: datetime,
    hard_tol: float,
    fellback: bool,
) -> None:
    """Single INFO line for scenario A vs B diagnosis; DEBUG lists API field names on first contract."""
    pre_u, pre_top, pre_uc, pre_cs = _btc_discovery_stats(pre) if pre else (0, [], 0, [])
    post_u, post_top, post_uc, post_cs = _btc_discovery_stats(post) if post else (0, [], 0, [])
    LOG.info(
        "BTC discovery: hour_end_utc=%s hard_tol_sec=%.0f fellback_unfiltered=%s | "
        "pre_hard_close n=%d unique_event_tickers=%d unique_close_time_str=%d "
        "per_ticker_top=%s close_str_sample=%s || post_hard_close n=%d tickers=%d close_str=%d "
        "per_ticker_top=%s close_str_sample=%s",
        he.isoformat(),
        hard_tol,
        fellback,
        len(pre),
        pre_u,
        pre_uc,
        pre_top,
        pre_cs,
        len(post),
        post_u,
        post_uc,
        post_top,
        post_cs,
    )
    if post:
        m0 = post[0]
        keys = sorted(vars(m0).keys() if hasattr(m0, "__dict__") else [])
        keys = [k for k in keys if not k.startswith("_")]
        LOG.debug("BTC discovery first market attribute keys: %s", keys)


def _log_btc_market_list_api_sample(markets: list[Any]) -> None:
    """Diagnostic: first market from GET /markets after series filter (remove once field mapping is stable)."""
    sample = markets[0]
    if isinstance(sample, dict):
        keys = sorted(sample.keys())
        vals = {k: sample[k] for k in keys[:20]}
    else:
        vd = vars(sample) if hasattr(sample, "__dict__") else {}
        keys = sorted(k for k in vd if not str(k).startswith("_"))
        vals = {k: getattr(sample, k, None) for k in keys[:20]}
    LOG.info("BTC raw contract sample keys: %s", keys)
    LOG.info("BTC raw contract sample values: %s", vals)


def _filter_btc_by_resolve_window(markets: list[Any], he: datetime, tol_sec: float) -> list[Any]:
    out: list[Any] = []
    for m in markets:
        u = parse_market_resolve_utc(m)
        if u is None:
            continue
        if abs((u - he).total_seconds()) <= tol_sec:
            out.append(m)
    return out


def discover_btc_hourly_markets(client: Any, *, hour_end_utc: datetime) -> list[Any]:
    """
    Markets for the BTC hourly event that resolves at hour_end_utc (:00).
    Uses series ticker from env VOL_BTC_HOURLY_SERIES (default KXBTC).
    Paginates with a loose resolve window, groups by ``event_ticker`` when needed,
    then applies a hard per-contract resolve filter (default ±60s).
    When the event mixes ``-B…`` range buckets and ``-T…`` cumulative thresholds, prefers
    the threshold subset (≥3 lines) so mids are naturally monotone. Set
    ``VOL_BTC_NO_THRESHOLD_PREF=1`` to disable and keep all lines.
    """
    series = os.environ.get("VOL_BTC_HOURLY_SERIES", DEFAULT_BTC_HOURLY_SERIES).strip()
    try:
        hard_tol = float(os.environ.get("VOL_BTC_CLOSE_TOLERANCE_SECS", "60").strip() or "60")
    except ValueError:
        hard_tol = 60.0
    hard_tol = max(1.0, min(hard_tol, 600.0))
    page_tol = max(120.0, hard_tol + 90.0)

    raw: list[Any] = []
    cursor = None
    he = hour_end_utc.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    prefix = _btc_event_ticker_prefix(hour_end_utc)
    while True:
        markets, cursor = get_markets_page_raw(
            client,
            limit=200,
            cursor=cursor or None,
            series_ticker=series,
        )
        for m in markets:
            u = parse_market_resolve_utc(m)
            if u is None:
                continue
            if abs((u - he).total_seconds()) > page_tol:
                continue
            raw.append(m)
        if not cursor:
            break
    if not raw:
        return []

    _log_btc_market_list_api_sample(raw)

    override = os.environ.get("VOL_BTC_EVENT_TICKER", "").strip()
    chosen: list[Any]
    if override:
        only_ov = [m for m in raw if str(getattr(m, "event_ticker", "") or "") == override]
        if only_ov:
            chosen = only_ov
        else:
            LOG.warning("BTC discovery: VOL_BTC_EVENT_TICKER=%r matched 0 markets; using prefix/grouping", override)
            chosen = _btc_choose_event_group(raw, prefix, he)
    else:
        chosen = _btc_choose_event_group(raw, prefix, he)

    filtered = _filter_btc_by_resolve_window(chosen, he, hard_tol)
    filtered = _prefer_btc_threshold_contracts(filtered)
    fellback = False
    if len(filtered) < 3:
        LOG.warning(
            "BTC discovery: hard resolve filter (±%.0fs vs %s) left only %d contracts "
            "(from %d). Falling back to pre-filter list; check parse_market_resolve_utc fields.",
            hard_tol,
            he.isoformat(),
            len(filtered),
            len(chosen),
        )
        fellback = True
        _log_btc_discovery_line(pre=chosen, post=chosen, he=he, hard_tol=hard_tol, fellback=True)
        return chosen

    _log_btc_discovery_line(pre=chosen, post=filtered, he=he, hard_tol=hard_tol, fellback=False)
    return filtered


def resolution_date_for_high_scan(local_now: datetime) -> date:
    """HIGH scans run on the resolution calendar day."""
    return local_now.date()


def resolution_date_for_low_scan(local_now: datetime) -> date:
    """LOW evening scans target the next calendar day's low."""
    return (local_now + timedelta(days=1)).date()
