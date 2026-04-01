"""NWS / METAR public API helpers."""

from __future__ import annotations

import os
import re
from datetime import datetime, time as dt_time, timedelta, timezone
from typing import Any, Optional

import pytz
import requests

from config import MAX_FORECAST_AGE_MINUTES

BASE_URL = "https://api.weather.gov"
HEADERS = {
    "User-Agent": os.environ.get(
        "NWS_USER_AGENT",
        "weather-kalshi-bot/1.0 (contact: set NWS_USER_AGENT in .env)",
    ),
    "Accept": "application/geo+json",
}

_GRID_CACHE: dict[str, dict[str, Any]] = {}


def get_grid_point(lat: float, lon: float) -> dict[str, Any]:
    """GET /points/{lat},{lon} — cache per (lat, lon) rounded."""
    key = f"{lat:.4f},{lon:.4f}"
    if key in _GRID_CACHE:
        return _GRID_CACHE[key]
    url = f"{BASE_URL}/points/{lat},{lon}"
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    props = r.json()["properties"]
    out = {
        "gridId": props["gridId"],
        "gridX": props["gridX"],
        "gridY": props["gridY"],
        "forecastHourlyUrl": props["forecastHourly"],
        "timeZone": props["timeZone"],
    }
    _GRID_CACHE[key] = out
    return out


def _parse_iso(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts)


def get_hourly_forecast(grid_info: dict[str, Any]) -> dict[str, Any]:
    url = grid_info["forecastHourlyUrl"]
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    data = r.json()
    gen_raw = data["properties"]["generatedAt"]
    generated_at = _parse_iso(gen_raw)
    if generated_at.tzinfo is None:
        generated_at = generated_at.replace(tzinfo=timezone.utc)
    age_minutes = (datetime.now(timezone.utc) - generated_at).total_seconds() / 60.0
    return {
        "periods": data["properties"]["periods"],
        "generated_at": generated_at,
        "age_minutes": age_minutes,
        "timezone": grid_info["timeZone"],
    }


def get_remaining_day_high(forecast_data: dict[str, Any]) -> Optional[float]:
    """Max forecast temp for remaining hours today (local), from now through end of local day."""
    tz = pytz.timezone(forecast_data["timezone"])
    now_local = datetime.now(tz)
    today_local = now_local.date()
    end_of_day = tz.localize(datetime.combine(today_local, dt_time(23, 59, 59)))

    temps: list[float] = []
    for period in forecast_data["periods"]:
        start = _parse_iso(period["startTime"])
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        start = start.astimezone(tz)
        if start.date() != today_local:
            continue
        if not (now_local <= start <= end_of_day):
            continue
        t = period.get("temperature")
        if t is not None:
            temps.append(float(t))
    return max(temps) if temps else None


def get_latest_observation(station_id: str) -> Optional[dict[str, Any]]:
    url = f"{BASE_URL}/stations/{station_id}/observations/latest"
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    props = r.json()["properties"]
    temp_c = props.get("temperature", {}).get("value")
    if temp_c is None:
        return None
    temp_f = (temp_c * 9 / 5) + 32
    return {
        "temp_f": round(temp_f, 1),
        "timestamp": props.get("timestamp"),
    }


def get_observed_high_today(station_id: str, local_tz_str: str) -> Optional[float]:
    tz = pytz.timezone(local_tz_str)
    now_local = datetime.now(tz)
    local_midnight = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    midnight_utc = local_midnight.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    url = f"{BASE_URL}/stations/{station_id}/observations"
    params = {"start": midnight_utc, "limit": 500}
    r = requests.get(url, headers=HEADERS, params=params, timeout=20)
    r.raise_for_status()
    feats = r.json().get("features") or []
    temps: list[float] = []
    for obs in feats:
        val = obs.get("properties", {}).get("temperature", {}).get("value")
        if val is None:
            continue
        f = (val * 9 / 5) + 32
        if -60 < f < 130:
            temps.append(f)
    return round(max(temps), 1) if temps else None


def get_projected_high(
    city_key: str, grid_info: dict[str, Any], metar_station: str
) -> Optional[dict[str, Any]]:
    forecast = get_hourly_forecast(grid_info)
    forecast_high = get_remaining_day_high(forecast)
    latest_obs = get_latest_observation(metar_station)
    observed_high = get_observed_high_today(metar_station, grid_info["timeZone"])

    candidates: list[float] = []
    if forecast_high is not None:
        candidates.append(forecast_high)
    if latest_obs:
        candidates.append(latest_obs["temp_f"])
    if observed_high is not None:
        candidates.append(observed_high)

    if not candidates:
        return None

    projected = max(candidates)
    stale = forecast["age_minutes"] > MAX_FORECAST_AGE_MINUTES

    return {
        "city": city_key,
        "projected_high": projected,
        "forecast_high": forecast_high,
        "observed_high_so_far": observed_high,
        "current_temp": latest_obs["temp_f"] if latest_obs else None,
        "forecast_age_minutes": forecast["age_minutes"],
        "is_stale": stale,
    }


def has_severe_weather_blackout(state_code: str) -> bool:
    """True if active alerts suggest skipping trading (tornado watch, severe t-storm, hurricane)."""
    url = f"{BASE_URL}/alerts/active"
    params = {"area": state_code}
    r = requests.get(url, headers=HEADERS, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    feats = data.get("features") or []
    bad = re.compile(
        r"tornado watch|severe thunderstorm|hurricane",
        re.IGNORECASE,
    )
    for f in feats:
        props = f.get("properties") or {}
        ev = (props.get("event") or "") + " " + (props.get("headline") or "")
        if bad.search(ev):
            return True
    return False
