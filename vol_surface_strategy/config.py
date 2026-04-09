"""Canonical city schedules, timezones, and static climatology for plausibility checks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

# --- BTC hourly discovery ---
# VOL_BTC_HOURLY_SERIES — series ticker (default KXBTC).
# VOL_BTC_EVENT_TICKER — exact event_ticker if prefix matching is ambiguous.
# VOL_BTC_CLOSE_TOLERANCE_SECS — per-contract resolve time must be within this many seconds of hour_end_utc (default 60).
# VOL_BTC_NO_THRESHOLD_PREF=1 — do not drop -B range buckets when -T cumulative lines exist.
DEFAULT_BTC_HOURLY_SERIES = "KXBTC"

# --- Trading gates (vol surface runner) ---
MIN_EDGE_CENTS = 5.0
MAX_SPREAD_ENTRY_CENTS = 4.0  # YES_ask − YES_bid; abort if greater than this

# --- Range-bucket weather (CDF still uses full ladder; these gate surface inputs only) ---
# Marginal bucket mid must lie in this band for its derived threshold row to enter the surface.
RANGE_BUCKET_RAW_MID_MIN_CENTS = 3.0
RANGE_BUCKET_RAW_MID_MAX_CENTS = 97.0
# Derived P(T≥K) mids for surface rows (CDF tails can sit near 0% or 100%).
RANGE_BUCKET_DERIVED_MID_MIN_CENTS = 0.5
RANGE_BUCKET_DERIVED_MID_MAX_CENTS = 99.5

# Weather liquidity gate (thin ladders; book depth is often <5k even when tradeable)
WEATHER_GATE2_MIN_MID_VOL = 1000.0
WEATHER_GATE2_MIN_VOL_FP = 1000.0
# Thin tails often land just under 1k resting size; keep slightly below headline 1k.
WEATHER_GATE2_MIN_BOOK_SZ = 800.0
# Strike-adjacent contracts (consecutive in sorted c1) need wider Φ⁻¹ separation than farther pairs.
RANGE_BUCKET_ADJACENT_MIN_DZ = 0.15

# --- Climatology: rough monthly mean HIGH for μ* plausibility (°F), by city_id + month 1-12 ---
# Simplified norms; refine from NOAA if needed.
_CLIM_HIGH: dict[str, dict[int, float]] = {
    "NYC": {1: 39, 2: 42, 3: 52, 4: 63, 5: 73, 6: 82, 7: 87, 8: 85, 9: 78, 10: 67, 11: 55, 12: 45},
    "CHI": {1: 32, 2: 36, 3: 47, 4: 59, 5: 70, 6: 80, 7: 85, 8: 83, 9: 76, 10: 63, 11: 49, 12: 37},
    "MIA": {1: 76, 2: 78, 3: 80, 4: 83, 5: 86, 6: 89, 7: 91, 8: 91, 9: 89, 10: 86, 11: 81, 12: 78},
    "AUS": {1: 62, 2: 66, 3: 73, 4: 80, 5: 87, 6: 92, 7: 96, 8: 97, 9: 90, 10: 82, 11: 72, 12: 64},
    "LAX": {1: 68, 2: 68, 3: 69, 4: 71, 5: 73, 6: 76, 7: 79, 8: 80, 9: 79, 10: 76, 11: 72, 12: 67},
    "PHL": {1: 41, 2: 44, 3: 53, 4: 65, 5: 75, 6: 84, 7: 89, 8: 87, 9: 80, 10: 69, 11: 57, 12: 46},
    "DEN": {1: 45, 2: 47, 3: 56, 4: 62, 5: 72, 6: 82, 7: 88, 8: 86, 9: 79, 10: 66, 11: 54, 12: 45},
    "PHX": {1: 67, 2: 71, 3: 77, 4: 85, 5: 95, 6: 104, 7: 106, 8: 105, 9: 100, 10: 89, 11: 76, 12: 66},
    "MSP": {1: 24, 2: 29, 3: 41, 4: 58, 5: 71, 6: 80, 7: 85, 8: 82, 9: 73, 10: 58, 11: 42, 12: 28},
    "BNA": {1: 49, 2: 54, 3: 63, 4: 73, 5: 81, 6: 88, 7: 91, 8: 90, 9: 85, 10: 74, 11: 62, 12: 52},
    "MSY": {1: 62, 2: 66, 3: 73, 4: 79, 5: 86, 6: 90, 7: 92, 8: 92, 9: 88, 10: 80, 11: 72, 12: 65},
    "OKC": {1: 50, 2: 55, 3: 64, 4: 73, 5: 81, 6: 90, 7: 95, 8: 94, 9: 86, 10: 74, 11: 61, 12: 51},
    "SAT": {1: 63, 2: 67, 3: 74, 4: 81, 5: 88, 6: 93, 7: 96, 8: 97, 9: 90, 10: 82, 11: 73, 12: 65},
    "TPA": {1: 70, 2: 73, 3: 77, 4: 82, 5: 88, 6: 90, 7: 91, 8: 91, 9: 89, 10: 85, 11: 78, 12: 72},
}


def climatology_mean_high(city_id: str, month: int) -> float:
    m = max(1, min(12, month))
    tab = _CLIM_HIGH.get(city_id, _CLIM_HIGH["NYC"])
    return tab.get(m, tab.get(6, 70.0))


# Mean daily minimum (°F) for μ* plausibility on LOW markets — offset from monthly mean high by typical diurnal range.
_CLIM_LOW_OFFSET_F: dict[str, float] = {
    "NYC": 19.0,
    "CHI": 20.0,
    "MIA": 11.0,
    "AUS": 16.0,
    "LAX": 14.0,
    "PHL": 20.0,
    "DEN": 16.0,
    "PHX": 15.0,
    "MSP": 19.0,
    "BNA": 17.0,
    "MSY": 14.0,
    "OKC": 17.0,
    "SAT": 16.0,
    "TPA": 12.0,
}


def climatology_mean_low(city_id: str, month: int) -> float:
    """Rough monthly mean daily minimum (°F) for LOW-temperature μ* checks."""
    m = max(1, min(12, month))
    high = climatology_mean_high(city_id, m)
    off = _CLIM_LOW_OFFSET_F.get(city_id, 18.0)
    return max(5.0, high - off)


@dataclass(frozen=True)
class CitySchedule:
    city_id: str
    station: str
    tz_name: str
    kalshi_name_variants: tuple[str, ...]
    # Kalshi daily temp series (GET /markets?series_ticker=…). None = catalog scan only.
    kalshi_high_series: Optional[str]
    kalshi_low_series: Optional[str]
    # Local (hour, minute) for HIGH scans
    high_first: tuple[int, int]
    high_second: Optional[tuple[int, int]]  # None = no second scan
    high_cutoff: tuple[int, int]
    # Extra substrings for catalog discovery when series tickers are unknown (BNA/TPA, etc.).
    # Also: VOL_WEATHER_SERIES_{CITY_ID}_HIGH / _LOW (e.g. VOL_WEATHER_SERIES_BNA_HIGH).
    discovery_blob_tokens: tuple[str, ...] = ()


# All times local per spec
CITIES: dict[str, CitySchedule] = {
    "NYC": CitySchedule(
        "NYC", "KNYC", "America/New_York", ("New York", "NYC", "KNYC"),
        "KXHIGHNY", "KXLOWTNYC",
        (10, 0), (13, 0), (14, 30),
    ),
    "CHI": CitySchedule(
        "CHI", "KORD", "America/Chicago", ("Chicago", "CHI", "KORD"),
        "KXHIGHCHI", "KXLOWTCHI",
        (10, 0), (13, 0), (14, 30),
    ),
    "MIA": CitySchedule(
        "MIA", "KMIA", "America/New_York", ("Miami", "MIA", "KMIA"),
        "KXHIGHMIA", "KXLOWTMIA",
        (10, 0), (13, 0), (14, 30),
    ),
    "AUS": CitySchedule(
        "AUS", "KAUS", "America/Chicago", ("Austin", "AUS", "KAUS"),
        "KXHIGHAUS", "KXLOWTAUS",
        (10, 0), (13, 30), (15, 0),
    ),
    "LAX": CitySchedule(
        "LAX", "KLAX", "America/Los_Angeles", ("Los Angeles", "LA", "KLAX"),
        "KXHIGHLAX", "KXLOWTLAX",
        (7, 0), None, (9, 0),
    ),
    "PHL": CitySchedule(
        "PHL", "KPHL", "America/New_York", ("Philadelphia", "PHL", "KPHL"),
        "KXHIGHPHIL", "KXLOWTPHIL",
        (10, 0), (13, 0), (14, 30),
    ),
    "DEN": CitySchedule(
        "DEN", "KDEN", "America/Denver", ("Denver", "DEN", "KDEN"),
        "KXHIGHDEN", "KXLOWTDEN",
        (10, 0), (13, 0), (14, 30),
    ),
    "PHX": CitySchedule(
        "PHX", "KPHX", "America/Phoenix", ("Phoenix", "PHX", "KPHX"),
        "KXHIGHTPHX", "KXLOWTPHX",
        (10, 0), (13, 30), (15, 30),
    ),
    "MSP": CitySchedule(
        "MSP", "KMSP", "America/Chicago", ("Minneapolis", "MSP", "KMSP"),
        "KXHIGHTMIN", "KXLOWTMIN",
        (10, 0), (13, 0), (14, 30),
    ),
    "BNA": CitySchedule(
        "BNA", "KBNA", "America/Chicago", ("Nashville", "BNA", "KBNA"),
        None,
        None,
        (10, 0), (13, 0), (14, 30),
        ("nashville", "music city"),
    ),
    "MSY": CitySchedule(
        "MSY", "KMSY", "America/Chicago", ("New Orleans", "MSY", "KMSY", "NOLA"),
        "KXHIGHTNOLA", "KXLOWTNOLA",
        (10, 0), (13, 0), (14, 30),
    ),
    "OKC": CitySchedule(
        "OKC", "KOKC", "America/Chicago", ("Oklahoma City", "OKC", "KOKC"),
        "KXHIGHTOKC", "KXLOWTOKC",
        (10, 0), (13, 0), (14, 30),
    ),
    "SAT": CitySchedule(
        "SAT", "KSAT", "America/Chicago", ("San Antonio", "SAT", "KSAT"),
        "KXHIGHTSATX",
        "KXLOWTSATX",
        (10, 0), (13, 30), (15, 0),
    ),
    "TPA": CitySchedule(
        "TPA", "KTPA", "America/New_York", ("Tampa", "TPA", "KTPA"),
        None,
        None,
        (10, 0), (13, 0), (14, 30),
        ("tampa", "tampa bay"),
    ),
}

# LOW: same wall times for every city (local)
LOW_FIRST_LOCAL = (21, 0)
LOW_SECOND_LOCAL = (23, 0)
LOW_CUTOFF_LOCAL = (2, 0)
