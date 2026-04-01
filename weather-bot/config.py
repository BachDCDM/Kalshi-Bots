"""City configs and trading constants."""

from __future__ import annotations

CITIES = {
    "LA": {
        "lat": 34.0522,
        "lon": -118.2437,
        "metar_station": "KLAX",
        "kalshi_name_variants": ["LA", "Los Angeles"],
        "alert_area": "CA",
    },
    "NYC": {
        "lat": 40.7128,
        "lon": -74.0060,
        # KNYC is not a valid METAR ICAO; JFK is a major reporting station for NYC area.
        "metar_station": "KJFK",
        "kalshi_name_variants": ["NYC", "New York"],
        "alert_area": "NY",
    },
    "CHI": {
        "lat": 41.8781,
        "lon": -87.6298,
        "metar_station": "KORD",
        "kalshi_name_variants": ["Chicago", "CHI"],
        "alert_area": "IL",
    },
    "MIA": {
        "lat": 25.7617,
        "lon": -80.1918,
        "metar_station": "KMIA",
        "kalshi_name_variants": ["Miami", "MIA"],
        "alert_area": "FL",
    },
    "DAL": {
        "lat": 32.7767,
        "lon": -96.7970,
        "metar_station": "KDFW",
        "kalshi_name_variants": ["Dallas", "DAL"],
        "alert_area": "TX",
    },
}

MIN_EDGE = 0.08
MIN_DISTANCE_FROM_BUCKET_EDGE = 1.5
MAX_SPREAD_CENTS = 6

TRADE_WINDOW_START_HOUR = 10
TRADE_WINDOW_END_HOUR = 15

EARLY_EXIT_PROFIT_CENTS = 20
EARLY_EXIT_MIN_HOURS_REMAINING = 2

MAX_FORECAST_AGE_MINUTES = 90

MAX_DAILY_LOSS_CENTS = 30_000  # $300
MAX_CONCURRENT_OPEN_TRADES = 5

DEFAULT_CONTRACTS = 10

# Calendar day boundary for daily loss limit (US equities-style trading day).
RISK_DAILY_TIMEZONE = "America/New_York"
