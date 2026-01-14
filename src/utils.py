"""
Utility functions for Kalshi trading bot.

Includes time helpers, price normalization, and mathematical utilities.
"""

import time
from datetime import datetime, timezone
from typing import Optional


def get_timestamp() -> float:
    """
    Get current Unix timestamp.

    Returns:
        Current time as Unix timestamp (seconds since epoch)
    """
    return time.time()


def format_timestamp(ts: float) -> str:
    """
    Convert Unix timestamp to human-readable format.

    Args:
        ts: Unix timestamp

    Returns:
        Formatted timestamp string (ISO 8601 format)
    """
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def calculate_tts(market_close_time: datetime) -> float:
    """
    Calculate time-to-settlement in seconds.

    Args:
        market_close_time: Market close time as datetime

    Returns:
        Seconds until market closes (can be negative if already closed)
    """
    now = datetime.now(timezone.utc)

    # Ensure market_close_time is timezone-aware
    if market_close_time.tzinfo is None:
        market_close_time = market_close_time.replace(tzinfo=timezone.utc)

    delta = market_close_time - now
    return delta.total_seconds()


def normalize_price(price_cents: int) -> float:
    """
    Convert Kalshi price from cents to decimal.

    Kalshi API returns prices as integers 0-100 (cents).
    Internally we use decimal 0.0-1.0 for calculations.

    Args:
        price_cents: Price in cents (0-100)

    Returns:
        Price in decimal (0.0-1.0)

    Example:
        >>> normalize_price(50)
        0.50
        >>> normalize_price(100)
        1.00
    """
    return price_cents / 100.0


def denormalize_price(price_decimal: float) -> int:
    """
    Convert price from decimal to Kalshi cents.

    Args:
        price_decimal: Price in decimal (0.0-1.0)

    Returns:
        Price in cents (0-100)

    Example:
        >>> denormalize_price(0.50)
        50
        >>> denormalize_price(0.523)
        52
    """
    return int(price_decimal * 100)


def safe_divide(a: float, b: float, default: float = 0.0) -> float:
    """
    Division with zero-check.

    Args:
        a: Numerator
        b: Denominator
        default: Value to return if b is zero

    Returns:
        a / b if b != 0, else default
    """
    if b == 0:
        return default
    return a / b


def ema_alpha(span: int) -> float:
    """
    Calculate EMA alpha from span.

    Formula: alpha = 2 / (span + 1)

    Args:
        span: EMA span (e.g., 120 for 2-minute window)

    Returns:
        EMA alpha parameter

    Example:
        >>> ema_alpha(120)
        0.016528925619834
    """
    return 2.0 / (span + 1)


def clamp(value: float, min_value: float, max_value: float) -> float:
    """
    Clamp value to range [min_value, max_value].

    Args:
        value: Value to clamp
        min_value: Minimum allowed value
        max_value: Maximum allowed value

    Returns:
        Clamped value

    Example:
        >>> clamp(0.5, 0.0, 1.0)
        0.5
        >>> clamp(1.5, 0.0, 1.0)
        1.0
        >>> clamp(-0.5, 0.0, 1.0)
        0.0
    """
    return max(min_value, min(value, max_value))


def validate_price(price: float) -> bool:
    """
    Check if price is in valid range [0, 1].

    Args:
        price: Price to validate

    Returns:
        True if price is valid, False otherwise
    """
    return 0 <= price <= 1


def format_pnl(pnl: float) -> str:
    """
    Format P&L for display.

    Args:
        pnl: P&L value

    Returns:
        Formatted P&L string with sign

    Example:
        >>> format_pnl(0.05)
        '+$0.05'
        >>> format_pnl(-0.03)
        '-$0.03'
    """
    sign = "+" if pnl >= 0 else "-"
    return f"{sign}${abs(pnl):.2f}"


def milliseconds_to_timestamp() -> str:
    """
    Get current timestamp in milliseconds as string.

    Used for Kalshi API authentication.

    Returns:
        Current time in milliseconds as string
    """
    return str(int(time.time() * 1000))
