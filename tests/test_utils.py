"""
Unit tests for utility functions.
"""

import pytest
from datetime import datetime, timezone
from src.utils import (
    normalize_price,
    denormalize_price,
    calculate_tts,
    ema_alpha,
    clamp,
    validate_price,
    safe_divide
)


def test_normalize_price():
    """Test price normalization from cents to decimal."""
    assert normalize_price(50) == 0.50
    assert normalize_price(100) == 1.00
    assert normalize_price(0) == 0.00
    assert normalize_price(75) == 0.75


def test_denormalize_price():
    """Test price conversion from decimal to cents."""
    assert denormalize_price(0.50) == 50
    assert denormalize_price(1.00) == 100
    assert denormalize_price(0.00) == 0
    assert denormalize_price(0.523) == 52  # Rounds down


def test_price_round_trip():
    """Test that price conversion round-trips correctly."""
    for cents in [0, 25, 50, 75, 100]:
        decimal = normalize_price(cents)
        back_to_cents = denormalize_price(decimal)
        assert back_to_cents == cents


def test_calculate_tts():
    """Test time-to-settlement calculation."""
    now = datetime.now(timezone.utc)

    # Future time
    future = datetime.now(timezone.utc).replace(hour=23, minute=59)
    tts = calculate_tts(future)
    assert tts > 0

    # Past time
    past = datetime.now(timezone.utc).replace(year=2020)
    tts = calculate_tts(past)
    assert tts < 0


def test_ema_alpha():
    """Test EMA alpha calculation."""
    # For 120-second window
    alpha = ema_alpha(120)
    expected = 2.0 / 121
    assert abs(alpha - expected) < 1e-10

    # For 60-second window
    alpha = ema_alpha(60)
    expected = 2.0 / 61
    assert abs(alpha - expected) < 1e-10


def test_clamp():
    """Test value clamping."""
    assert clamp(0.5, 0.0, 1.0) == 0.5
    assert clamp(1.5, 0.0, 1.0) == 1.0
    assert clamp(-0.5, 0.0, 1.0) == 0.0
    assert clamp(0.0, 0.0, 1.0) == 0.0
    assert clamp(1.0, 0.0, 1.0) == 1.0


def test_validate_price():
    """Test price validation."""
    assert validate_price(0.5) is True
    assert validate_price(0.0) is True
    assert validate_price(1.0) is True
    assert validate_price(-0.1) is False
    assert validate_price(1.1) is False


def test_safe_divide():
    """Test safe division with zero check."""
    assert safe_divide(10, 2) == 5.0
    assert safe_divide(10, 0) == 0.0
    assert safe_divide(10, 0, default=1.0) == 1.0
