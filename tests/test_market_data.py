"""
Unit tests for market data management.
"""

import pytest
import time
from src.market_data import OrderBook, RollingBuffer


def test_orderbook_creation():
    """Test OrderBook creation and validation."""
    ob = OrderBook(
        ticker="TEST",
        timestamp=1000.0,
        yes_bid=0.50,
        yes_ask=0.52,
        no_bid=0.48,
        no_ask=0.50
    )

    assert ob.ticker == "TEST"
    assert ob.yes_bid == 0.50
    assert ob.yes_ask == 0.52


def test_orderbook_mid():
    """Test MID calculation."""
    ob = OrderBook(
        ticker="TEST",
        timestamp=1000.0,
        yes_bid=0.50,
        yes_ask=0.52,
        no_bid=0.48,
        no_ask=0.50
    )

    assert ob.mid == 0.51  # (0.50 + 0.52) / 2


def test_orderbook_spread():
    """Test spread calculation."""
    ob = OrderBook(
        ticker="TEST",
        timestamp=1000.0,
        yes_bid=0.50,
        yes_ask=0.52,
        no_bid=0.48,
        no_ask=0.50
    )

    assert abs(ob.spread - 0.02) < 1e-10  # 0.52 - 0.50 with float tolerance


def test_orderbook_invalid_price():
    """Test that invalid prices raise ValueError."""
    with pytest.raises(ValueError):
        OrderBook(
            ticker="TEST",
            timestamp=1000.0,
            yes_bid=1.5,  # Invalid: > 1.0
            yes_ask=0.52,
            no_bid=0.48,
            no_ask=0.50
        )


def test_rolling_buffer_add():
    """Test adding values to rolling buffer."""
    buffer = RollingBuffer(max_seconds=60)

    buffer.add(0.50, 1000.0)
    buffer.add(0.51, 1001.0)
    buffer.add(0.52, 1002.0)

    assert len(buffer) == 3
    assert buffer.get_all_values() == [0.50, 0.51, 0.52]


def test_rolling_buffer_pruning():
    """Test that old data is pruned."""
    buffer = RollingBuffer(max_seconds=5)

    # Add data at t=0, 1, 2, 3, 4, 5
    for i in range(6):
        buffer.add(float(i), float(i))

    # Add data at t=10 (should prune everything before t=5)
    buffer.add(10.0, 10.0)

    values = buffer.get_all_values()
    assert 10.0 in values
    assert 5.0 in values
    assert 0.0 not in values  # Should be pruned


def test_rolling_buffer_get_value_at():
    """Test getting value from N seconds ago."""
    buffer = RollingBuffer(max_seconds=60)

    buffer.add(0.50, 1000.0)
    buffer.add(0.51, 1005.0)
    buffer.add(0.52, 1010.0)

    # Get value from 10 seconds ago (current time = 1010)
    value = buffer.get_value_at(10.0, 1010.0)
    assert value == 0.50  # Closest to t=1000

    # Get value from 5 seconds ago
    value = buffer.get_value_at(5.0, 1010.0)
    assert value == 0.51  # Closest to t=1005


def test_rolling_buffer_empty():
    """Test operations on empty buffer."""
    buffer = RollingBuffer(max_seconds=60)

    assert buffer.is_empty is True
    assert len(buffer) == 0
    assert buffer.get_value_at(10.0, 1000.0) is None
    assert buffer.get_all_values() == []


def test_rolling_buffer_get_values_since():
    """Test getting values from last N seconds."""
    buffer = RollingBuffer(max_seconds=60)

    buffer.add(0.50, 1000.0)
    buffer.add(0.51, 1005.0)
    buffer.add(0.52, 1010.0)

    # Get values from last 7 seconds (current time = 1010)
    values = buffer.get_values_since(7.0, 1010.0)
    assert 0.51 in values  # t=1005
    assert 0.52 in values  # t=1010
    assert 0.50 not in values  # t=1000 is more than 7s ago
