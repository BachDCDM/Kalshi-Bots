"""
Unit tests for signal calculations.
"""

import pytest
import numpy as np
from unittest.mock import MagicMock
from src.signals import SignalCalculator
from src.config import Config
from src.market_data import MarketDataManager, OrderBook


@pytest.fixture
def mock_config():
    """Create a mock config with test parameters."""
    config = MagicMock(spec=Config)
    config.BASELINE_WINDOW = 120
    config.VOLATILITY_WINDOW = 60
    config.SHOCK_WINDOW = 10
    config.MIN_SHOCK = 0.06
    config.SHOCK_MULTIPLIER = 3.0
    config.MIN_DEVIATION = 0.04
    config.DEVIATION_MULTIPLIER = 2.0
    config.EXIT_BAND_FLOOR = 0.015
    return config


@pytest.fixture
def mock_logger():
    """Create a mock logger."""
    logger = MagicMock()
    return logger


@pytest.fixture
def data_manager_with_flat_data(mock_config, mock_logger):
    """Create data manager with flat (zero volatility) data."""
    mgr = MarketDataManager(mock_config, mock_logger)

    # Add 120 seconds of flat data
    for i in range(120):
        ob = OrderBook(
            ticker="TEST",
            timestamp=float(i),
            yes_bid=0.50,
            yes_ask=0.52,
            no_bid=0.48,
            no_ask=0.50
        )
        mgr.update(ob)

    return mgr


@pytest.fixture
def data_manager_with_volatile_data(mock_config, mock_logger):
    """Create data manager with volatile data."""
    mgr = MarketDataManager(mock_config, mock_logger)

    # Add data with high volatility
    for i in range(120):
        price = 0.50 + (0.02 if i % 2 == 0 else -0.02)
        ob = OrderBook(
            ticker="TEST",
            timestamp=float(i),
            yes_bid=price,
            yes_ask=price + 0.02,
            no_bid=0.48,
            no_ask=0.50
        )
        mgr.update(ob)

    return mgr


def test_calculate_baseline_with_flat_data(mock_config, mock_logger, data_manager_with_flat_data):
    """Test baseline calculation with flat data."""
    calc = SignalCalculator(mock_config, data_manager_with_flat_data, mock_logger)
    baseline = calc.calculate_baseline()

    assert baseline is not None
    assert abs(baseline - 0.51) < 0.01  # Should be close to MID


def test_calculate_vol_60_with_flat_data(mock_config, mock_logger, data_manager_with_flat_data):
    """Test volatility with zero-volatility data."""
    calc = SignalCalculator(mock_config, data_manager_with_flat_data, mock_logger)
    vol = calc.calculate_vol_60()

    assert vol is not None
    assert vol < 0.001  # Should be very small


def test_calculate_vol_60_with_volatile_data(mock_config, mock_logger, data_manager_with_volatile_data):
    """Test volatility with high-volatility data."""
    calc = SignalCalculator(mock_config, data_manager_with_volatile_data, mock_logger)
    vol = calc.calculate_vol_60()

    assert vol is not None
    assert vol > 0.01  # Should detect volatility


def test_shock_threshold_uses_floor(mock_config, mock_logger, data_manager_with_flat_data):
    """Test that shock threshold uses floor when volatility is low."""
    calc = SignalCalculator(mock_config, data_manager_with_flat_data, mock_logger)
    shock_th = calc.calculate_shock_threshold()

    assert shock_th == mock_config.MIN_SHOCK  # Should use floor


def test_delta_threshold_uses_floor(mock_config, mock_logger, data_manager_with_flat_data):
    """Test that delta threshold uses floor when volatility is low."""
    calc = SignalCalculator(mock_config, data_manager_with_flat_data, mock_logger)
    delta_th = calc.calculate_delta_threshold()

    assert delta_th == mock_config.MIN_DEVIATION  # Should use floor


def test_exit_band_uses_floor(mock_config, mock_logger, data_manager_with_flat_data):
    """Test that exit band uses floor when volatility is low."""
    calc = SignalCalculator(mock_config, data_manager_with_flat_data, mock_logger)
    exit_band = calc.calculate_exit_band()

    assert exit_band == mock_config.EXIT_BAND_FLOOR  # Should use floor


def test_shock_threshold_scales_with_volatility(mock_config, mock_logger, data_manager_with_volatile_data):
    """Test that shock threshold scales with volatility."""
    calc = SignalCalculator(mock_config, data_manager_with_volatile_data, mock_logger)
    vol = calc.calculate_vol_60()
    shock_th = calc.calculate_shock_threshold(vol)

    expected = max(mock_config.MIN_SHOCK, mock_config.SHOCK_MULTIPLIER * vol)
    assert abs(shock_th - expected) < 0.001


def test_get_all_signals(mock_config, mock_logger, data_manager_with_flat_data):
    """Test that get_all_signals returns complete dict."""
    calc = SignalCalculator(mock_config, data_manager_with_flat_data, mock_logger)
    signals = calc.get_all_signals()

    # Check all required keys present
    assert 'mid' in signals
    assert 'baseline' in signals
    assert 'vol_60' in signals
    assert 'ret_10' in signals
    assert 'delta' in signals
    assert 'shock_th' in signals
    assert 'delta_th' in signals
    assert 'exit_band' in signals
    assert 'timestamp' in signals


def test_calculate_ret_10_insufficient_data(mock_config, mock_logger):
    """Test that ret_10 returns 0 with flat data over insufficient time."""
    mgr = MarketDataManager(mock_config, mock_logger)

    # Add only 5 seconds of flat data
    for i in range(5):
        ob = OrderBook(
            ticker="TEST",
            timestamp=float(i),
            yes_bid=0.50,
            yes_ask=0.52,
            no_bid=0.48,
            no_ask=0.50
        )
        mgr.update(ob)

    calc = SignalCalculator(mock_config, mgr, mock_logger)
    ret_10 = calc.calculate_ret_10()

    # With flat data, return should be 0 (price hasn't changed)
    # Buffer returns closest available data when requested time not available
    assert ret_10 is not None
    assert abs(ret_10) < 0.001  # Should be ~0 for flat data
