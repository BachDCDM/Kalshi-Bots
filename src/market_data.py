"""
Market data management for Kalshi trading bot.

Includes OrderBook dataclass, RollingBuffer for time-series data,
and MarketDataManager for tracking market state.
"""

import time
from collections import deque
from dataclasses import dataclass
from typing import Optional

import numpy as np

from src.logger import TradingLogger
from src.utils import normalize_price


@dataclass
class OrderBook:
    """Immutable snapshot of order book state."""

    ticker: str
    timestamp: float
    yes_bid: Optional[float]  # Normalized to decimal [0, 1]
    yes_ask: Optional[float]
    no_bid: Optional[float]
    no_ask: Optional[float]

    @property
    def mid(self) -> Optional[float]:
        """
        Calculate MID price: (YES_BID + YES_ASK) / 2.

        Returns:
            MID price or None if bids/asks missing
        """
        if self.yes_bid is not None and self.yes_ask is not None:
            return (self.yes_bid + self.yes_ask) / 2
        return None

    @property
    def spread(self) -> Optional[float]:
        """
        Calculate bid-ask spread.

        Returns:
            Spread or None if bids/asks missing
        """
        if self.yes_bid is not None and self.yes_ask is not None:
            return self.yes_ask - self.yes_bid
        return None

    def __post_init__(self):
        """Validate prices are in [0, 1] range."""
        for price in [self.yes_bid, self.yes_ask, self.no_bid, self.no_ask]:
            if price is not None and not (0 <= price <= 1):
                raise ValueError(f"Price {price} out of range [0, 1]")

    @classmethod
    def from_kalshi_data(cls, data: dict, ticker: str, timestamp: float) -> "OrderBook":
        """
        Create OrderBook from Kalshi API data.

        Args:
            data: Kalshi orderbook data
            ticker: Market ticker
            timestamp: Timestamp

        Returns:
            OrderBook instance
        """
        orderbook_data = data.get("orderbook", {})
        yes_data = orderbook_data.get("yes", [])
        no_data = orderbook_data.get("no", [])

        # Extract best bid/ask (prices are in cents)
        yes_bid = normalize_price(yes_data[0][0]) if yes_data and len(yes_data[0]) > 0 else None
        yes_ask = normalize_price(yes_data[0][1]) if yes_data and len(yes_data[0]) > 1 else None
        no_bid = normalize_price(no_data[0][0]) if no_data and len(no_data[0]) > 0 else None
        no_ask = normalize_price(no_data[0][1]) if no_data and len(no_data[0]) > 1 else None

        return cls(
            ticker=ticker,
            timestamp=timestamp,
            yes_bid=yes_bid,
            yes_ask=yes_ask,
            no_bid=no_bid,
            no_ask=no_ask
        )


class RollingBuffer:
    """Fixed-size circular buffer with timestamp indexing."""

    def __init__(self, max_seconds: int):
        """
        Initialize rolling buffer.

        Args:
            max_seconds: Maximum age of data to keep in seconds
        """
        self.max_seconds = max_seconds
        self.values = deque()  # Store values
        self.timestamps = deque()  # Store corresponding timestamps

    def add(self, value: float, timestamp: float):
        """
        Add value and prune old data.

        Args:
            value: Value to add
            timestamp: Timestamp for value
        """
        self.values.append(value)
        self.timestamps.append(timestamp)

        # Prune data older than max_seconds
        cutoff = timestamp - self.max_seconds
        while self.timestamps and self.timestamps[0] < cutoff:
            self.timestamps.popleft()
            self.values.popleft()

    def get_value_at(self, seconds_ago: float, current_time: float) -> Optional[float]:
        """
        Get value closest to N seconds ago.

        Args:
            seconds_ago: How many seconds ago
            current_time: Current timestamp

        Returns:
            Value closest to target time or None if buffer empty
        """
        if not self.timestamps:
            return None

        target_time = current_time - seconds_ago

        # Convert to numpy arrays for searchsorted
        timestamps_array = np.array(list(self.timestamps))
        idx = np.searchsorted(timestamps_array, target_time)

        if idx == 0:
            return self.values[0]
        if idx >= len(self.values):
            return self.values[-1]

        # Return closer of two adjacent values
        before_diff = abs(self.timestamps[idx - 1] - target_time)
        after_diff = abs(self.timestamps[idx] - target_time)

        if before_diff < after_diff:
            return self.values[idx - 1]
        return self.values[idx]

    def get_all_values(self) -> list[float]:
        """
        Return all values in buffer.

        Returns:
            List of all values
        """
        return list(self.values)

    def get_values_since(self, seconds_ago: float, current_time: float) -> list[float]:
        """
        Get all values from the last N seconds.

        Args:
            seconds_ago: How many seconds back
            current_time: Current timestamp

        Returns:
            List of values from last N seconds
        """
        cutoff = current_time - seconds_ago
        result = []
        for ts, val in zip(self.timestamps, self.values):
            if ts >= cutoff:
                result.append(val)
        return result

    def __len__(self) -> int:
        """Return number of items in buffer."""
        return len(self.values)

    @property
    def is_empty(self) -> bool:
        """Check if buffer is empty."""
        return len(self.values) == 0


class MarketDataManager:
    """Manages rolling buffers and data staleness detection."""

    def __init__(self, config, logger: TradingLogger):
        """
        Initialize market data manager.

        Args:
            config: Bot configuration
            logger: Trading logger
        """
        self.config = config
        self.logger = logger

        # Rolling buffers (with extra capacity for safety)
        self.mid_buffer = RollingBuffer(max_seconds=config.BASELINE_WINDOW + 30)
        self.mid_changes_buffer = RollingBuffer(max_seconds=config.VOLATILITY_WINDOW + 10)

        # State tracking
        self.last_update_time: Optional[float] = None
        self.last_mid: Optional[float] = None
        self.current_orderbook: Optional[OrderBook] = None

    def update(self, orderbook: OrderBook):
        """
        Process new order book update.

        Args:
            orderbook: New order book snapshot
        """
        current_mid = orderbook.mid

        if current_mid is None:
            self.logger.log_error(
                "invalid_orderbook",
                "Orderbook has no valid MID",
                context={"orderbook": str(orderbook)}
            )
            return

        # Add MID to buffer
        self.mid_buffer.add(current_mid, orderbook.timestamp)

        # Calculate 1-second MID change
        if self.last_mid is not None and self.last_update_time is not None:
            time_delta = orderbook.timestamp - self.last_update_time
            # Accept time deltas approximately 1 second (0.5s - 1.5s)
            if 0.5 <= time_delta <= 1.5:
                mid_change = current_mid - self.last_mid
                self.mid_changes_buffer.add(mid_change, orderbook.timestamp)

        # Update state
        self.last_update_time = orderbook.timestamp
        self.last_mid = current_mid
        self.current_orderbook = orderbook

    def is_stale(self, threshold_seconds: float) -> bool:
        """
        Check if data is stale.

        Args:
            threshold_seconds: Staleness threshold

        Returns:
            True if data is stale
        """
        if self.last_update_time is None:
            return True
        return (time.time() - self.last_update_time) > threshold_seconds

    def has_sufficient_data(self) -> bool:
        """
        Check if we have enough data for signal calculations.

        Need 120s of MID history and 60s of volatility history.

        Returns:
            True if sufficient data available
        """
        # Need at least 60 MID data points and 30 change measurements
        return (
            len(self.mid_buffer) >= 60 and
            len(self.mid_changes_buffer) >= 30
        )

    def get_mid_at(self, seconds_ago: float) -> Optional[float]:
        """
        Get MID from N seconds ago.

        Args:
            seconds_ago: How many seconds ago

        Returns:
            MID value or None if not available
        """
        if self.last_update_time is None:
            return None
        return self.mid_buffer.get_value_at(seconds_ago, self.last_update_time)

    def get_all_mids(self) -> list[float]:
        """
        Get all MID values.

        Returns:
            List of all MID values
        """
        return self.mid_buffer.get_all_values()

    def get_all_mid_changes(self) -> list[float]:
        """
        Get all 1-second MID changes.

        Returns:
            List of all MID changes
        """
        return self.mid_changes_buffer.get_all_values()
