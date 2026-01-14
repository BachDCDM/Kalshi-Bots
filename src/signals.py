"""
Signal calculations for Kalshi trading bot.

Implements all trading signals: MID, BASELINE, VOL_60, RET_10, DELTA,
shock detection, and overreaction detection.
"""

from typing import Literal, Optional

import numpy as np

from src.config import Config
from src.logger import TradingLogger
from src.market_data import MarketDataManager


class SignalCalculator:
    """Calculates all trading signals from market data."""

    def __init__(self, config: Config, data_manager: MarketDataManager, logger: TradingLogger):
        """
        Initialize signal calculator.

        Args:
            config: Bot configuration
            data_manager: Market data manager
            logger: Trading logger
        """
        self.config = config
        self.data_manager = data_manager
        self.logger = logger

        # Pre-calculate EMA alpha
        self.ema_alpha = 2.0 / (config.BASELINE_WINDOW + 1)

    def calculate_baseline(self) -> Optional[float]:
        """
        Calculate 2-minute EMA of MID prices.

        Formula: EMA uses exponential weights
        alpha = 2 / (span + 1)

        Returns:
            Baseline (EMA) or None if insufficient data
        """
        mids = self.data_manager.get_all_mids()

        if len(mids) < 10:  # Need minimum data
            return None

        # Use exponentially decaying weights
        # More recent values get higher weight
        weights = np.exp(np.linspace(-1.0, 0.0, len(mids)))
        weights /= weights.sum()

        ema = np.average(mids, weights=weights)

        return float(ema)

    def calculate_vol_60(self) -> Optional[float]:
        """
        Calculate std dev of 1-second MID changes over last 60 seconds.

        Returns:
            Volatility (standard deviation) or None if insufficient data
        """
        changes = self.data_manager.get_all_mid_changes()

        if len(changes) < 10:  # Need minimum data
            return None

        # Standard deviation with Bessel's correction (ddof=1)
        vol = np.std(changes, ddof=1)

        return float(vol)

    def calculate_ret_10(self) -> Optional[float]:
        """
        Calculate 10-second return: MID(now) - MID(10 seconds ago).

        Returns:
            10-second return or None if insufficient data
        """
        current_mid = self.data_manager.last_mid
        past_mid = self.data_manager.get_mid_at(seconds_ago=10.0)

        if current_mid is None or past_mid is None:
            return None

        return current_mid - past_mid

    def calculate_delta(self, baseline: Optional[float] = None) -> Optional[float]:
        """
        Calculate DELTA = MID - BASELINE.

        Args:
            baseline: Pre-calculated baseline (optional, will calculate if None)

        Returns:
            Delta or None if insufficient data
        """
        current_mid = self.data_manager.last_mid

        if baseline is None:
            baseline = self.calculate_baseline()

        if current_mid is None or baseline is None:
            return None

        return current_mid - baseline

    def calculate_shock_threshold(self, vol_60: Optional[float] = None) -> float:
        """
        Calculate shock threshold: max(0.06, 3 × VOL_60).

        Args:
            vol_60: Pre-calculated volatility (optional)

        Returns:
            Shock threshold
        """
        if vol_60 is None:
            vol_60 = self.calculate_vol_60()

        if vol_60 is None:
            return self.config.MIN_SHOCK

        return max(self.config.MIN_SHOCK, self.config.SHOCK_MULTIPLIER * vol_60)

    def calculate_delta_threshold(self, vol_60: Optional[float] = None) -> float:
        """
        Calculate deviation threshold: max(0.04, 2 × VOL_60).

        Args:
            vol_60: Pre-calculated volatility (optional)

        Returns:
            Delta threshold
        """
        if vol_60 is None:
            vol_60 = self.calculate_vol_60()

        if vol_60 is None:
            return self.config.MIN_DEVIATION

        return max(self.config.MIN_DEVIATION, self.config.DEVIATION_MULTIPLIER * vol_60)

    def calculate_exit_band(self, vol_60: Optional[float] = None) -> float:
        """
        Calculate exit band: max(0.015, VOL_60).

        Args:
            vol_60: Pre-calculated volatility (optional)

        Returns:
            Exit band threshold
        """
        if vol_60 is None:
            vol_60 = self.calculate_vol_60()

        if vol_60 is None:
            return self.config.EXIT_BAND_FLOOR

        return max(self.config.EXIT_BAND_FLOOR, vol_60)

    def detect_shock(self) -> Optional[Literal["UP", "DOWN"]]:
        """
        Detect shock: |RET_10| >= SHOCK_TH.

        Returns "UP" for positive shock, "DOWN" for negative shock, None otherwise.
        Logs every shock detection.

        Returns:
            "UP", "DOWN", or None
        """
        ret_10 = self.calculate_ret_10()
        vol_60 = self.calculate_vol_60()
        shock_th = self.calculate_shock_threshold(vol_60)

        if ret_10 is None:
            return None

        if ret_10 >= shock_th:
            self.logger.log_shock(
                shock_direction="UP",
                shock_th=shock_th,
                ret_10=ret_10,
                vol_60=vol_60,
                entry_attempted=False
            )
            return "UP"

        if ret_10 <= -shock_th:
            self.logger.log_shock(
                shock_direction="DOWN",
                shock_th=shock_th,
                ret_10=ret_10,
                vol_60=vol_60,
                entry_attempted=False
            )
            return "DOWN"

        return None

    def detect_overreaction(self) -> Optional[Literal["UP", "DOWN"]]:
        """
        Detect overreaction: |DELTA| >= DELTA_TH.

        Returns "UP" for positive deviation, "DOWN" for negative, None otherwise.

        Returns:
            "UP", "DOWN", or None
        """
        baseline = self.calculate_baseline()
        delta = self.calculate_delta(baseline)
        vol_60 = self.calculate_vol_60()
        delta_th = self.calculate_delta_threshold(vol_60)

        if delta is None:
            return None

        if delta >= delta_th:
            return "UP"

        if delta <= -delta_th:
            return "DOWN"

        return None

    def get_all_signals(self) -> dict:
        """
        Calculate all signals and return as dict for logging.

        Returns:
            Dict with all signal values
        """
        vol_60 = self.calculate_vol_60()
        baseline = self.calculate_baseline()
        ret_10 = self.calculate_ret_10()
        delta = self.calculate_delta(baseline)
        shock_th = self.calculate_shock_threshold(vol_60)
        delta_th = self.calculate_delta_threshold(vol_60)
        exit_band = self.calculate_exit_band(vol_60)

        return {
            "mid": self.data_manager.last_mid,
            "baseline": baseline,
            "vol_60": vol_60,
            "ret_10": ret_10,
            "delta": delta,
            "shock_th": shock_th,
            "delta_th": delta_th,
            "exit_band": exit_band,
            "timestamp": self.data_manager.last_update_time,
        }
