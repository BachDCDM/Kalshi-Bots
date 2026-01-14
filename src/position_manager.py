"""
Position management for Kalshi trading bot.

Tracks positions and P&L.
"""

import time
from dataclasses import dataclass
from typing import Literal, Optional

from src.kalshi_client import KalshiClient
from src.logger import TradingLogger


@dataclass
class Position:
    """Represents an open position."""

    ticker: str
    side: Literal["yes", "no"]
    size: int
    entry_price: float
    entry_time: float


class PositionManager:
    """Tracks positions and P&L."""

    def __init__(self, kalshi_client: KalshiClient, logger: TradingLogger):
        """
        Initialize position manager.

        Args:
            kalshi_client: Kalshi API client
            logger: Trading logger
        """
        self.client = kalshi_client.rest
        self.logger = logger
        self.positions: dict[str, Position] = {}

    async def sync_positions(self):
        """
        Sync positions with Kalshi account.

        Critical for ensuring local state matches reality after errors.
        """
        try:
            response = await self.client.get_positions()
            positions = response.get("positions", [])

            self.positions.clear()

            for pos in positions:
                ticker = pos.get("ticker")
                # Kalshi returns net position - could be positive or negative
                position_count = pos.get("position", 0)

                if position_count == 0:
                    continue

                # Determine side based on position count
                side = "yes" if position_count > 0 else "no"
                size = abs(position_count)

                # We don't have entry price/time from API, use placeholder
                self.positions[ticker] = Position(
                    ticker=ticker,
                    side=side,
                    size=size,
                    entry_price=0.0,  # Unknown from sync
                    entry_time=time.time()
                )

            self.logger.log_info(
                "positions_synced",
                position_count=len(self.positions)
            )

        except Exception as e:
            self.logger.log_error(
                "position_sync_failed",
                str(e),
                exc_info=True
            )

    def record_entry(
        self,
        ticker: str,
        side: Literal["yes", "no"],
        size: int,
        price: float
    ):
        """
        Record new position entry.

        Args:
            ticker: Market ticker
            side: "yes" or "no"
            size: Position size
            price: Entry price
        """
        self.positions[ticker] = Position(
            ticker=ticker,
            side=side,
            size=size,
            entry_price=price,
            entry_time=time.time()
        )

        self.logger.log_info(
            "position_opened",
            ticker=ticker,
            side=side,
            size=size,
            entry_price=price
        )

    def record_exit(
        self,
        ticker: str,
        exit_price: float
    ) -> tuple[Optional[float], Optional[float]]:
        """
        Record position exit.

        Args:
            ticker: Market ticker
            exit_price: Exit price

        Returns:
            (realized_pnl, hold_time_seconds)
        """
        position = self.positions.get(ticker)

        if position is None:
            self.logger.log_error(
                "exit_without_position",
                "Attempted to exit non-existent position",
                context={"ticker": ticker}
            )
            return None, None

        # Calculate P&L
        # For YES: profit = (exit - entry) × size
        # For NO: profit = (entry - exit) × size
        if position.side == "yes":
            pnl_per_contract = exit_price - position.entry_price
        else:
            pnl_per_contract = position.entry_price - exit_price

        realized_pnl = pnl_per_contract * position.size
        hold_time = time.time() - position.entry_time

        self.logger.log_info(
            "position_closed",
            ticker=ticker,
            side=position.side,
            entry_price=position.entry_price,
            exit_price=exit_price,
            pnl=realized_pnl,
            hold_time=hold_time
        )

        # Remove position
        del self.positions[ticker]

        return realized_pnl, hold_time

    def get_position(self, ticker: str) -> Optional[Position]:
        """
        Get current position for ticker.

        Args:
            ticker: Market ticker

        Returns:
            Position or None if no position
        """
        return self.positions.get(ticker)

    def has_position(self, ticker: str) -> bool:
        """
        Check if we have a position.

        Args:
            ticker: Market ticker

        Returns:
            True if position exists
        """
        return ticker in self.positions
