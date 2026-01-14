"""
Main entry point for Kalshi trading bot.

Orchestrates all components and handles the main event loop.
"""

import asyncio
import signal
from datetime import datetime

from src.config import Config
from src.kalshi_client import KalshiClient
from src.logger import TradingLogger
from src.market_data import MarketDataManager, OrderBook
from src.order_manager import OrderManager
from src.position_manager import PositionManager
from src.signals import SignalCalculator
from src.strategy import Strategy


class TradingBot:
    """Main bot orchestrator."""

    def __init__(self):
        """Initialize trading bot and all components."""
        # Load configuration
        self.config = Config()

        # Initialize logging
        self.logger = TradingLogger(log_dir=self.config.LOG_DIR)

        # Initialize components
        self.kalshi_client = KalshiClient(self.config, self.logger)
        self.data_manager = MarketDataManager(self.config, self.logger)
        self.signal_calculator = SignalCalculator(
            self.config,
            self.data_manager,
            self.logger
        )
        self.position_manager = PositionManager(self.kalshi_client, self.logger)
        self.order_manager = OrderManager(
            self.kalshi_client,
            self.config,
            self.logger
        )
        self.strategy = Strategy(
            self.config,
            self.signal_calculator,
            self.order_manager,
            self.position_manager,
            self.data_manager,
            self.logger
        )

        # Market metadata
        self.market_close_time: datetime = None

        # Shutdown flag
        self.shutdown_requested = False

    async def initialize(self):
        """Initialize bot - fetch market metadata, sync positions."""
        self.logger.log_info("bot_initializing")

        # Fetch market metadata
        market_info = await self.kalshi_client.rest.get_market(
            self.config.TARGET_MARKET_TICKER
        )

        # Extract close time
        close_time_str = market_info.get("market", {}).get("close_time")
        self.market_close_time = datetime.fromisoformat(close_time_str.replace('Z', '+00:00'))

        self.logger.log_info(
            "market_loaded",
            ticker=self.config.TARGET_MARKET_TICKER,
            close_time=self.market_close_time.isoformat()
        )

        # Sync positions
        await self.position_manager.sync_positions()

        # Check if we have existing positions (safety check)
        if self.position_manager.has_position(self.config.TARGET_MARKET_TICKER):
            self.logger.log_error(
                "existing_position_detected",
                "Bot starting with existing position - manual intervention needed"
            )
            raise RuntimeError("Existing position detected at startup")

        self.logger.log_info("bot_initialized")

    async def run(self):
        """Main bot loop."""
        await self.initialize()

        self.logger.log_info("bot_running")

        try:
            async for ws_data in self.kalshi_client.ws.orderbook_stream(
                self.config.TARGET_MARKET_TICKER
            ):
                if self.shutdown_requested:
                    break

                # Parse orderbook from WebSocket data
                orderbook = OrderBook.from_kalshi_data(
                    ws_data,
                    self.config.TARGET_MARKET_TICKER,
                    self.data_manager.last_update_time or 0.0
                )

                # Process orderbook update
                await self.strategy.on_orderbook_update(
                    orderbook,
                    self.market_close_time
                )

        except Exception as e:
            self.logger.log_error(
                "main_loop_error",
                str(e),
                exc_info=True
            )
            raise

        finally:
            await self.shutdown()

    async def shutdown(self):
        """Graceful shutdown."""
        self.logger.log_info("bot_shutting_down")

        # Flatten positions
        await self.strategy.shutdown()

        # Close connections
        await self.kalshi_client.close()

        # Close logger
        self.logger.close()

        self.logger.log_info("bot_shutdown_complete")

    def signal_handler(self, sig, frame):
        """
        Handle SIGINT/SIGTERM.

        Args:
            sig: Signal number
            frame: Current stack frame
        """
        self.logger.log_info("shutdown_signal_received", signal=sig)
        self.shutdown_requested = True


async def main():
    """Entry point."""
    bot = TradingBot()

    # Setup signal handlers
    signal.signal(signal.SIGINT, bot.signal_handler)
    signal.signal(signal.SIGTERM, bot.signal_handler)

    try:
        await bot.run()
    except KeyboardInterrupt:
        print("\nShutdown requested by user")
    except Exception as e:
        print(f"Fatal error: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
