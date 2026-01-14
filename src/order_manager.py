"""
Order management for Kalshi trading bot.

Handles order lifecycle: submission, monitoring, cancellation.
"""

import asyncio
import time
from typing import Literal, Optional

from src.config import Config
from src.kalshi_client import KalshiClient
from src.logger import TradingLogger
from src.market_data import OrderBook
from src.utils import denormalize_price


class OrderManager:
    """Handles order lifecycle: submit, monitor, cancel."""

    def __init__(self, kalshi_client: KalshiClient, config: Config, logger: TradingLogger):
        """
        Initialize order manager.

        Args:
            kalshi_client: Kalshi API client
            config: Bot configuration
            logger: Trading logger
        """
        self.client = kalshi_client.rest
        self.config = config
        self.logger = logger

    async def submit_limit_order(
        self,
        ticker: str,
        side: Literal["yes", "no"],
        price: float,
        size: int
    ) -> Optional[str]:
        """
        Submit limit order and return order_id.

        Args:
            ticker: Market ticker
            side: "yes" or "no"
            price: Price in decimal (0-1)
            size: Number of contracts

        Returns:
            order_id if successful, None on error
        """
        try:
            # Convert price to cents
            price_cents = denormalize_price(price)

            # Determine which price to set based on side
            yes_price = price_cents if side == "yes" else None
            no_price = price_cents if side == "no" else None

            response = await self.client.submit_order(
                ticker=ticker,
                side=side,
                action="buy",
                count=size,
                type="limit",
                yes_price=yes_price,
                no_price=no_price
            )

            order_id = response.get("order", {}).get("order_id")

            self.logger.log_info(
                "order_submitted",
                order_id=order_id,
                ticker=ticker,
                side=side,
                price=price,
                size=size
            )

            return order_id

        except Exception as e:
            self.logger.log_error(
                "order_submission_failed",
                str(e),
                context={"ticker": ticker, "side": side, "price": price},
                exc_info=True
            )
            return None

    async def cancel_order(self, order_id: str) -> bool:
        """
        Cancel order, return True if successful.

        Args:
            order_id: Order ID to cancel

        Returns:
            True if cancelled successfully
        """
        try:
            await self.client.cancel_order(order_id)
            self.logger.log_info("order_cancelled", order_id=order_id)
            return True
        except Exception as e:
            self.logger.log_error(
                "order_cancellation_failed",
                str(e),
                context={"order_id": order_id},
                exc_info=True
            )
            return False

    async def wait_for_fill(
        self,
        order_id: str,
        timeout: float
    ) -> tuple[bool, Optional[float]]:
        """
        Poll for order fill status.

        Args:
            order_id: Order ID to monitor
            timeout: Timeout in seconds

        Returns:
            (filled: bool, fill_price: Optional[float])
        """
        start_time = time.time()
        poll_interval = 0.5  # Poll every 500ms

        while time.time() - start_time < timeout:
            try:
                response = await self.client.get_order(order_id)
                order = response.get("order", {})
                status = order.get("status")

                if status == "filled":
                    # Extract fill price
                    fill_price_cents = order.get("yes_price") or order.get("no_price")
                    fill_price = fill_price_cents / 100.0 if fill_price_cents else None

                    self.logger.log_info(
                        "order_filled",
                        order_id=order_id,
                        fill_price=fill_price
                    )

                    return True, fill_price

                elif status in ["cancelled", "rejected"]:
                    self.logger.log_info(
                        "order_not_filled",
                        order_id=order_id,
                        status=status
                    )
                    return False, None

                # Still pending, continue polling
                await asyncio.sleep(poll_interval)

            except Exception as e:
                self.logger.log_error(
                    "order_status_check_failed",
                    str(e),
                    context={"order_id": order_id},
                    exc_info=True
                )
                await asyncio.sleep(poll_interval)

        # Timeout reached
        self.logger.log_info(
            "order_fill_timeout",
            order_id=order_id,
            timeout=timeout
        )
        return False, None

    async def market_exit(
        self,
        ticker: str,
        side: Literal["yes", "no"],
        size: int,
        orderbook: OrderBook
    ) -> Optional[str]:
        """
        Exit position using aggressive limit order.

        Place limit order at worst available price to ensure quick fill.

        Args:
            ticker: Market ticker
            side: "yes" or "no"
            size: Number of contracts to sell
            orderbook: Current order book

        Returns:
            order_id if successful, None on error
        """
        try:
            # For YES position, sell at best bid (or lower)
            # For NO position, sell at best NO bid (or lower)
            if side == "yes":
                price = orderbook.yes_bid - 0.01 if orderbook.yes_bid else 0.01
            else:
                price = orderbook.no_bid - 0.01 if orderbook.no_bid else 0.01

            # Clamp to valid range
            price = max(0.01, min(0.99, price))

            # Convert to cents
            price_cents = denormalize_price(price)

            response = await self.client.submit_order(
                ticker=ticker,
                side=side,
                action="sell",
                count=size,
                type="limit",
                yes_price=price_cents if side == "yes" else None,
                no_price=price_cents if side == "no" else None
            )

            order_id = response.get("order", {}).get("order_id")

            self.logger.log_info(
                "exit_order_submitted",
                order_id=order_id,
                ticker=ticker,
                side=side,
                price=price,
                size=size
            )

            return order_id

        except Exception as e:
            self.logger.log_error(
                "exit_order_failed",
                str(e),
                context={"ticker": ticker, "side": side, "size": size},
                exc_info=True
            )
            return None
