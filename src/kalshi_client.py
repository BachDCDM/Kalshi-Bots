"""
Kalshi API client with RSA-PSS authentication.

Provides REST and WebSocket interfaces to Kalshi prediction markets.
"""

import asyncio
import base64
import json
import time
from typing import Any, AsyncGenerator, Optional

import httpx
import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from src.config import Config
from src.logger import TradingLogger
from src.utils import milliseconds_to_timestamp


class KalshiAuth:
    """RSA-PSS SHA256 authentication for Kalshi API."""

    def __init__(self, api_key: str, private_key_pem: str):
        """
        Initialize authentication.

        Args:
            api_key: Kalshi API key
            private_key_pem: RSA private key in PEM format
        """
        self.api_key = api_key

        # Load RSA private key
        self.private_key = serialization.load_pem_private_key(
            private_key_pem.encode(),
            password=None
        )

    def sign_request(self, method: str, path: str, timestamp: str) -> str:
        """
        Sign request using RSA-PSS.

        Args:
            method: HTTP method (GET, POST, etc.)
            path: Request path (without query parameters)
            timestamp: Timestamp in milliseconds as string

        Returns:
            Base64-encoded signature
        """
        # Message format: {timestamp}{method}{path}
        message = f"{timestamp}{method}{path}"

        # Sign with RSA-PSS
        signature = self.private_key.sign(
            message.encode(),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )

        # Base64 encode
        return base64.b64encode(signature).decode()

    def get_headers(self, method: str, path: str) -> dict[str, str]:
        """
        Get authentication headers for request.

        Args:
            method: HTTP method
            path: Request path

        Returns:
            Headers dict with authentication
        """
        timestamp = milliseconds_to_timestamp()
        signature = self.sign_request(method, path, timestamp)

        return {
            "KALSHI-ACCESS-KEY": self.api_key,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
            "Content-Type": "application/json"
        }


class KalshiRestClient:
    """REST API client for Kalshi."""

    def __init__(self, config: Config, logger: TradingLogger):
        """
        Initialize REST client.

        Args:
            config: Bot configuration
            logger: Trading logger
        """
        self.base_url = config.KALSHI_API_BASE_URL
        self.auth = KalshiAuth(config.KALSHI_API_KEY, config.KALSHI_API_SECRET)
        self.client = httpx.AsyncClient(timeout=10.0)
        self.logger = logger

    async def get_market(self, ticker: str) -> dict[str, Any]:
        """
        Get market metadata.

        Args:
            ticker: Market ticker

        Returns:
            Market data dict

        Raises:
            httpx.HTTPStatusError: On API error
        """
        path = f"/trade-api/v2/markets/{ticker}"
        headers = self.auth.get_headers("GET", path)

        try:
            response = await self.client.get(f"{self.base_url}{path}", headers=headers)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            self.logger.log_error(
                "api_error",
                f"GET {path} failed: {e}",
                context={"status_code": e.response.status_code}
            )
            raise

    async def get_orderbook(self, ticker: str) -> dict[str, Any]:
        """
        Get current order book.

        Args:
            ticker: Market ticker

        Returns:
            Order book data dict
        """
        path = f"/trade-api/v2/markets/{ticker}/orderbook"
        headers = self.auth.get_headers("GET", path)

        try:
            response = await self.client.get(f"{self.base_url}{path}", headers=headers)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            self.logger.log_error(
                "api_error",
                f"GET {path} failed: {e}",
                context={"status_code": e.response.status_code}
            )
            raise

    async def get_positions(self) -> dict[str, Any]:
        """
        Get account positions.

        Returns:
            Positions data dict
        """
        path = "/trade-api/v2/portfolio/positions"
        headers = self.auth.get_headers("GET", path)

        try:
            response = await self.client.get(f"{self.base_url}{path}", headers=headers)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            self.logger.log_error(
                "api_error",
                f"GET {path} failed: {e}",
                context={"status_code": e.response.status_code}
            )
            raise

    async def submit_order(
        self,
        ticker: str,
        side: str,
        action: str,
        count: int,
        type: str,
        yes_price: Optional[int] = None,
        no_price: Optional[int] = None
    ) -> dict[str, Any]:
        """
        Submit order.

        Args:
            ticker: Market ticker
            side: "yes" or "no"
            action: "buy" or "sell"
            count: Number of contracts
            type: "limit" or "market"
            yes_price: YES price in cents (for limit orders)
            no_price: NO price in cents (for limit orders)

        Returns:
            Order response dict
        """
        path = "/trade-api/v2/portfolio/orders"
        headers = self.auth.get_headers("POST", path)

        payload = {
            "ticker": ticker,
            "side": side,
            "action": action,
            "count": count,
            "type": type,
        }

        if yes_price is not None:
            payload["yes_price"] = yes_price
        if no_price is not None:
            payload["no_price"] = no_price

        try:
            response = await self.client.post(
                f"{self.base_url}{path}",
                json=payload,
                headers=headers
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            self.logger.log_error(
                "api_error",
                f"POST {path} failed: {e}",
                context={"status_code": e.response.status_code, "payload": payload}
            )
            raise

    async def cancel_order(self, order_id: str) -> dict[str, Any]:
        """
        Cancel order.

        Args:
            order_id: Order ID to cancel

        Returns:
            Cancellation response dict
        """
        path = f"/trade-api/v2/portfolio/orders/{order_id}"
        headers = self.auth.get_headers("DELETE", path)

        try:
            response = await self.client.delete(f"{self.base_url}{path}", headers=headers)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            self.logger.log_error(
                "api_error",
                f"DELETE {path} failed: {e}",
                context={"status_code": e.response.status_code, "order_id": order_id}
            )
            raise

    async def get_order(self, order_id: str) -> dict[str, Any]:
        """
        Get order status.

        Args:
            order_id: Order ID

        Returns:
            Order data dict
        """
        path = f"/trade-api/v2/portfolio/orders/{order_id}"
        headers = self.auth.get_headers("GET", path)

        try:
            response = await self.client.get(f"{self.base_url}{path}", headers=headers)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            self.logger.log_error(
                "api_error",
                f"GET {path} failed: {e}",
                context={"status_code": e.response.status_code, "order_id": order_id}
            )
            raise

    async def close(self):
        """Close HTTP client."""
        await self.client.aclose()


class KalshiWebSocketClient:
    """WebSocket client for Kalshi order book streaming."""

    def __init__(self, config: Config, logger: TradingLogger):
        """
        Initialize WebSocket client.

        Args:
            config: Bot configuration
            logger: Trading logger
        """
        self.ws_url = config.KALSHI_WS_URL
        self.auth = KalshiAuth(config.KALSHI_API_KEY, config.KALSHI_API_SECRET)
        self.logger = logger
        self.ws = None
        self.reconnect_backoff = 1.0
        self.max_backoff = 60.0

        # Current orderbook state (for delta updates)
        self.current_orderbook: Optional[dict[str, Any]] = None

    async def connect(self):
        """Connect to WebSocket with authentication."""
        path = "/trade-api/ws/v2"
        headers = self.auth.get_headers("GET", path)

        # Convert headers to list of tuples for websockets library
        header_list = [(k, v) for k, v in headers.items()]

        self.ws = await websockets.connect(
            self.ws_url,
            extra_headers=header_list
        )
        self.logger.log_info("websocket_connected", url=self.ws_url)

    async def subscribe_orderbook(self, ticker: str):
        """
        Subscribe to order book updates.

        Args:
            ticker: Market ticker to subscribe to
        """
        subscribe_msg = {
            "id": 1,
            "cmd": "subscribe",
            "params": {
                "channels": [f"orderbook_delta:{ticker}"]
            }
        }
        await self.ws.send(json.dumps(subscribe_msg))
        self.logger.log_info("subscribed_to_orderbook", ticker=ticker)

    async def orderbook_stream(
        self,
        ticker: str
    ) -> AsyncGenerator[Any, None]:
        """
        Async generator yielding OrderBook objects from WebSocket stream.

        Handles reconnection with exponential backoff.

        Args:
            ticker: Market ticker to stream

        Yields:
            OrderBook objects (will be parsed in market_data.py)
        """
        while True:
            try:
                if not self.ws:
                    await self.connect()
                    await self.subscribe_orderbook(ticker)
                    self.reconnect_backoff = 1.0

                async for message in self.ws:
                    try:
                        data = json.loads(message)

                        if data.get("type") == "orderbook_snapshot":
                            self.current_orderbook = data
                            yield data

                        elif data.get("type") == "orderbook_delta":
                            # Apply delta to current state
                            if self.current_orderbook:
                                self._apply_delta(data)
                                yield self.current_orderbook
                            else:
                                # No snapshot yet, skip delta
                                continue

                        elif data.get("type") == "error":
                            self.logger.log_error(
                                "websocket_error",
                                data.get("msg", "Unknown error"),
                                context=data
                            )

                    except json.JSONDecodeError as e:
                        self.logger.log_error(
                            "websocket_parse_error",
                            str(e),
                            context={"message": message}
                        )

            except websockets.ConnectionClosed as e:
                self.logger.log_error(
                    "websocket_disconnected",
                    f"Connection closed: {e}",
                    context={"backoff": self.reconnect_backoff}
                )
                await asyncio.sleep(self.reconnect_backoff)
                self.reconnect_backoff = min(
                    self.reconnect_backoff * 2,
                    self.max_backoff
                )
                self.ws = None
                self.current_orderbook = None

            except Exception as e:
                self.logger.log_error(
                    "websocket_error",
                    str(e),
                    exc_info=True
                )
                await asyncio.sleep(self.reconnect_backoff)

    def _apply_delta(self, delta: dict[str, Any]):
        """
        Apply orderbook delta to current state.

        Args:
            delta: Delta message from WebSocket
        """
        # This is a simplified implementation
        # Full implementation would merge the delta into current_orderbook
        # For now, we'll just log that we received a delta
        pass

    async def close(self):
        """Close WebSocket connection."""
        if self.ws:
            await self.ws.close()
            self.logger.log_info("websocket_closed")


class KalshiClient:
    """Unified Kalshi client combining REST and WebSocket."""

    def __init__(self, config: Config, logger: TradingLogger):
        """
        Initialize Kalshi client.

        Args:
            config: Bot configuration
            logger: Trading logger
        """
        self.rest = KalshiRestClient(config, logger)
        self.ws = KalshiWebSocketClient(config, logger)
        self.logger = logger

    async def close(self):
        """Close all connections."""
        await self.rest.close()
        await self.ws.close()
        self.logger.log_info("kalshi_client_closed")
