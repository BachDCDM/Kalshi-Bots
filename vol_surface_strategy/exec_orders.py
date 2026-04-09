"""Place maker orders with expiration_ts."""

from __future__ import annotations

import uuid
from typing import Any, Optional

from kalshi_python_sync import KalshiClient


def place_buy(
    client: KalshiClient,
    *,
    ticker: str,
    side: str,
    count: int,
    price_cents: int,
    expiration_ts: int,
    post_only: bool = True,
) -> Any:
    cid = str(uuid.uuid4())[:32]
    kw: dict[str, Any] = {
        "ticker": ticker,
        "client_order_id": cid,
        "side": side,
        "action": "buy",
        "count": count,
        "time_in_force": "good_till_canceled",
        "expiration_ts": expiration_ts,
        "post_only": post_only,
    }
    if side == "yes":
        kw["yes_price"] = price_cents
    else:
        kw["no_price"] = price_cents
    return client.create_order(**kw)


def cancel_order(client: KalshiClient, order_id: str) -> Any:
    return client.cancel_order(order_id=order_id)


def resting_order_limit_cents(order: Any, side: str) -> Optional[int]:
    """Limit price we posted for a buy on `side` (yes/no)."""
    if side == "yes":
        v = getattr(order, "yes_price", None)
    else:
        v = getattr(order, "no_price", None)
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None
