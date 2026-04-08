"""Place maker orders with expiration_ts."""

from __future__ import annotations

import uuid
from typing import Any

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
