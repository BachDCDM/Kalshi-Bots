"""Kalshi trading via official SDK (RSA auth)."""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Optional

import certifi
from dotenv import load_dotenv
from kalshi_python_sync import Configuration, KalshiClient

_ROOT = Path(__file__).resolve().parent
load_dotenv(_ROOT / ".env")
load_dotenv(_ROOT.parent / ".env")


def _resolve_pem_path(path_str: str) -> Path:
    p = Path(path_str).expanduser()
    if not p.is_absolute():
        p = _ROOT / p
        if not p.is_file():
            p = _ROOT.parent / Path(path_str).name
    return p


def _load_private_key_pem() -> str:
    raw = (os.environ.get("KALSHI_PRIVATE_KEY") or "").strip()
    if raw:
        pem = raw.replace("\\n", "\n")
        if "BEGIN" not in pem:
            raise RuntimeError("KALSHI_PRIVATE_KEY must be a PEM block")
        return pem
    path_str = (os.environ.get("KALSHI_PRIVATE_KEY_PATH") or "").strip()
    candidates: list[Path] = []
    if path_str:
        candidates.append(_resolve_pem_path(path_str))
    candidates.extend([_ROOT / "kalshi.pem", _ROOT.parent / "kalshi.pem"])
    for path in candidates:
        if path.is_file():
            pem = path.read_text(encoding="utf-8")
            if "BEGIN" in pem and "PRIVATE KEY" in pem:
                return pem
    raise RuntimeError(
        "Set KALSHI_PRIVATE_KEY or KALSHI_PRIVATE_KEY_PATH, or place kalshi.pem in "
        f"{_ROOT} or {_ROOT.parent}"
    )


def load_client() -> KalshiClient:
    key_id = (os.environ.get("KALSHI_API_KEY_ID") or "").strip()
    if not key_id:
        print("Set KALSHI_API_KEY_ID", file=sys.stderr)
        sys.exit(1)
    pem = _load_private_key_pem()
    host = os.environ.get(
        "KALSHI_HOST",
        "https://api.elections.kalshi.com/trade-api/v2",
    )
    cfg = Configuration(host=host, ssl_ca_cert=certifi.where())
    cfg.api_key_id = key_id
    cfg.private_key_pem = pem
    return KalshiClient(cfg)


def _parse_close_local(m: Any, tz) -> datetime:
    ct = m.close_time
    if isinstance(ct, datetime):
        dt = ct
    else:
        s = str(ct)
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(tz)


def _market_from_api_dict(m: dict[str, Any]) -> Any:
    """Lightweight object with attribute access; avoids strict SDK Market parsing."""
    bag = SimpleNamespace()
    for k, v in m.items():
        setattr(bag, k, v)
    return bag


def _get_markets_open_page_raw(
    client: KalshiClient,
    *,
    limit: int = 200,
    cursor: Optional[str],
) -> tuple[list[Any], str]:
    """GET /markets as JSON; Kalshi sometimes returns nulls the SDK Market model rejects."""
    raw_resp = client.get_markets_without_preload_content(
        status="open",
        limit=limit,
        cursor=cursor or None,
        mve_filter="exclude",
    )
    if getattr(raw_resp, "status", 200) != 200:
        raise RuntimeError(f"get_markets HTTP {getattr(raw_resp, 'status', '?')}")
    body = raw_resp.read()
    if isinstance(body, str):
        text = body
    else:
        text = body.decode("utf-8")
    payload = json.loads(text)
    markets_raw = payload.get("markets") or []
    next_cursor = str(payload.get("cursor") or "").strip()
    markets = [_market_from_api_dict(dict(m)) for m in markets_raw if isinstance(m, dict)]
    return markets, next_cursor


def fetch_open_temp_markets_for_city(
    client: KalshiClient,
    kalshi_name_variants: list[str],
    local_tz,
) -> list[Any]:
    """Paginate open markets and filter to same-day high-temperature style buckets for a city."""
    today_local = datetime.now(local_tz).date()
    out: list[Any] = []
    cursor: Optional[str] = None
    variants_l = [v.lower() for v in kalshi_name_variants]

    while True:
        markets, next_c = _get_markets_open_page_raw(client, limit=200, cursor=cursor)
        for m in markets:
            title = (getattr(m, "title", None) or "").lower()
            if not any(v in title for v in variants_l):
                continue
            if "temperature" not in title and "temp" not in title:
                continue
            if "high" not in title and "highest" not in title:
                continue
            try:
                cl = _parse_close_local(m, local_tz).date()
            except Exception:
                continue
            if cl != today_local:
                continue
            out.append(m)
        if not next_c:
            break
        cursor = next_c
    return out


def market_range_label(m: Any) -> str:
    """Bucket label for parsing (subtitle or YES leg title)."""
    for attr in ("subtitle", "yes_sub_title", "title"):
        v = getattr(m, attr, None)
        if v:
            return str(v).strip()
    return ""


def _dollars_to_cents(s: str) -> int:
    return int(round(float(s) * 100))


def get_orderbook(client: KalshiClient, ticker: str) -> dict[str, Any]:
    r = client.get_market_orderbook(ticker=ticker)
    ob = r.orderbook_fp
    yes_rows = ob.yes_dollars or []
    no_rows = ob.no_dollars or []

    yes_bid = None
    for row in yes_rows:
        if len(row) >= 2:
            yes_bid = _dollars_to_cents(str(row[0]))
            break

    best_no_bid = None
    for row in no_rows:
        if len(row) >= 2:
            best_no_bid = _dollars_to_cents(str(row[0]))
            break

    yes_ask = (100 - best_no_bid) if best_no_bid is not None else None
    spread = (yes_ask - yes_bid) if (yes_bid is not None and yes_ask is not None) else None
    midpoint = ((yes_bid + yes_ask) / 2) if (yes_bid is not None and yes_ask is not None) else None

    return {
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "spread": spread,
        "midpoint": midpoint,
    }


def parse_bucket_range(text: str) -> tuple[Optional[int], Optional[int]]:
    """Parse range label into (low, high); None = open-ended."""
    if not text:
        return (None, None)
    t = text.lower().replace("℉", "").replace("f", "")
    t = t.replace("–", "-").strip()

    if "or below" in t:
        digits = "".join(c for c in t.split("or below")[0] if c.isdigit())
        if not digits:
            return (None, None)
        return (None, int(digits))

    if "or above" in t:
        digits = "".join(c for c in t.split("or above")[0] if c.isdigit())
        if not digits:
            return (None, None)
        return (int(digits), None)

    if " to " in t:
        parts = t.split(" to ")
        if len(parts) == 2:
            low = "".join(c for c in parts[0] if c.isdigit())
            high = "".join(c for c in parts[1] if c.isdigit())
            if low and high:
                return (int(low), int(high))

    m = re.search(r"(\d+)\s*[-–]\s*(\d+)", t)
    if m:
        return (int(m.group(1)), int(m.group(2)))

    return (None, None)


def place_order(
    client: KalshiClient,
    ticker: str,
    side: str,
    price_cents: int,
    count: int,
) -> Any:
    """Buy YES at yes_price, or buy NO at no_price (cents)."""
    kwargs: dict[str, Any] = {
        "ticker": ticker,
        "action": "buy",
        "side": side,
        "count": count,
        "time_in_force": "good_till_canceled",
    }
    if side == "yes":
        kwargs["yes_price"] = price_cents
    else:
        kwargs["no_price"] = price_cents
    return client.create_order(**kwargs).order


def close_position(
    client: KalshiClient,
    ticker: str,
    side: str,
    price_cents: int,
    count: int,
) -> Any:
    """Sell YES at yes_price, or sell NO at no_price."""
    kwargs: dict[str, Any] = {
        "ticker": ticker,
        "action": "sell",
        "side": side,
        "count": int(count),
        "time_in_force": "good_till_canceled",
    }
    if side == "yes":
        kwargs["yes_price"] = price_cents
    else:
        kwargs["no_price"] = price_cents
    return client.create_order(**kwargs).order


def get_positions(client: KalshiClient) -> list[Any]:
    out: list[Any] = []
    cursor: Optional[str] = None
    while True:
        r = client.get_positions(
            count_filter="position", limit=200, cursor=cursor
        )
        out.extend(list(getattr(r, "market_positions", None) or []))
        cursor = getattr(r, "cursor", None) or ""
        if not cursor:
            break
    return out


def position_side_and_size(pos: Any) -> tuple[Optional[str], float]:
    """Infer side from signed position_fp (positive = YES, negative = NO)."""
    fp = float(pos.position_fp or "0")
    if abs(fp) < 1e-6:
        return None, 0.0
    if fp > 0:
        return "yes", abs(fp)
    return "no", abs(fp)
