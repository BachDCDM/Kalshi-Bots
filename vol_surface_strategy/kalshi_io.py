"""Kalshi client + raw market fetch with 429 backoff."""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Optional

import certifi
from dotenv import load_dotenv
from kalshi_python_sync import Configuration, KalshiClient
from kalshi_python_sync.exceptions import ApiException

_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_ROOT / ".env", override=False)


def _pem() -> str:
    raw = (os.environ.get("KALSHI_PRIVATE_KEY") or "").strip()
    if raw:
        return raw.replace("\\n", "\n")
    for p in [Path(os.environ.get("KALSHI_PRIVATE_KEY_PATH") or ""), _ROOT / "kalshi.pem"]:
        if p and Path(p).expanduser().is_file():
            pem = Path(p).expanduser().read_text(encoding="utf-8")
            if "BEGIN" in pem:
                return pem
    print("No kalshi.pem / KALSHI_PRIVATE_KEY", file=sys.stderr)
    sys.exit(1)


def load_client() -> KalshiClient:
    key = (os.environ.get("KALSHI_API_KEY_ID") or "").strip()
    if not key:
        print("Set KALSHI_API_KEY_ID", file=sys.stderr)
        sys.exit(1)
    host = os.environ.get("KALSHI_HOST", "https://api.elections.kalshi.com/trade-api/v2")
    cfg = Configuration(host=host, ssl_ca_cert=certifi.where())
    cfg.api_key_id = key
    cfg.private_key_pem = _pem()
    return KalshiClient(cfg)


def _bag(d: dict) -> Any:
    b = SimpleNamespace()
    for k, v in d.items():
        setattr(b, k, v)
    return b


def _normalize_market_dict(d: dict) -> dict:
    """
    Kalshi ``GET /markets`` often omits ``series_ticker`` on each row even when ``event_ticker``
    is present. Series is almost always the leading token before the first ``-`` in the event
    ticker (e.g. ``KXMLBSPREAD-26APR...`` → ``KXMLBSPREAD``). Also accept camelCase keys if present.
    """
    out = dict(d)
    et = str(out.get("event_ticker") or out.get("eventTicker") or "").strip()
    st = str(out.get("series_ticker") or out.get("seriesTicker") or "").strip()
    if et:
        out["event_ticker"] = et
    if st:
        out["series_ticker"] = st
    elif et:
        out["series_ticker"] = et.split("-", 1)[0] if "-" in et else et
    return out


def get_markets_page_raw(
    client: KalshiClient,
    *,
    limit: int = 200,
    cursor: Optional[str] = None,
    status: str = "open",
    series_ticker: Optional[str] = None,
    event_ticker: Optional[str] = None,
    min_close_ts: Optional[int] = None,
    max_close_ts: Optional[int] = None,
    mve_filter: Optional[str] = "exclude",
) -> tuple[list[Any], str]:
    kwargs: dict[str, Any] = {"status": status, "limit": limit}
    if cursor:
        kwargs["cursor"] = cursor
    if series_ticker:
        kwargs["series_ticker"] = series_ticker
    if event_ticker:
        kwargs["event_ticker"] = event_ticker
    if min_close_ts is not None:
        kwargs["min_close_ts"] = min_close_ts
    if max_close_ts is not None:
        kwargs["max_close_ts"] = max_close_ts
    if mve_filter:
        kwargs["mve_filter"] = mve_filter

    delay = 2.0
    max_attempts = 10
    for attempt in range(max_attempts):
        try:
            raw_resp = client.get_markets_without_preload_content(**kwargs)
            status_code = int(getattr(raw_resp, "status", 200) or 200)
            # Raw urllib response uses status; 429 is common on long catalog scans — retry with backoff.
            if status_code == 429:
                if attempt < max_attempts - 1:
                    time.sleep(delay)
                    delay = min(delay * 2, 60.0)
                    continue
                raise RuntimeError("get_markets HTTP 429 (rate limited after retries)")
            if status_code != 200:
                raise RuntimeError(f"get_markets HTTP {status_code}")
            body = raw_resp.read()
            text = body if isinstance(body, str) else body.decode("utf-8")
            payload = json.loads(text)
            markets = [
                _bag(_normalize_market_dict(dict(m)))
                for m in (payload.get("markets") or [])
                if isinstance(m, dict)
            ]
            return markets, str(payload.get("cursor") or "")
        except ApiException as e:
            if getattr(e, "status", None) == 429 and attempt < max_attempts - 1:
                time.sleep(delay)
                delay = min(delay * 2, 60.0)
            else:
                raise
    return [], ""


def _bag_event_with_markets(d: dict) -> Any:
    """Event dict from /events; nested ``markets`` become SimpleNamespace rows like GET /markets."""
    mk_raw = d.get("markets")
    d2 = {k: v for k, v in d.items() if k != "markets"}
    ev = _bag(d2)
    if isinstance(mk_raw, list):
        setattr(
            ev,
            "markets",
            [_bag(_normalize_market_dict(dict(x))) if isinstance(x, dict) else x for x in mk_raw],
        )
    else:
        setattr(ev, "markets", [])
    return ev


def get_multivariate_events_page_raw(
    client: KalshiClient,
    *,
    limit: int = 200,
    cursor: Optional[str] = None,
    series_ticker: Optional[str] = None,
    collection_ticker: Optional[str] = None,
    with_nested_markets: bool = False,
) -> tuple[list[Any], str]:
    """
    Paginated ``GET /events/multivariate`` (raw JSON → SimpleNamespace).

    Sports game ladders are usually **multivariate**; plain ``get_events`` omits them.
    """
    kwargs: dict[str, Any] = {"limit": min(200, max(1, limit))}
    if cursor:
        kwargs["cursor"] = cursor
    if series_ticker:
        kwargs["series_ticker"] = series_ticker
    if collection_ticker:
        kwargs["collection_ticker"] = collection_ticker
    kwargs["with_nested_markets"] = with_nested_markets

    delay = 2.0
    max_attempts = 10
    for attempt in range(max_attempts):
        try:
            raw_resp = client._events_api.get_multivariate_events_without_preload_content(**kwargs)
            status_code = int(getattr(raw_resp, "status", 200) or 200)
            if status_code == 429:
                if attempt < max_attempts - 1:
                    time.sleep(delay)
                    delay = min(delay * 2, 60.0)
                    continue
                raise RuntimeError("get_multivariate_events HTTP 429 (rate limited after retries)")
            if status_code != 200:
                raise RuntimeError(f"get_multivariate_events HTTP {status_code}")
            body = raw_resp.read()
            text = body if isinstance(body, str) else body.decode("utf-8")
            payload = json.loads(text)
            events = [
                _bag_event_with_markets(dict(e))
                for e in (payload.get("events") or [])
                if isinstance(e, dict)
            ]
            return events, str(payload.get("cursor") or "")
        except ApiException as e:
            if getattr(e, "status", None) == 429 and attempt < max_attempts - 1:
                time.sleep(delay)
                delay = min(delay * 2, 60.0)
            else:
                raise
    return [], ""


def get_market(client: KalshiClient, ticker: str) -> Any:
    return client.get_market(ticker=ticker).market


def portfolio_total_cents(client: KalshiClient) -> int:
    from kalshi_python_sync.api import PortfolioApi

    r = PortfolioApi(client).get_balance()
    return int(getattr(r, "balance", 0) or 0) + int(getattr(r, "portfolio_value", 0) or 0)


def _fp(x: Any) -> float:
    try:
        return float(x or 0)
    except (TypeError, ValueError):
        return 0.0


def net_contracts_for_ticker(client: KalshiClient, ticker: str) -> float:
    """Absolute net position size on a market ticker."""
    r = client.get_positions(ticker=ticker, count_filter="position")
    positions = getattr(r, "market_positions", None) or []
    for p in positions:
        if str(getattr(p, "ticker", "") or "") == ticker:
            return abs(_fp(getattr(p, "position_fp", "0")))
    return 0.0


def fetch_order(client: KalshiClient, order_id: str) -> Any:
    return client.get_order(order_id=order_id).order
