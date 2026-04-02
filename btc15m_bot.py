#!/usr/bin/env python3
"""
Kalshi BTC 15m market bot: dual-sided entry bids in the first N minutes, then exit limit.

Each completed market session appends a row to ``btc15m_data/trades.db`` (``btc_sessions``).
The same database has ``btc_order_events``: one row when entry YES/NO orders are placed and
one per exit order, so you can confirm API placement before the session closes.

``success`` is 1 only if an exit was placed (``exit_handled``), exactly one entry leg filled,
and ``exit_cents > entry_cents`` (intended take-profit threshold). It does not wait for exit
fills or settlement. ``lowest_yes_mid_cents_first5`` is the minimum YES midpoint (¢) sampled
during the entry window after open.

Environment (or use a ``.env`` file next to this script — see ``.env.example``):
  KALSHI_API_KEY_ID — API key ID from Kalshi
  Private key (use one):
    KALSHI_PRIVATE_KEY_PATH — path to a ``.pem`` file (recommended; relative paths are
      resolved from this script's folder), or
    KALSHI_PRIVATE_KEY — single-line PEM with ``\\n`` between lines, or paste in ``kalshi.pem``
      in the project folder (see below).
  If unset, looks for ``kalshi.pem`` next to this script.

Optional:
  KALSHI_HOST — default https://api.elections.kalshi.com/trade-api/v2
  (use demo: https://demo-api.kalshi.co/trade-api/v2)
"""

from __future__ import annotations

import logging
import os
import sqlite3
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import certifi
from dotenv import load_dotenv
from kalshi_python_sync import Configuration, KalshiClient
from kalshi_python_sync.exceptions import NotFoundException

LOG = logging.getLogger("btc15m_bot")

_ROOT = Path(__file__).resolve().parent
load_dotenv(_ROOT / ".env")


def _fp(s: str) -> float:
    return float(s or "0")


def _price_dollars_to_cents_str(d: Optional[str]) -> str:
    if d is None or d == "":
        return "—"
    try:
        return f"{int(round(float(d) * 100))}¢"
    except (TypeError, ValueError):
        return str(d)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ts(ts: Any) -> datetime:
    if isinstance(ts, datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    if isinstance(ts, str):
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        return datetime.fromisoformat(ts)
    raise TypeError(f"Unexpected time type: {type(ts)}")


def _resolve_pem_path(path_str: str) -> Path:
    p = Path(path_str).expanduser()
    if not p.is_absolute():
        p = _ROOT / p
    return p


def _load_private_key_pem() -> str:
    """PEM from KALSHI_PRIVATE_KEY, KALSHI_PRIVATE_KEY_PATH, or ./kalshi.pem."""
    raw = (os.environ.get("KALSHI_PRIVATE_KEY") or "").strip()
    if raw:
        pem = raw.replace("\\n", "\n")
        if "BEGIN" not in pem:
            LOG.error("KALSHI_PRIVATE_KEY should include -----BEGIN ... PRIVATE KEY-----")
            sys.exit(1)
        return pem

    path_str = (os.environ.get("KALSHI_PRIVATE_KEY_PATH") or "").strip()
    candidates: list[Path] = []
    if path_str:
        candidates.append(_resolve_pem_path(path_str))
    candidates.append(_ROOT / "kalshi.pem")

    for path in candidates:
        if not path.is_file():
            continue
        pem = path.read_text(encoding="utf-8")
        if "BEGIN" not in pem or "PRIVATE KEY" not in pem:
            LOG.error("File does not look like a PEM private key: %s", path)
            sys.exit(1)
        return pem

    LOG.error(
        "No private key found. Save your Kalshi PEM to kalshi.pem in %s "
        "or set KALSHI_PRIVATE_KEY_PATH (see .env.example). "
        "If you edited .env in the editor, save the file (⌘S) so the key is on disk.",
        _ROOT,
    )
    sys.exit(1)


def _load_client() -> KalshiClient:
    key_id = (os.environ.get("KALSHI_API_KEY_ID") or "").strip()
    if not key_id:
        LOG.error("Set KALSHI_API_KEY_ID")
        sys.exit(1)
    pem = _load_private_key_pem()
    host = os.environ.get(
        "KALSHI_HOST", "https://api.elections.kalshi.com/trade-api/v2"
    )
    cfg = Configuration(host=host, ssl_ca_cert=certifi.where())
    # This SDK build expects auth values attached as attributes.
    cfg.api_key_id = key_id
    cfg.private_key_pem = pem
    return KalshiClient(cfg)


@dataclass
class Session:
    market_ticker: str
    session_id: str = field(default_factory=lambda: uuid.uuid4().hex[:10])
    yes_order_id: Optional[str] = None
    no_order_id: Optional[str] = None
    sell_yes_order_id: Optional[str] = None
    sell_no_order_id: Optional[str] = None
    entries_submitted: bool = False
    exit_handled: bool = False
    post_entry_cleanup_done: bool = False
    min_yes_mid_cents_first5: Optional[int] = None

    def cid_yes(self) -> str:
        return f"b15m-{self.session_id}-y"

    def cid_no(self) -> str:
        return f"b15m-{self.session_id}-n"

    def cid_sell_yes(self) -> str:
        return f"b15m-{self.session_id}-sy"

    def cid_sell_no(self) -> str:
        return f"b15m-{self.session_id}-sn"


def _trade_db_path() -> Path:
    d = _ROOT / "btc15m_data"
    d.mkdir(parents=True, exist_ok=True)
    return d / "trades.db"


def _init_trade_db() -> None:
    path = _trade_db_path()
    conn = sqlite3.connect(path, timeout=10)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS btc_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ended_at_utc TEXT NOT NULL,
                ended_hour_utc INTEGER NOT NULL,
                market_ticker TEXT,
                market_open_utc TEXT,
                market_close_utc TEXT,
                entry_cents INTEGER,
                exit_cents INTEGER,
                yes_entry_fills REAL,
                no_entry_fills REAL,
                exit_handled INTEGER,
                lowest_yes_mid_cents_first5 INTEGER,
                success INTEGER NOT NULL,
                prev1_success INTEGER,
                prev2_success INTEGER,
                prev3_success INTEGER,
                prev4_success INTEGER
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS btc_order_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                at_utc TEXT NOT NULL,
                kind TEXT NOT NULL,
                market_ticker TEXT,
                session_id TEXT,
                order_id TEXT,
                order_id_secondary TEXT,
                side TEXT,
                count INTEGER,
                price_cents INTEGER
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def _append_btc_order_event(
    kind: str,
    *,
    market_ticker: str,
    session_id: str,
    order_id: Optional[str] = None,
    order_id_secondary: Optional[str] = None,
    side: Optional[str] = None,
    count: Optional[int] = None,
    price_cents: Optional[int] = None,
) -> None:
    """Append a row when orders are successfully placed (diagnostics; does not affect success stats)."""
    _init_trade_db()
    at = _utc_now().astimezone(timezone.utc).isoformat()
    try:
        path = _trade_db_path()
        conn = sqlite3.connect(path, timeout=10)
        try:
            conn.execute(
                """
                INSERT INTO btc_order_events (
                    at_utc, kind, market_ticker, session_id,
                    order_id, order_id_secondary, side, count, price_cents
                ) VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (
                    at,
                    kind,
                    market_ticker,
                    session_id,
                    order_id,
                    order_id_secondary,
                    side,
                    count,
                    price_cents,
                ),
            )
            conn.commit()
        finally:
            conn.close()
        LOG.info(
            "[TRADE_LOG] order_event kind=%s ticker=%s session=%s",
            kind,
            market_ticker,
            session_id,
        )
    except Exception:
        LOG.exception("Trade log: failed to write btc_order_events row")


def _yes_mid_cents_from_snap(snap: Any) -> Optional[int]:
    bid = getattr(snap, "yes_bid_dollars", None)
    ask = getattr(snap, "yes_ask_dollars", None)
    try:
        bc = int(round(float(bid) * 100)) if bid not in (None, "") else None
        ac = int(round(float(ask) * 100)) if ask not in (None, "") else None
        if bc is not None and ac is not None:
            return (bc + ac) // 2
        if bc is not None:
            return bc
        if ac is not None:
            return ac
    except (TypeError, ValueError):
        pass
    return None


def _prev_four_success_columns(conn: sqlite3.Connection) -> tuple[Optional[int], ...]:
    cur = conn.execute(
        "SELECT success FROM btc_sessions ORDER BY id DESC LIMIT 4",
    )
    asc = list(reversed([r[0] for r in cur.fetchall()]))
    out: list[Optional[int]] = [None, None, None, None]
    n = len(asc)
    for j, v in enumerate(asc):
        out[4 - n + j] = int(v) if v is not None else None
    return (out[0], out[1], out[2], out[3])


def _compute_session_success(
    session: Session,
    entry_cents: int,
    exit_cents: int,
    yes_f: float,
    no_f: float,
) -> int:
    if not session.exit_handled:
        return 0
    if yes_f > 0 and no_f <= 0:
        return 1 if exit_cents > entry_cents else 0
    if no_f > 0 and yes_f <= 0:
        return 1 if exit_cents > entry_cents else 0
    return 0


def _log_btc_session_row(
    client: KalshiClient,
    session: Session,
    ticker: str,
    open_t: datetime,
    close_t: datetime,
    entry_cents: int,
    exit_cents: int,
) -> None:
    _init_trade_db()
    ended = _utc_now()
    yes_f = 0.0
    no_f = 0.0
    if session.yes_order_id:
        try:
            yes_f = _fp(
                _get_order(
                    client,
                    session.yes_order_id,
                    ticker=ticker,
                    client_order_id=session.cid_yes(),
                    expected_side="yes",
                ).fill_count_fp
            )
        except Exception:
            LOG.debug("Trade log: could not read YES entry order", exc_info=True)
    if session.no_order_id:
        try:
            no_f = _fp(
                _get_order(
                    client,
                    session.no_order_id,
                    ticker=ticker,
                    client_order_id=session.cid_no(),
                    expected_side="no",
                ).fill_count_fp
            )
        except Exception:
            LOG.debug("Trade log: could not read NO entry order", exc_info=True)

    success = _compute_session_success(session, entry_cents, exit_cents, yes_f, no_f)
    hour_utc = ended.astimezone(timezone.utc).hour

    try:
        path = _trade_db_path()
        conn = sqlite3.connect(path, timeout=10)
        try:
            p1, p2, p3, p4 = _prev_four_success_columns(conn)
            conn.execute(
                """
                INSERT INTO btc_sessions (
                    ended_at_utc, ended_hour_utc, market_ticker,
                    market_open_utc, market_close_utc,
                    entry_cents, exit_cents,
                    yes_entry_fills, no_entry_fills, exit_handled,
                    lowest_yes_mid_cents_first5, success,
                    prev1_success, prev2_success, prev3_success, prev4_success
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    ended.astimezone(timezone.utc).isoformat(),
                    hour_utc,
                    ticker,
                    open_t.astimezone(timezone.utc).isoformat(),
                    close_t.astimezone(timezone.utc).isoformat(),
                    entry_cents,
                    exit_cents,
                    yes_f,
                    no_f,
                    1 if session.exit_handled else 0,
                    session.min_yes_mid_first5,
                    success,
                    p1,
                    p2,
                    p3,
                    p4,
                ),
            )
            conn.commit()
        finally:
            conn.close()
        LOG.info(
            "[TRADE_LOG] session saved | success=%s | yes_f=%.1f no_f=%.1f | "
            "lowest_yes_mid_5m=%s | exit_handled=%s",
            success,
            yes_f,
            no_f,
            session.min_yes_mid_first5,
            session.exit_handled,
        )
    except Exception:
        LOG.exception("Trade log: failed to write btc_sessions row")


def _find_open_btc15m(client: KalshiClient, series_ticker: str) -> Optional[Any]:
    r = client.get_markets(series_ticker=series_ticker, status="open", limit=50)
    markets = getattr(r, "markets", None) or []
    if not markets:
        return None
    now = _utc_now()
    in_window = []
    for m in markets:
        ot = _parse_ts(m.open_time)
        ct = _parse_ts(m.close_time)
        if ot <= now < ct:
            in_window.append(m)
    if in_window:
        return min(in_window, key=lambda x: _parse_ts(x.close_time))
    upcoming = [m for m in markets if _parse_ts(m.open_time) > now]
    if upcoming:
        return min(upcoming, key=lambda x: _parse_ts(x.open_time))
    return markets[0]


def _log_market_minute_debug(
    client: KalshiClient,
    ticker: str,
    open_t: datetime,
    close_t: datetime,
    now: datetime,
) -> None:
    """Fetch market snapshot and log time-in-market + YES/NO bid/ask."""
    try:
        snap = client.get_market(ticker=ticker).market
    except Exception as e:
        LOG.warning("Debug: get_market(%s) failed: %s", ticker, e)
        return

    yes_bid = _price_dollars_to_cents_str(getattr(snap, "yes_bid_dollars", None))
    yes_ask = _price_dollars_to_cents_str(getattr(snap, "yes_ask_dollars", None))
    no_bid = _price_dollars_to_cents_str(getattr(snap, "no_bid_dollars", None))
    no_ask = _price_dollars_to_cents_str(getattr(snap, "no_ask_dollars", None))
    st = getattr(snap, "status", None)

    if now < open_t:
        wait = open_t - now
        LOG.info(
            "[debug] %s | waiting for open | starts in %dm %ds | status=%s | "
            "YES bid/ask %s / %s | NO bid/ask %s / %s",
            ticker,
            int(wait.total_seconds() // 60),
            int(wait.total_seconds() % 60),
            st,
            yes_bid,
            yes_ask,
            no_bid,
            no_ask,
        )
        return

    elapsed = now - open_t
    total = close_t - open_t
    remain = close_t - now
    mins_in = int(elapsed.total_seconds() // 60)
    secs_in = int(elapsed.total_seconds() % 60)
    total_mins = max(1, int(round(total.total_seconds() / 60)))
    LOG.info(
        "[debug] %s | %dm %02ds since open (minute %d of ~%d) | closes in %dm %ds | status=%s | "
        "YES bid/ask %s / %s | NO bid/ask %s / %s",
        ticker,
        mins_in,
        secs_in,
        mins_in,
        total_mins,
        int(remain.total_seconds() // 60),
        int(remain.total_seconds() % 60),
        st,
        yes_bid,
        yes_ask,
        no_bid,
        no_ask,
    )


def _position_size(client: KalshiClient, ticker: str) -> float:
    r = client.get_positions(ticker=ticker, count_filter="position")
    positions = getattr(r, "market_positions", None) or []
    for p in positions:
        if getattr(p, "ticker", None) == ticker:
            return abs(_fp(getattr(p, "position_fp", "0")))
    return 0.0


def _place_entry(
    client: KalshiClient,
    session: Session,
    ticker: str,
    contracts: int,
    entry_cents: int,
) -> None:
    if session.entries_submitted:
        return
    session.entries_submitted = True
    LOG.info(
        "[TRADE] Submitting ENTRY buy YES x%d @ %dc | buy NO x%d @ %dc | %s",
        contracts,
        entry_cents,
        contracts,
        entry_cents,
        ticker,
    )
    try:
        yes = client.create_order(
            ticker=ticker,
            client_order_id=session.cid_yes(),
            side="yes",
            action="buy",
            count=contracts,
            yes_price=entry_cents,
            time_in_force="good_till_canceled",
        )
        session.yes_order_id = yes.order.order_id
        no = client.create_order(
            ticker=ticker,
            client_order_id=session.cid_no(),
            side="no",
            action="buy",
            count=contracts,
            no_price=entry_cents,
            time_in_force="good_till_canceled",
        )
        session.no_order_id = no.order.order_id
    except Exception:
        if session.yes_order_id:
            try:
                client.cancel_order(order_id=session.yes_order_id)
                LOG.warning(
                    "Partial entry: cancelled YES order %s after failure placing NO",
                    session.yes_order_id,
                )
            except Exception as ce:
                LOG.warning("Partial entry: could not cancel YES %s: %s", session.yes_order_id, ce)
        session.yes_order_id = None
        session.no_order_id = None
        session.entries_submitted = False
        raise
    LOG.info(
        "[TRADE] Entry orders live | YES order_id=%s | NO order_id=%s",
        session.yes_order_id,
        session.no_order_id,
    )
    _append_btc_order_event(
        "entry_pair",
        market_ticker=ticker,
        session_id=session.session_id,
        order_id=session.yes_order_id,
        order_id_secondary=session.no_order_id,
        side=None,
        count=contracts,
        price_cents=entry_cents,
    )


def _get_order(
    client: KalshiClient,
    order_id: str,
    *,
    ticker: Optional[str] = None,
    client_order_id: Optional[str] = None,
    expected_side: Optional[str] = None,
) -> Any:
    """Fetch order by id. Retries and falls back to get_orders(ticker) if GET-by-id 404s."""

    def _matches_side(o: Any) -> bool:
        if not expected_side:
            return True
        return getattr(o, "side", None) == expected_side

    last: Optional[Exception] = None
    for attempt in range(3):
        try:
            return client.get_order(order_id=order_id).order
        except NotFoundException as e:
            last = e
            if attempt < 2:
                time.sleep(0.25)
    if ticker:
        for _ in range(3):
            r = client.get_orders(ticker=ticker, limit=200)
            orders = getattr(r, "orders", None) or []
            for o in orders:
                if getattr(o, "order_id", None) == order_id and _matches_side(o):
                    LOG.info(
                        "Resolved order via get_orders (direct get_order was 404) | order_id=%s",
                        order_id,
                    )
                    return o
            if client_order_id:
                for o in orders:
                    if (
                        getattr(o, "client_order_id", None) == client_order_id
                        and _matches_side(o)
                    ):
                        oid = getattr(o, "order_id", None)
                        LOG.info(
                            "Resolved order via get_orders by client_order_id (GET-by-id 404) | order_id=%s",
                            oid,
                        )
                        return o
            time.sleep(0.25)
    if last:
        raise last
    raise RuntimeError("get_order failed")


def _cancel_if_resting(
    client: KalshiClient,
    order_id: Optional[str],
    *,
    ticker: Optional[str] = None,
    client_order_id: Optional[str] = None,
    expected_side: Optional[str] = None,
) -> None:
    if not order_id:
        return
    try:
        o = _get_order(
            client,
            order_id,
            ticker=ticker,
            client_order_id=client_order_id,
            expected_side=expected_side,
        )
        st = getattr(o.status, "value", o.status)
        if st == "resting":
            client.cancel_order(order_id=order_id)
            LOG.info("Cancelled order %s", order_id)
    except Exception as e:
        LOG.warning("Cancel %s: %s", order_id, e)


def _place_sell(
    client: KalshiClient,
    session: Session,
    ticker: str,
    side: str,
    count: int,
    exit_cents: int,
) -> None:
    cid = session.cid_sell_yes() if side == "yes" else session.cid_sell_no()
    kwargs: dict[str, Any] = {
        "ticker": ticker,
        "client_order_id": cid,
        "side": side,
        "action": "sell",
        "count": count,
        "time_in_force": "good_till_canceled",
    }
    if side == "yes":
        kwargs["yes_price"] = exit_cents
    else:
        kwargs["no_price"] = exit_cents
    LOG.info(
        "[TRADE] Submitting EXIT sell %s x%d @ %dc | %s",
        side,
        count,
        exit_cents,
        ticker,
    )
    r = client.create_order(**kwargs)
    oid = r.order.order_id
    if side == "yes":
        session.sell_yes_order_id = oid
    else:
        session.sell_no_order_id = oid
    LOG.info(
        "[TRADE] Exit order live | SELL %s order_id=%s @ %dc x%d",
        side,
        oid,
        exit_cents,
        count,
    )
    _append_btc_order_event(
        "exit",
        market_ticker=ticker,
        session_id=session.session_id,
        order_id=oid,
        order_id_secondary=None,
        side=side,
        count=count,
        price_cents=exit_cents,
    )


def _handle_fills(
    client: KalshiClient,
    session: Session,
    ticker: str,
    exit_cents: int,
) -> None:
    if session.exit_handled:
        return
    if not session.yes_order_id or not session.no_order_id:
        return

    yes_o = _get_order(
        client,
        session.yes_order_id,
        ticker=ticker,
        client_order_id=session.cid_yes(),
        expected_side="yes",
    )
    no_o = _get_order(
        client,
        session.no_order_id,
        ticker=ticker,
        client_order_id=session.cid_no(),
        expected_side="no",
    )
    yes_f = _fp(yes_o.fill_count_fp)
    no_f = _fp(no_o.fill_count_fp)

    if yes_f <= 0 and no_f <= 0:
        return

    session.exit_handled = True
    qty_yes = int(yes_f)
    qty_no = int(no_f)

    if yes_f > 0 and no_f <= 0:
        _cancel_if_resting(
            client,
            session.no_order_id,
            ticker=ticker,
            client_order_id=session.cid_no(),
            expected_side="no",
        )
        if qty_yes > 0:
            _place_sell(client, session, ticker, "yes", qty_yes, exit_cents)
    elif no_f > 0 and yes_f <= 0:
        _cancel_if_resting(
            client,
            session.yes_order_id,
            ticker=ticker,
            client_order_id=session.cid_yes(),
            expected_side="yes",
        )
        if qty_no > 0:
            _place_sell(client, session, ticker, "no", qty_no, exit_cents)
    else:
        _cancel_if_resting(
            client,
            session.yes_order_id,
            ticker=ticker,
            client_order_id=session.cid_yes(),
            expected_side="yes",
        )
        _cancel_if_resting(
            client,
            session.no_order_id,
            ticker=ticker,
            client_order_id=session.cid_no(),
            expected_side="no",
        )
        if qty_yes > 0:
            _place_sell(client, session, ticker, "yes", qty_yes, exit_cents)
        if qty_no > 0:
            _place_sell(client, session, ticker, "no", qty_no, exit_cents)
        LOG.warning("Both sides had fills; placed sells for each leg.")


def _cancel_entry_only_if_flat(
    client: KalshiClient,
    session: Session,
    ticker: str,
) -> None:
    if session.post_entry_cleanup_done:
        return
    pos = _position_size(client, ticker)
    if pos > 1e-6:
        session.post_entry_cleanup_done = True
        LOG.info("Post-entry window: position %.2f — leaving exit orders working.", pos)
        return
    yes_f = (
        _fp(
            _get_order(
                client,
                session.yes_order_id,
                ticker=ticker,
                client_order_id=session.cid_yes(),
                expected_side="yes",
            ).fill_count_fp
        )
        if session.yes_order_id
        else 0
    )
    no_f = (
        _fp(
            _get_order(
                client,
                session.no_order_id,
                ticker=ticker,
                client_order_id=session.cid_no(),
                expected_side="no",
            ).fill_count_fp
        )
        if session.no_order_id
        else 0
    )
    if yes_f > 0 or no_f > 0:
        session.post_entry_cleanup_done = True
        return

    LOG.info(
        "Entry window ended with no fill — cancelling resting entry orders on %s",
        ticker,
    )
    _cancel_if_resting(
        client,
        session.yes_order_id,
        ticker=ticker,
        client_order_id=session.cid_yes(),
        expected_side="yes",
    )
    _cancel_if_resting(
        client,
        session.no_order_id,
        ticker=ticker,
        client_order_id=session.cid_no(),
        expected_side="no",
    )
    session.post_entry_cleanup_done = True


def run_session(
    client: KalshiClient,
    *,
    series_ticker: str,
    contracts: int,
    entry_cents: int,
    exit_cents: int,
    entry_window_minutes: int,
    poll_seconds: float,
) -> None:
    m = _find_open_btc15m(client, series_ticker)
    if not m:
        LOG.warning("No open %s market; waiting…", series_ticker)
        return

    ticker = m.ticker
    open_t = _parse_ts(m.open_time)
    close_t = _parse_ts(m.close_time)
    entry_end = open_t + timedelta(minutes=entry_window_minutes)

    session = Session(market_ticker=ticker)
    LOG.info(
        "Market %s | open=%s close=%s | entry phase ends %s",
        ticker,
        open_t.isoformat(),
        close_t.isoformat(),
        entry_end.isoformat(),
    )

    last_debug_wall_minute: Optional[int] = None
    while _utc_now() < close_t:
        now = _utc_now()
        wall_minute = int(now.timestamp() // 60)
        if wall_minute != last_debug_wall_minute:
            last_debug_wall_minute = wall_minute
            _log_market_minute_debug(client, ticker, open_t, close_t, now)

        m = _find_open_btc15m(client, series_ticker)
        if not m:
            time.sleep(poll_seconds)
            continue
        if m.ticker != ticker:
            LOG.info("Market ticker changed; stopping session loop.")
            break

        if now >= open_t and not session.entries_submitted:
            if now <= entry_end:
                _place_entry(client, session, ticker, contracts, entry_cents)
            else:
                LOG.info("Joined after entry window; skipping entry placement.")
                session.entries_submitted = True
                session.post_entry_cleanup_done = True

        if open_t <= now < entry_end:
            try:
                snap_m = client.get_market(ticker=ticker).market
                mid = _yes_mid_cents_from_snap(snap_m)
                if mid is not None:
                    if (
                        session.min_yes_mid_first5 is None
                        or mid < session.min_yes_mid_first5
                    ):
                        session.min_yes_mid_first5 = mid
            except Exception:
                pass

        if session.entries_submitted and session.yes_order_id and session.no_order_id:
            _handle_fills(client, session, ticker, exit_cents)

            if now > entry_end and not session.exit_handled:
                _cancel_entry_only_if_flat(client, session, ticker)

        time.sleep(poll_seconds)

    LOG.info("Market close reached for %s — stopping loop (exit orders left working).", ticker)
    _log_btc_session_row(client, session, ticker, open_t, close_t, entry_cents, exit_cents)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    series = os.environ.get("KALSHI_SERIES_TICKER", "KXBTC15M")
    contracts = int(os.environ.get("KALSHI_CONTRACTS", "10"))
    entry_cents = int(os.environ.get("KALSHI_ENTRY_CENTS", "25"))
    exit_cents = int(os.environ.get("KALSHI_EXIT_CENTS", "50"))
    entry_min = int(os.environ.get("KALSHI_ENTRY_WINDOW_MINUTES", "5"))
    poll = float(os.environ.get("KALSHI_POLL_SECONDS", "1.0"))

    client = _load_client()
    _init_trade_db()
    LOG.info(
        "BTC 15m bot | series=%s contracts=%d entry=%dc exit=%dc window=%dm",
        series,
        contracts,
        entry_cents,
        exit_cents,
        entry_min,
    )

    while True:
        try:
            run_session(
                client,
                series_ticker=series,
                contracts=contracts,
                entry_cents=entry_cents,
                exit_cents=exit_cents,
                entry_window_minutes=entry_min,
                poll_seconds=poll,
            )
        except KeyboardInterrupt:
            LOG.info("Interrupted.")
            raise SystemExit(0)
        except Exception:
            LOG.exception("Session error; retrying in 30s")
            time.sleep(30)
        else:
            LOG.info("Session finished; waiting 5s for next market…")
            time.sleep(5)


if __name__ == "__main__":
    main()
