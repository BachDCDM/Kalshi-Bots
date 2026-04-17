"""SQLite panel DB: last scan per market, order events, optional settlement P&amp;L."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

_ROOT = Path(__file__).resolve().parent.parent
PANEL_DB_PATH = _ROOT / "vol_surface_data" / "panel.db"


def _conn() -> sqlite3.Connection:
    PANEL_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(PANEL_DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def init_panel_db() -> None:
    with _conn() as c:
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS last_scan (
                market_key TEXT PRIMARY KEY,
                updated_utc TEXT NOT NULL,
                market_type TEXT,
                action TEXT,
                reason TEXT,
                edge_cents REAL,
                outlier_ticker TEXT,
                side TEXT,
                entry_cents INTEGER,
                contracts INTEGER,
                sigma_star REAL,
                underlying REAL,
                gate_log_json TEXT
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS order_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                at_utc TEXT NOT NULL,
                market_key TEXT NOT NULL,
                event TEXT NOT NULL,
                ticker TEXT,
                side TEXT,
                contracts INTEGER,
                price_cents INTEGER,
                order_id TEXT,
                detail TEXT
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_outcomes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_key TEXT NOT NULL,
                ticker TEXT NOT NULL,
                market_type TEXT,
                side TEXT,
                contracts INTEGER,
                entry_cents INTEGER,
                cost_cents INTEGER,
                placed_at_utc TEXT,
                pnl_cents INTEGER,
                status TEXT NOT NULL,
                resolved_utc TEXT,
                note TEXT
            )
            """
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_order_events_key ON order_events(market_key, at_utc DESC)"
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_trade_outcomes_key ON trade_outcomes(market_key)"
        )
        c.commit()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def record_last_scan(
    market_key: str,
    *,
    market_type: str,
    res: Any,
) -> None:
    """Persist latest run_scan result for the control panel."""
    init_panel_db()
    gate_log = getattr(res, "gate_log", None) or []
    glj = json.dumps(gate_log, default=str)[:16000]
    t = _now_iso()
    with _conn() as c:
        c.execute(
            """
            INSERT INTO last_scan (
                market_key, updated_utc, market_type, action, reason,
                edge_cents, outlier_ticker, side, entry_cents, contracts,
                sigma_star, underlying, gate_log_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(market_key) DO UPDATE SET
                updated_utc=excluded.updated_utc,
                market_type=excluded.market_type,
                action=excluded.action,
                reason=excluded.reason,
                edge_cents=excluded.edge_cents,
                outlier_ticker=excluded.outlier_ticker,
                side=excluded.side,
                entry_cents=excluded.entry_cents,
                contracts=excluded.contracts,
                sigma_star=excluded.sigma_star,
                underlying=excluded.underlying,
                gate_log_json=excluded.gate_log_json
            """,
            (
                market_key,
                t,
                market_type,
                getattr(res, "action", None),
                getattr(res, "reason", None),
                getattr(res, "edge_cents", None),
                getattr(res, "outlier_ticker", None),
                getattr(res, "side", None),
                getattr(res, "entry_cents", None),
                getattr(res, "contracts_to_buy", None),
                getattr(res, "sigma_star", None),
                getattr(res, "underlying", None),
                glj,
            ),
        )
        c.commit()


def record_order_event(
    market_key: str,
    event: str,
    *,
    ticker: Optional[str] = None,
    side: Optional[str] = None,
    contracts: Optional[int] = None,
    price_cents: Optional[int] = None,
    order_id: Optional[str] = None,
    detail: Optional[str] = None,
) -> None:
    init_panel_db()
    with _conn() as c:
        c.execute(
            """
            INSERT INTO order_events (
                at_utc, market_key, event, ticker, side, contracts, price_cents, order_id, detail
            ) VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (
                _now_iso(),
                market_key,
                event,
                ticker,
                side,
                contracts,
                price_cents,
                order_id,
                detail,
            ),
        )
        c.commit()


def upsert_open_trade(
    market_key: str,
    *,
    ticker: str,
    market_type: str,
    side: str,
    contracts: int,
    entry_cents: int,
    placed_at_utc: Optional[str] = None,
) -> None:
    """Record a position we expect to settle (for P&amp;L once market finalizes)."""
    init_panel_db()
    cost = max(0, contracts * entry_cents)
    t = placed_at_utc or _now_iso()
    with _conn() as c:
        c.execute("DELETE FROM trade_outcomes WHERE market_key = ? AND status = 'open'", (market_key,))
        c.execute(
            """
            INSERT INTO trade_outcomes (
                market_key, ticker, market_type, side, contracts, entry_cents,
                cost_cents, placed_at_utc, pnl_cents, status, note
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                market_key,
                ticker,
                market_type,
                side,
                contracts,
                entry_cents,
                cost,
                t,
                None,
                "open",
                None,
            ),
        )
        c.commit()


def mark_trade_resolved(
    market_key: str,
    *,
    pnl_cents: int,
    note: str = "",
) -> None:
    init_panel_db()
    t = _now_iso()
    with _conn() as c:
        c.execute(
            """
            UPDATE trade_outcomes
            SET status = 'resolved', pnl_cents = ?, resolved_utc = ?, note = COALESCE(?, note)
            WHERE market_key = ? AND status = 'open'
            """,
            (pnl_cents, t, note or None, market_key),
        )
        c.commit()


def resolve_open_trades_for_kalshi_settlement(
    *,
    ticker: str,
    net_pnl_cents: int,
    resolved_utc: str,
) -> int:
    """
    When the control panel syncs Kalshi settlements, mark any **open** vol-surface row
    with this ``ticker`` as resolved using **net** P&amp;L (same basis as ``settlement_ledger``).

    Returns the number of rows updated (normally 0 or 1).
    """
    if not ticker or not str(ticker).strip():
        return 0
    init_panel_db()
    note = f"Kalshi settlement net_pnl_cents={int(net_pnl_cents)}"
    with _conn() as c:
        cur = c.execute(
            """
            UPDATE trade_outcomes
            SET status = 'resolved', pnl_cents = ?, resolved_utc = ?, note = ?
            WHERE ticker = ? AND status = 'open'
            """,
            (int(net_pnl_cents), resolved_utc, note, str(ticker).strip()),
        )
        n = int(cur.rowcount or 0)
        c.commit()
    return n


def sum_realized_pnl_cents() -> int:
    init_panel_db()
    with _conn() as c:
        r = c.execute(
            "SELECT COALESCE(SUM(pnl_cents), 0) FROM trade_outcomes WHERE status = 'resolved' AND pnl_cents IS NOT NULL"
        ).fetchone()
        return int(r[0] or 0)


def sum_pnl_by_market_type() -> dict[str, int]:
    init_panel_db()
    out: dict[str, int] = {
        "btc_hourly": 0,
        "weather_high": 0,
        "weather_low": 0,
        "sports_vol_surface": 0,
    }
    with _conn() as c:
        cur = c.execute(
            """
            SELECT COALESCE(market_type, ''), COALESCE(SUM(pnl_cents), 0)
            FROM trade_outcomes
            WHERE status = 'resolved' AND pnl_cents IS NOT NULL
            GROUP BY market_type
            """
        )
        for mt, s in cur.fetchall():
            key = str(mt or "")
            if key == "btc_hourly":
                out["btc_hourly"] = int(s or 0)
            elif key == "weather_high":
                out["weather_high"] = int(s or 0)
            elif key == "weather_low":
                out["weather_low"] = int(s or 0)
            elif key == "sports_vol_surface":
                out["sports_vol_surface"] = int(s or 0)
    return out
