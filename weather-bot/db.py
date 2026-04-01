"""SQLite logging for forecasts, signals, trades, and errors."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

_DB_DIR = Path(__file__).resolve().parent / "db"
DB_PATH = _DB_DIR / "trades.db"


def init_db() -> None:
    _DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS forecasts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT, city TEXT,
            projected_high REAL, forecast_high REAL,
            observed_high REAL, current_temp REAL,
            forecast_age_minutes REAL, is_stale INTEGER
        )
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT, city TEXT, ticker TEXT, bucket_title TEXT,
            model_prob REAL, market_prob REAL, edge REAL,
            side TEXT, yes_price_cents INTEGER,
            action_taken TEXT, skip_reason TEXT
        )
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            open_ts TEXT, city TEXT, ticker TEXT, bucket_title TEXT,
            side TEXT, entry_price_cents INTEGER, contracts INTEGER,
            close_ts TEXT, close_price_cents INTEGER,
            close_reason TEXT, pnl_cents INTEGER
        )
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT, source TEXT, endpoint TEXT,
            status_code INTEGER, message TEXT
        )
        """
    )

    conn.commit()
    conn.close()


def log_forecast(data: dict) -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        INSERT INTO forecasts (ts, city, projected_high, forecast_high,
            observed_high, current_temp, forecast_age_minutes, is_stale)
        VALUES (?,?,?,?,?,?,?,?)
        """,
        (
            datetime.now(timezone.utc).isoformat(),
            data["city"],
            data["projected_high"],
            data.get("forecast_high"),
            data.get("observed_high_so_far"),
            data.get("current_temp"),
            data.get("forecast_age_minutes"),
            int(data.get("is_stale", 0)),
        ),
    )
    conn.commit()
    conn.close()


def log_signal(city: str, signal: dict, action: str, skip_reason: str | None = None) -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        INSERT INTO signals (ts, city, ticker, bucket_title, model_prob,
            market_prob, edge, side, yes_price_cents, action_taken, skip_reason)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            datetime.now(timezone.utc).isoformat(),
            city,
            signal["ticker"],
            signal.get("title"),
            signal["model_prob"],
            signal["market_prob"],
            signal["edge"],
            signal.get("side"),
            signal.get("limit_price_cents"),
            action,
            skip_reason,
        ),
    )
    conn.commit()
    conn.close()


def log_trade_open(city: str, signal: dict, contracts: int) -> int:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO trades (open_ts, city, ticker, bucket_title, side,
            entry_price_cents, contracts)
        VALUES (?,?,?,?,?,?,?)
        """,
        (
            datetime.now(timezone.utc).isoformat(),
            city,
            signal["ticker"],
            signal.get("title"),
            signal["side"],
            signal["limit_price_cents"],
            contracts,
        ),
    )
    conn.commit()
    trade_id = c.lastrowid
    conn.close()
    return int(trade_id)


def log_trade_close(trade_id: int, close_price_cents: int, close_reason: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT side, entry_price_cents, contracts FROM trades WHERE id=?",
        (trade_id,),
    )
    row = c.fetchone()
    if row:
        side, entry, contracts = row
        if side == "yes":
            pnl = (close_price_cents - entry) * contracts
        else:
            pnl = (close_price_cents - entry) * contracts

        c.execute(
            """
            UPDATE trades SET close_ts=?, close_price_cents=?,
                close_reason=?, pnl_cents=? WHERE id=?
            """,
            (
                datetime.now(timezone.utc).isoformat(),
                close_price_cents,
                close_reason,
                pnl,
                trade_id,
            ),
        )
        conn.commit()
    conn.close()


def log_error(source: str, endpoint: str, status_code: int, message: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        INSERT INTO errors (ts, source, endpoint, status_code, message)
        VALUES (?,?,?,?,?)
        """,
        (
            datetime.now(timezone.utc).isoformat(),
            source,
            endpoint,
            status_code,
            message,
        ),
    )
    conn.commit()
    conn.close()


def count_open_trades() -> int:
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT COUNT(*) FROM trades WHERE close_ts IS NULL",
    ).fetchone()
    conn.close()
    return int(row[0]) if row else 0


def realized_pnl_today_cents(risk_tz: str) -> int:
    """Sum realized PnL (closed trades) for the current calendar day in risk_tz."""
    import pytz

    tz = pytz.timezone(risk_tz)
    today = datetime.now(tz).date()
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT pnl_cents, close_ts FROM trades WHERE close_ts IS NOT NULL AND pnl_cents IS NOT NULL"
    ).fetchall()
    conn.close()
    total = 0
    for pnl, cts in rows:
        s = cts.replace("Z", "+00:00") if cts.endswith("Z") else cts
        cdt = datetime.fromisoformat(s)
        if cdt.tzinfo is None:
            cdt = cdt.replace(tzinfo=timezone.utc)
        if cdt.astimezone(tz).date() == today:
            total += int(pnl)
    return total


def fetch_open_trades_by_ticker() -> dict[str, dict]:
    """Hydrate tracking from DB (open rows only)."""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        """
        SELECT id, ticker, side, entry_price_cents, contracts
        FROM trades WHERE close_ts IS NULL
        """
    ).fetchall()
    conn.close()
    out: dict[str, dict] = {}
    for tid, ticker, side, ep, c in rows:
        out[str(ticker)] = {
            "trade_id": int(tid),
            "side": side,
            "entry_price_cents": int(ep),
            "contracts": int(c),
        }
    return out


def fetch_open_trades_for_city(city: str) -> dict[str, dict]:
    """Open trades for one city (for early-exit tracking)."""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        """
        SELECT id, ticker, side, entry_price_cents, contracts
        FROM trades WHERE close_ts IS NULL AND city=?
        """,
        (city,),
    ).fetchall()
    conn.close()
    out: dict[str, dict] = {}
    for tid, ticker, side, ep, c in rows:
        out[str(ticker)] = {
            "trade_id": int(tid),
            "side": side,
            "entry_price_cents": int(ep),
            "contracts": int(c),
        }
    return out
