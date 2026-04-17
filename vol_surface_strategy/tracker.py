"""SQLite position tracker: one row per market period."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = _ROOT / "vol_surface_data" / "tracker.db"


def _conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def init_db() -> None:
    with _conn() as c:
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS positions (
                key TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                market_type TEXT,
                city_id TEXT,
                resolution_date TEXT,
                hour_start_utc TEXT,
                order_id TEXT,
                ticker TEXT,
                side TEXT,
                entry_cents INTEGER,
                contracts INTEGER,
                deployed_cents INTEGER,
                created_utc TEXT,
                updated_utc TEXT
            )
            """
        )
        c.commit()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class PositionRow:
    key: str
    status: str
    market_type: str
    city_id: Optional[str]
    resolution_date: Optional[str]
    hour_start_utc: Optional[str]
    order_id: Optional[str]
    ticker: Optional[str]
    side: Optional[str]
    entry_cents: Optional[int]
    contracts: Optional[int]
    deployed_cents: int


def weather_key(city_id: str, hi_lo: str, resolution_date: date) -> str:
    return f"w:{city_id}:{hi_lo}:{resolution_date.isoformat()}"


def btc_key(hour_start: datetime) -> str:
    hs = hour_start.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return f"btc:{hs.isoformat()}"


def sports_key(event_ticker: str, series_ticker: str, ladder_shard: str = "") -> str:
    """One open vol-surface position per (event, series[, shard]) ladder — same spirit as one key per BTC hour."""
    et = (event_ticker or "").strip()
    st = (series_ticker or "").strip()
    sh = (ladder_shard or "").strip()
    if not sh:
        return f"s:{et}:{st}"
    return f"s:{et}:{st}#{sh}"


def get_row(key: str) -> Optional[PositionRow]:
    init_db()
    with _conn() as c:
        r = c.execute("SELECT * FROM positions WHERE key = ?", (key,)).fetchone()
        if not r:
            return None
        return PositionRow(
            key=r["key"],
            status=r["status"],
            market_type=r["market_type"],
            city_id=r["city_id"],
            resolution_date=r["resolution_date"],
            hour_start_utc=r["hour_start_utc"],
            order_id=r["order_id"],
            ticker=r["ticker"],
            side=r["side"],
            entry_cents=r["entry_cents"],
            contracts=r["contracts"],
            deployed_cents=int(r["deployed_cents"] or 0),
        )


def upsert_pending(
    key: str,
    *,
    market_type: str,
    city_id: Optional[str] = None,
    resolution_date: Optional[str] = None,
    hour_start_utc: Optional[str] = None,
    status: str = "pending_first_scan",
) -> None:
    init_db()
    t = _now()
    with _conn() as c:
        c.execute(
            """
            INSERT INTO positions (key, status, market_type, city_id, resolution_date, hour_start_utc,
                order_id, ticker, side, entry_cents, contracts, deployed_cents, created_utc, updated_utc)
            VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, 0, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                status = excluded.status,
                updated_utc = excluded.updated_utc
            """,
            (key, status, market_type, city_id, resolution_date, hour_start_utc, t, t),
        )
        c.commit()


def update_status(key: str, status: str, **kwargs: object) -> None:
    init_db()
    t = _now()
    fields = ["status = ?", "updated_utc = ?"]
    vals: list = [status, t]
    for k, v in kwargs.items():
        fields.append(f"{k} = ?")
        vals.append(v)
    vals.append(key)
    with _conn() as c:
        c.execute(f"UPDATE positions SET {', '.join(fields)} WHERE key = ?", vals)
        c.commit()


def deployed_cents_total(portfolio_total_cents: int) -> tuple[int, float]:
    init_db()
    with _conn() as c:
        r = c.execute(
            "SELECT COALESCE(SUM(deployed_cents), 0) FROM positions WHERE status IN "
            "('order_resting','position_active','filled')"
        ).fetchone()
        total = int(r[0] or 0)
    frac = total / portfolio_total_cents if portfolio_total_cents > 0 else 0.0
    return total, min(1.0, frac)


def list_all_rows(limit: int = 200) -> list[PositionRow]:
    """Recent tracker rows (for dashboard)."""
    init_db()
    lim = max(1, min(500, int(limit)))
    with _conn() as c:
        rows = c.execute(
            f"SELECT * FROM positions ORDER BY updated_utc DESC LIMIT {lim}"
        ).fetchall()
    out: list[PositionRow] = []
    for r in rows:
        out.append(
            PositionRow(
                key=r["key"],
                status=r["status"],
                market_type=r["market_type"],
                city_id=r["city_id"],
                resolution_date=r["resolution_date"],
                hour_start_utc=r["hour_start_utc"],
                order_id=r["order_id"],
                ticker=r["ticker"],
                side=r["side"],
                entry_cents=r["entry_cents"],
                contracts=r["contracts"],
                deployed_cents=int(r["deployed_cents"] or 0),
            )
        )
    return out


def list_btc_resting() -> list[sqlite3.Row]:
    init_db()
    with _conn() as c:
        return list(
            c.execute(
                "SELECT key, order_id, ticker FROM positions WHERE market_type = 'btc_hourly' AND status = 'order_resting'"
            ).fetchall()
        )


def daily_cleanup_utc() -> None:
    """Drop terminal weather rows from prior resolution dates (UTC date)."""
    init_db()
    today = date.today().isoformat()
    with _conn() as c:
        c.execute(
            """
            DELETE FROM positions
            WHERE status IN ('resolved', 'expired')
              AND resolution_date IS NOT NULL
              AND resolution_date < ?
            """,
            (today,),
        )
        c.commit()
