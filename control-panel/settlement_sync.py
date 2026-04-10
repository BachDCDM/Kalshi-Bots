"""
Sync Kalshi settlement history into a local SQLite ledger for per-strategy P&L.

Uses PortfolioApi.get_settlements (authoritative realized revenue per market).
Classifies each settlement to a strategy id:

1. Optional ``settlement_prefixes`` on each strategy in ``strategies.yaml`` (first match wins).
2. Defaults: ``btc15m`` (KXBTC15M / BTC15M), ``vol_surface`` (KXBTC- hourly),
   ``weather`` (KXHIGH / KXLOW / KXHIGHT …).

Weather vs vol-surface share many tickers; use ``settlement_prefixes`` on one or both
strategies to split (e.g. list series only your vol bot trades).

Dedupe key: (ticker, settled_time_iso). New syncs INSERT OR REPLACE so late API updates apply.
"""

from __future__ import annotations

import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from kalshi_python_sync.api.portfolio_api import PortfolioApi

from kalshi_readout import load_strategy_env_map, make_kalshi_client


def ledger_db_path(repo: Path) -> Path:
    p = (repo / "control-panel" / "data" / "settlement_ledger.db").resolve()
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _conn(repo: Path) -> sqlite3.Connection:
    c = sqlite3.connect(str(ledger_db_path(repo)), timeout=30)
    c.row_factory = sqlite3.Row
    return c


def init_ledger_db(repo: Path) -> None:
    with _conn(repo) as c:
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS kalshi_settlements (
                ticker TEXT NOT NULL,
                settled_time_iso TEXT NOT NULL,
                event_ticker TEXT,
                market_result TEXT,
                revenue_cents INTEGER NOT NULL,
                strategy_id TEXT NOT NULL,
                synced_at_utc TEXT NOT NULL,
                PRIMARY KEY (ticker, settled_time_iso)
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS settlement_sync_meta (
                k TEXT PRIMARY KEY,
                v TEXT NOT NULL
            )
            """
        )
        c.commit()


def _meta_get(repo: Path, key: str) -> Optional[str]:
    init_ledger_db(repo)
    with _conn(repo) as c:
        r = c.execute("SELECT v FROM settlement_sync_meta WHERE k = ?", (key,)).fetchone()
        return str(r[0]) if r else None


def _meta_set(repo: Path, key: str, value: str) -> None:
    init_ledger_db(repo)
    with _conn(repo) as c:
        c.execute(
            "INSERT INTO settlement_sync_meta (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
            (key, value),
        )
        c.commit()


def classify_settlement_ticker(ticker: str, strategies: list[dict[str, Any]]) -> str:
    """Return strategy_id or 'unassigned'."""
    u = ticker.strip().upper()
    for s in strategies:
        sid = s.get("id")
        if not sid:
            continue
        prefs = s.get("settlement_prefixes") or []
        if not isinstance(prefs, list):
            continue
        for p in prefs:
            pu = str(p).strip().upper()
            if pu and u.startswith(pu):
                return str(sid)
    if "BTC15M" in u or "KXBTC15M" in u:
        return "btc15m"
    if u.startswith("KXBTC-"):
        return "vol_surface"
    for pref in ("KXHIGH", "KXLOW", "KXHIGHT"):
        if u.startswith(pref):
            return "weather"
    return "unassigned"


def get_ledger_pnl_by_strategy(repo: Path) -> dict[str, int]:
    """Sum revenue_cents per strategy_id (excludes 'unassigned')."""
    init_ledger_db(repo)
    p = ledger_db_path(repo)
    if not p.is_file():
        return {}
    try:
        with _conn(repo) as c:
            cur = c.execute(
                """
                SELECT strategy_id, COALESCE(SUM(revenue_cents), 0)
                FROM kalshi_settlements
                WHERE strategy_id != 'unassigned'
                GROUP BY strategy_id
                """
            )
            return {str(row[0]): int(row[1] or 0) for row in cur.fetchall()}
    except sqlite3.Error:
        return {}


def ledger_row_counts(repo: Path) -> dict[str, int]:
    init_ledger_db(repo)
    p = ledger_db_path(repo)
    if not p.is_file():
        return {}
    try:
        with _conn(repo) as c:
            cur = c.execute(
                "SELECT strategy_id, COUNT(*) FROM kalshi_settlements GROUP BY strategy_id"
            )
            return {str(row[0]): int(row[1]) for row in cur.fetchall()}
    except sqlite3.Error:
        return {}


def sync_settlements_once(
    repo: Path,
    strategies: list[dict[str, Any]],
    *,
    env_file: str = ".env",
    lookback_days: Optional[int] = None,
) -> dict[str, Any]:
    """
    Pull settlements from Kalshi (paged), upsert into ledger. Returns summary dict.
    """
    init_ledger_db(repo)
    vals = load_strategy_env_map(repo, env_file)
    client = make_kalshi_client(repo, vals)
    if not client:
        return {"ok": False, "error": "no Kalshi client", "inserted": 0, "updated": 0}

    now = int(time.time())
    if lookback_days is None:
        try:
            lookback_days = int(os.environ.get("CONTROL_PANEL_SETTLEMENT_LOOKBACK_DAYS", "120"))
        except ValueError:
            lookback_days = 120
    lookback_days = max(7, min(365 * 3, int(lookback_days)))
    min_ts = now - lookback_days * 86400

    api = PortfolioApi(client)
    cursor: Optional[str] = None
    total_rows = 0
    by_strategy: dict[str, int] = {}
    err: Optional[str] = None
    try:
        while True:
            resp = api.get_settlements(limit=200, cursor=cursor, min_ts=min_ts)
            fills = getattr(resp, "settlements", None) or []
            synced = datetime.now(timezone.utc).isoformat()
            with _conn(repo) as c:
                for st in fills:
                    ticker = str(getattr(st, "ticker", "") or "")
                    if not ticker:
                        continue
                    settled = getattr(st, "settled_time", None)
                    if settled is None:
                        continue
                    if isinstance(settled, datetime):
                        st_iso = settled.astimezone(timezone.utc).isoformat()
                    else:
                        st_iso = str(settled)
                    ev = str(getattr(st, "event_ticker", "") or "")
                    mres = str(getattr(st, "market_result", "") or "")
                    rev = getattr(st, "revenue", None)
                    if rev is None:
                        continue
                    revenue_cents = int(rev)
                    sid = classify_settlement_ticker(ticker, strategies)
                    c.execute(
                        """
                        INSERT INTO kalshi_settlements (
                            ticker, settled_time_iso, event_ticker, market_result,
                            revenue_cents, strategy_id, synced_at_utc
                        ) VALUES (?,?,?,?,?,?,?)
                        ON CONFLICT(ticker, settled_time_iso) DO UPDATE SET
                            event_ticker=excluded.event_ticker,
                            market_result=excluded.market_result,
                            revenue_cents=excluded.revenue_cents,
                            strategy_id=excluded.strategy_id,
                            synced_at_utc=excluded.synced_at_utc
                        """,
                        (ticker, st_iso, ev, mres, revenue_cents, sid, synced),
                    )
                    total_rows += 1
                    by_strategy[sid] = by_strategy.get(sid, 0) + 1
                c.commit()
            cursor = getattr(resp, "cursor", None) or None
            if not cursor:
                break
        _meta_set(repo, "last_sync_ok_utc", datetime.now(timezone.utc).isoformat())
    except Exception as e:
        err = str(e)
        _meta_set(repo, "last_sync_error", err[:2000])

    return {
        "ok": err is None,
        "error": err,
        "settlements_processed": total_rows,
        "by_strategy_counts": by_strategy,
        "min_ts_used": min_ts,
        "ledger_db": str(ledger_db_path(repo)),
    }
