#!/usr/bin/env python3
"""
Debug Kalshi settlement P&L using the same rules as settlement_sync.py.

Important (especially for KXBTC15M dual-sided trading):
  Each settlement row is one *market ticker* at resolution. Kalshi reports
  ``yes_total_cost`` / ``no_total_cost`` as **gross** cost per leg. API ``revenue``
  can be **net** (internal YES/NO offset), so this tool matches ``settlement_sync``:
  gross payout is **reconstructed** from counts and ``value``; net PnL is that payout
  minus both costs and fees. Summing every row in a series is *cumulative account
  P&L from all those markets*, not "one bot's closed sessions". It can be very
  large vs ``trade_outcomes`` in ``btc15m_data/trades.db``, which tracks your
  bot's session-level realized PnL.

Default output is compact (no per-row flood). Use ``--dump-rows`` for full row
logs (old behavior).

Examples (from repo root):

  .venv/bin/python control-panel/debug_settlement_pnl.py
  .venv/bin/python control-panel/debug_settlement_pnl.py --compare-btc15m-db
  .venv/bin/python control-panel/debug_settlement_pnl.py --dump-rows --strategy btc15m
  .venv/bin/python control-panel/debug_settlement_pnl.py --lookback-days 14 --write-ledger
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import yaml
from kalshi_python_sync.api.portfolio_api import PortfolioApi

_CP = Path(__file__).resolve().parent
_REPO = _CP.parent
if str(_CP) not in sys.path:
    sys.path.insert(0, str(_CP))

from kalshi_readout import load_strategy_env_map, make_kalshi_client  # noqa: E402
from settlement_sync import (  # noqa: E402
    classify_settlement_ticker,
    init_ledger_db,
    ledger_db_path,
    settlement_net_pnl_cents,
    sync_settlements_once,
)

LOG = logging.getLogger("debug_settlement_pnl")


def _load_strategies(repo: Path) -> list[dict[str, Any]]:
    yp = repo / "control-panel" / "strategies.yaml"
    raw = yaml.safe_load(yp.read_text(encoding="utf-8"))
    return list(raw.get("strategies") or [])


def _settlement_snapshot(st: Any) -> dict[str, Any]:
    keys = (
        "ticker",
        "settled_time",
        "event_ticker",
        "market_result",
        "revenue",
        "yes_count_fp",
        "no_count_fp",
        "yes_total_cost",
        "no_total_cost",
        "yes_total_cost_dollars",
        "no_total_cost_dollars",
        "fee_cost",
        "value",
        "yes_count",
        "no_count",
    )
    out: dict[str, Any] = {}
    for k in keys:
        if hasattr(st, k):
            out[k] = getattr(st, k)
    return out


def _trade_outcomes_pnl_cents(db_path: Path) -> Optional[tuple[int, int]]:
    """Returns (resolved_row_count, sum_pnl_cents) or None if DB/table missing."""
    if not db_path.is_file():
        return None
    try:
        conn = sqlite3.connect(str(db_path), timeout=15)
        try:
            cur = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='trade_outcomes'"
            )
            if not cur.fetchone():
                return None
            n, s = conn.execute(
                """
                SELECT COUNT(*), COALESCE(SUM(pnl_cents), 0)
                FROM trade_outcomes
                WHERE status = 'resolved' AND pnl_cents IS NOT NULL
                """
            ).fetchone()
            return int(n or 0), int(s or 0)
        finally:
            conn.close()
    except sqlite3.Error:
        return None


def _fetch_and_analyze(
    repo: Path,
    strategies: list[dict[str, Any]],
    *,
    env_file: str,
    lookback_days: int,
    strategy_filter: Optional[set[str]],
    write_ledger: bool,
    dump_rows: bool,
    raw_samples: int,
    compare_btc15m_db: bool,
    btc_db_path: Path,
) -> int:
    if write_ledger:
        LOG.info("Writing ledger via sync_settlements_once (same as control panel job)")
        r = sync_settlements_once(repo, strategies, env_file=env_file, lookback_days=lookback_days)
        LOG.info("sync result: %s", r)
        init_ledger_db(repo)
        p = ledger_db_path(repo)
        conn = sqlite3.connect(str(p), timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.execute(
                """
                SELECT strategy_id,
                       COUNT(*) AS n,
                       COALESCE(SUM(COALESCE(net_pnl_cents, 0)), 0) AS pnl
                FROM kalshi_settlements
                GROUP BY strategy_id
                ORDER BY strategy_id
                """
            )
            LOG.info("=== LEDGER DB AFTER SYNC (by strategy_id) ===")
            for row in cur.fetchall():
                LOG.info("  %s | rows=%s | pnl_cents=%s", row["strategy_id"], row["n"], row["pnl"])
        finally:
            conn.close()
        return 0

    vals = load_strategy_env_map(repo, env_file)
    client = make_kalshi_client(repo, vals)
    if not client:
        LOG.error("Could not build Kalshi client (check %s and keys in repo)", env_file)
        return 1

    now = int(time.time())
    lookback_days = max(7, min(365 * 3, int(lookback_days)))
    min_ts = now - lookback_days * 86400
    LOG.info(
        "Fetching settlements: lookback_days=%s min_ts=%s (%s)",
        lookback_days,
        min_ts,
        datetime.fromtimestamp(min_ts, tz=timezone.utc).isoformat(),
    )
    LOG.info(
        "Interpretation: each row is one market at expiry. "
        "Summing many KXBTC15M rows = all account PnL from those markets in the window, "
        "not the same as bot session logs in trade_outcomes."
    )

    api = PortfolioApi(client)
    cursor: Optional[str] = None
    page = 0
    rows_by_strategy: dict[str, int] = defaultdict(int)
    pnl_by_strategy: dict[str, int] = defaultdict(int)
    total_rows = 0
    dual_leg_cost_rows = 0
    net_histogram: Counter[int] = Counter()
    samples_left = max(0, raw_samples)

    while True:
        page += 1
        LOG.debug("GET settlements page=%s cursor=%s limit=200 min_ts=%s", page, cursor, min_ts)
        resp = api.get_settlements(limit=200, cursor=cursor, min_ts=min_ts)
        fills = getattr(resp, "settlements", None) or []
        LOG.info("Page %s: %s settlement objects", page, len(fills))
        for st in fills:
            ticker = str(getattr(st, "ticker", "") or "")
            settled = getattr(st, "settled_time", None)
            if not ticker:
                continue
            if settled is None:
                continue
            if isinstance(settled, datetime):
                st_iso = settled.astimezone(timezone.utc).isoformat()
            else:
                st_iso = str(settled)

            api_rev = int(getattr(st, "revenue", None) or 0)
            gross_payout_cents, yc, nc, fc, net_cents = settlement_net_pnl_cents(st)
            if os.environ.get("CONTROL_PANEL_SETTLEMENT_USE_GROSS", "").strip().lower() in (
                "1",
                "true",
                "yes",
            ):
                net_cents = gross_payout_cents

            sid = classify_settlement_ticker(ticker, strategies)
            rows_by_strategy[sid] += 1
            pnl_by_strategy[sid] += int(net_cents)
            total_rows += 1
            if yc > 0 and nc > 0:
                dual_leg_cost_rows += 1
            if sid == "btc15m":
                net_histogram[int(net_cents)] += 1

            if samples_left > 0:
                samples_left -= 1
                snap = _settlement_snapshot(st)
                LOG.info("RAW_SAMPLE json=%s", json.dumps(snap, default=str))

            snap = _settlement_snapshot(st)
            detail = (
                f"ROW | strategy_id={sid} | ticker={ticker} | settled={st_iso} | "
                f"market_result={getattr(st, 'market_result', None)} | "
                f"gross_payout={gross_payout_cents} (api_revenue={api_rev}) "
                f"yes_cost={yc} no_cost={nc} fee={fc} | net_pnl={net_cents} | snap={snap}"
            )
            if not dump_rows:
                pass
            elif strategy_filter is not None and sid not in strategy_filter:
                LOG.debug(detail)
            else:
                LOG.info(detail)

        cursor = getattr(resp, "cursor", None) or None
        if not cursor:
            break

    LOG.info("=== SUMMARY: row counts by strategy_id ===")
    for k in sorted(rows_by_strategy.keys()):
        LOG.info("  %s: %s rows", k, rows_by_strategy[k])
    LOG.info("=== SUMMARY: sum(net_pnl_cents) by strategy_id ===")
    for k in sorted(pnl_by_strategy.keys()):
        LOG.info("  %s: %s cents ($%.2f)", k, pnl_by_strategy[k], pnl_by_strategy[k] / 100.0)

    assigned = {k: v for k, v in pnl_by_strategy.items() if k != "unassigned"}
    LOG.info(
        "TOTAL assigned (excl. unassigned): %s cents ($%.2f) across %s rows",
        sum(assigned.values()),
        sum(assigned.values()) / 100.0,
        sum(rows_by_strategy[k] for k in rows_by_strategy if k != "unassigned"),
    )
    LOG.info(
        "Rows with BOTH yes_cost>0 and no_cost>0: %s of %s (often dual-sided inventory in same market)",
        dual_leg_cost_rows,
        total_rows,
    )
    if "btc15m" in rows_by_strategy and net_histogram:
        top = net_histogram.most_common(12)
        LOG.info("BTC15m net_pnl histogram (top values): %s", top)

    if compare_btc15m_db:
        to = _trade_outcomes_pnl_cents(btc_db_path)
        st_btc = pnl_by_strategy.get("btc15m", 0)
        if to is None:
            LOG.warning(
                "Could not read trade_outcomes from %s (missing file or table).",
                btc_db_path,
            )
        else:
            n_to, sum_to = to
            LOG.info(
                "COMPARE btc15m: settlements sum (this run) = %s cents ($%.2f) | "
                "trade_outcomes DB: %s resolved rows, sum = %s cents ($%.2f)",
                st_btc,
                st_btc / 100.0,
                n_to,
                sum_to,
                sum_to / 100.0,
            )
            LOG.info(
                "Large gap is expected if settlements include many windows / dual-leg residual; "
                "trade_outcomes reflects bot-logged closed sessions only."
            )

    if "unassigned" in pnl_by_strategy:
        LOG.warning(
            "unassigned PnL: %s cents — tickers did not match any strategy prefix/rule",
            pnl_by_strategy["unassigned"],
        )
    LOG.info("Settlements scanned (revenue field present): %s", total_rows)
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="Debug Kalshi settlement PnL / bot attribution.")
    p.add_argument("--repo", type=Path, default=_REPO, help="Repository root")
    p.add_argument("--env", default=".env", help="Env file relative to repo")
    p.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="Override lookback (default: CONTROL_PANEL_SETTLEMENT_LOOKBACK_DAYS or 120)",
    )
    p.add_argument(
        "--strategy",
        action="append",
        dest="strategies",
        metavar="ID",
        help="With --dump-rows: only dump rows for these strategy_ids (repeatable).",
    )
    p.add_argument("--write-ledger", action="store_true", help="Persist settlement_ledger.db")
    p.add_argument("--http-debug", action="store_true", help="urllib3/requests DEBUG")
    p.add_argument(
        "--dump-rows",
        action="store_true",
        help="Log every settlement row at INFO (very large). Default: off.",
    )
    p.add_argument(
        "--raw-sample",
        type=int,
        default=0,
        metavar="N",
        help="Log first N settlements as JSON snapshots at INFO (small, useful for API shape).",
    )
    p.add_argument(
        "--compare-btc15m-db",
        action="store_true",
        help="After summary, compare btc15m settlement sum vs trade_outcomes in btc DB.",
    )
    p.add_argument(
        "--btc-db",
        type=Path,
        default=None,
        help="Path to btc15m trades.db (default: <repo>/btc15m_data/trades.db)",
    )
    p.add_argument(
        "--log-level",
        choices=("warning", "info", "debug"),
        default="info",
        help="Root log level (default: info). Use debug only when diagnosing pagination.",
    )
    args = p.parse_args()
    repo = args.repo.resolve()
    btc_db = (args.btc_db or (repo / "btc15m_data" / "trades.db")).resolve()

    root_level = getattr(logging, args.log_level.upper())
    logging.basicConfig(level=root_level, format="%(levelname)s %(name)s: %(message)s", stream=sys.stderr)
    logging.getLogger("urllib3").setLevel(logging.DEBUG if args.http_debug else logging.WARNING)
    logging.getLogger("requests").setLevel(logging.DEBUG if args.http_debug else logging.WARNING)

    lb = args.lookback_days
    if lb is None:
        try:
            lb = int(os.environ.get("CONTROL_PANEL_SETTLEMENT_LOOKBACK_DAYS", "120"))
        except ValueError:
            lb = 120

    strat_filter: Optional[set[str]] = set(args.strategies) if args.strategies else None
    if args.dump_rows and strat_filter:
        LOG.info("--dump-rows with filter strategy_id in: %s", sorted(strat_filter))

    strategies = _load_strategies(repo)
    LOG.info("Loaded %s strategies from control-panel/strategies.yaml", len(strategies))

    return _fetch_and_analyze(
        repo,
        strategies,
        env_file=args.env,
        lookback_days=lb,
        strategy_filter=strat_filter,
        write_ledger=args.write_ledger,
        dump_rows=args.dump_rows,
        raw_samples=args.raw_sample,
        compare_btc15m_db=args.compare_btc15m_db,
        btc_db_path=btc_db,
    )


if __name__ == "__main__":
    raise SystemExit(main())
