#!/usr/bin/env python3
"""
Export Kalshi portfolio fills and/or settlements to CSV.

**Fills** — trades only (no settlement payouts).

**Settlements** — resolution cash; use ``--with-settlements`` for a second CSV, or
``--combined`` for one **chronological** ledger (fills + settlements) with shared
columns plus ``detail_json`` for the full API object.

Default fills: ``~/Desktop/kalshi_fills_<utc>.csv``

  .venv/bin/python control-panel/export_fills_csv.py --days 30
  .venv/bin/python control-panel/export_fills_csv.py --days 30 --with-settlements
  .venv/bin/python control-panel/export_fills_csv.py --days 30 --combined
  .venv/bin/python control-panel/export_fills_csv.py --days 30 --combined -o ~/Desktop/all_events.csv
  .venv/bin/python control-panel/export_fills_csv.py --days 30 --combined --also-split
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import yaml

_CP = Path(__file__).resolve().parent
_REPO = _CP.parent
if str(_CP) not in sys.path:
    sys.path.insert(0, str(_CP))

from kalshi_python_sync.api.portfolio_api import PortfolioApi  # noqa: E402

from kalshi_readout import load_strategy_env_map, make_kalshi_client  # noqa: E402
from settlement_sync import (  # noqa: E402
    classify_settlement_ticker,
    settlement_net_pnl_cents,
)


def _cell(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, datetime):
        return v.astimezone(timezone.utc).isoformat()
    return str(v)


def _fill_dict_to_row(d: dict[str, Any]) -> dict[str, str]:
    return {k: _cell(v) for k, v in sorted(d.items())}


class _JsonObj:
    __slots__ = ("_d",)

    def __init__(self, d: dict[str, Any]) -> None:
        object.__setattr__(self, "_d", d)

    def __getattr__(self, name: str) -> Any:
        return self._d.get(name)


def _load_strategies(repo: Path) -> list[dict[str, Any]]:
    yp = repo / "control-panel" / "strategies.yaml"
    raw = yaml.safe_load(yp.read_text(encoding="utf-8"))
    return list(raw.get("strategies") or [])


def _fetch_fills_raw(
    api: PortfolioApi,
    *,
    min_ts: int,
    max_ts: int,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    cursor: Optional[str] = None
    pages = 0
    max_pages = 10_000
    while pages < max_pages:
        pages += 1
        http = api.get_fills_without_preload_content(
            limit=200,
            cursor=cursor,
            min_ts=min_ts,
            max_ts=max_ts,
        )
        chunk = http.read()
        if isinstance(chunk, bytes):
            chunk = chunk.decode("utf-8")
        payload = json.loads(chunk)
        fills = payload.get("fills") or []
        out.extend(fills)
        cur = payload.get("cursor") or ""
        if not fills:
            break
        if not cur or cur == cursor:
            break
        cursor = cur
    return out


def _fetch_settlements_raw(
    api: PortfolioApi,
    *,
    min_ts: int,
    max_ts: int,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    cursor: Optional[str] = None
    pages = 0
    max_pages = 10_000
    while pages < max_pages:
        pages += 1
        http = api.get_settlements_without_preload_content(
            limit=200,
            cursor=cursor,
            min_ts=min_ts,
            max_ts=max_ts,
        )
        chunk = http.read()
        if isinstance(chunk, bytes):
            chunk = chunk.decode("utf-8")
        payload = json.loads(chunk)
        batch = payload.get("settlements") or []
        out.extend(batch)
        cur = payload.get("cursor") or ""
        if not batch:
            break
        if not cur or cur == cursor:
            break
        cursor = cur
    return out


def _parse_epoch_seconds(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            pass
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except ValueError:
            return None
    return None


def _fill_event_ts(d: dict[str, Any]) -> tuple[float, str]:
    for key in ("ts", "created_time", "fill_ts", "time"):
        raw = d.get(key)
        sec = _parse_epoch_seconds(raw)
        if sec is not None:
            iso = datetime.fromtimestamp(sec, tz=timezone.utc).isoformat()
            return sec, iso
    return 0.0, ""


def _settlement_event_ts(d: dict[str, Any]) -> tuple[float, str]:
    for key in ("settled_time", "ts", "created_time"):
        raw = d.get(key)
        sec = _parse_epoch_seconds(raw)
        if sec is not None:
            iso = datetime.fromtimestamp(sec, tz=timezone.utc).isoformat()
            return sec, iso
    return 0.0, ""


def _market_ticker_fill(d: dict[str, Any]) -> str:
    return str(d.get("ticker") or d.get("market_ticker") or "")


def _settlement_rows_for_csv(
    raw: list[dict[str, Any]],
    *,
    repo: Path,
) -> list[dict[str, str]]:
    strategies = _load_strategies(repo)
    rows: list[dict[str, str]] = []
    for d in raw:
        row = _fill_dict_to_row(d)
        st = _JsonObj(d)
        ticker = str(getattr(st, "ticker", "") or "")
        row["strategy_id"] = classify_settlement_ticker(ticker, strategies) if ticker else ""
        try:
            gp, yc, nc, fc, net = settlement_net_pnl_cents(st)
        except Exception:
            gp, yc, nc, fc, net = (0, 0, 0, 0, 0)
        row["computed_gross_payout_cents"] = str(gp)
        row["computed_yes_cost_cents"] = str(yc)
        row["computed_no_cost_cents"] = str(nc)
        row["computed_fee_cents"] = str(fc)
        row["computed_net_pnl_cents"] = str(net)
        rows.append(row)
    return rows


COMBINED_FIELDNAMES = (
    "record_type",
    "event_ts",
    "event_ts_utc",
    "ticker",
    "strategy_id",
    "computed_gross_payout_cents",
    "computed_yes_cost_cents",
    "computed_no_cost_cents",
    "computed_fee_cents",
    "computed_net_pnl_cents",
    "detail_json",
)


def _combined_rows(
    raw_fills: list[dict[str, Any]],
    raw_settlements: list[dict[str, Any]],
    *,
    repo: Path,
) -> list[dict[str, str]]:
    strategies = _load_strategies(repo)
    keyed: list[tuple[tuple[float, str, str], dict[str, str]]] = []

    for d in raw_fills:
        ts, iso = _fill_event_ts(d)
        ticker = _market_ticker_fill(d)
        sid = classify_settlement_ticker(ticker, strategies) if ticker else ""
        row = {
            "record_type": "fill",
            "event_ts": f"{ts:.6f}",
            "event_ts_utc": iso,
            "ticker": ticker,
            "strategy_id": sid,
            "computed_gross_payout_cents": "",
            "computed_yes_cost_cents": "",
            "computed_no_cost_cents": "",
            "computed_fee_cents": "",
            "computed_net_pnl_cents": "",
            "detail_json": json.dumps(d, separators=(",", ":"), default=str),
        }
        keyed.append(((ts, "0fill", ticker), row))

    for d in raw_settlements:
        ts, iso = _settlement_event_ts(d)
        st = _JsonObj(d)
        ticker = str(getattr(st, "ticker", "") or "")
        sid = classify_settlement_ticker(ticker, strategies) if ticker else ""
        try:
            gp, yc, nc, fc, net = settlement_net_pnl_cents(st)
        except Exception:
            gp, yc, nc, fc, net = (0, 0, 0, 0, 0)
        row = {
            "record_type": "settlement",
            "event_ts": f"{ts:.6f}",
            "event_ts_utc": iso,
            "ticker": ticker,
            "strategy_id": sid,
            "computed_gross_payout_cents": str(gp),
            "computed_yes_cost_cents": str(yc),
            "computed_no_cost_cents": str(nc),
            "computed_fee_cents": str(fc),
            "computed_net_pnl_cents": str(net),
            "detail_json": json.dumps(d, separators=(",", ":"), default=str),
        }
        keyed.append(((ts, "1settlement", ticker), row))

    keyed.sort(key=lambda x: x[0])
    return [r for _, r in keyed]


def _write_dict_rows(path: Path, rows: list[dict[str, str]], fieldnames: Optional[list[str]] = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fn = fieldnames if fieldnames is not None else sorted({k for r in rows for k in r})
    with path.open("w", newline="", encoding="utf-8") as fp:
        w = csv.DictWriter(fp, fieldnames=fn, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def export_portfolio(
    *,
    repo: Path,
    env_file: str,
    days: int,
    fills_output: Optional[Path],
    settlements_output: Optional[Path],
    combined_output: Optional[Path],
) -> int:
    vals = load_strategy_env_map(repo, env_file)
    client = make_kalshi_client(repo, vals)
    if not client:
        print("Could not build Kalshi client; check .env / keys.", file=sys.stderr)
        return 1

    now = int(time.time())
    min_ts = now - max(1, min(365, days)) * 86400
    max_ts = now

    api = PortfolioApi(client)
    need_settlements = settlements_output is not None or combined_output is not None

    raw_fills = _fetch_fills_raw(api, min_ts=min_ts, max_ts=max_ts)
    raw_settlements = _fetch_settlements_raw(api, min_ts=min_ts, max_ts=max_ts) if need_settlements else []

    if fills_output is not None:
        if not raw_fills:
            print("No fills in window.", file=sys.stderr)
            fills_output.parent.mkdir(parents=True, exist_ok=True)
            fills_output.write_text("", encoding="utf-8")
        else:
            _write_dict_rows(fills_output, [_fill_dict_to_row(f) for f in raw_fills])
            print(f"Wrote {len(raw_fills)} fills to {fills_output}")

    if settlements_output is not None:
        if not raw_settlements:
            print("No settlements in window.", file=sys.stderr)
            settlements_output.parent.mkdir(parents=True, exist_ok=True)
            settlements_output.write_text("", encoding="utf-8")
        else:
            st_rows = _settlement_rows_for_csv(raw_settlements, repo=repo)
            _write_dict_rows(settlements_output, st_rows)
            print(f"Wrote {len(st_rows)} settlements to {settlements_output}")

    if combined_output is not None:
        rows = _combined_rows(raw_fills, raw_settlements, repo=repo)
        if not rows:
            print("No fills or settlements in window for combined export.", file=sys.stderr)
            combined_output.parent.mkdir(parents=True, exist_ok=True)
            combined_output.write_text("", encoding="utf-8")
        else:
            _write_dict_rows(combined_output, rows, fieldnames=list(COMBINED_FIELDNAMES))
            print(f"Wrote {len(rows)} combined rows to {combined_output}")

    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="Export Kalshi fills / settlements / combined CSV.")
    p.add_argument("--repo", type=Path, default=_REPO, help="Repo root (default: parent of control-panel/)")
    p.add_argument("--env", default=".env", help="Env file relative to repo")
    p.add_argument("--days", type=int, default=30, help="Lookback days (default 30)")
    p.add_argument(
        "--output",
        "-o",
        type=Path,
        default=None,
        help=(
            "Fills CSV path when writing fills. With ``--combined`` alone (no ``--also-split``), "
            "``-o`` sets the **combined** CSV path instead."
        ),
    )
    p.add_argument(
        "--with-settlements",
        action="store_true",
        help="Also write settlements CSV (separate file).",
    )
    p.add_argument(
        "--settlements-output",
        type=Path,
        default=None,
        help="Settlements CSV path (default: *_settlements.csv next to fills, or ~/Desktop/kalshi_settlements_<utc>.csv)",
    )
    p.add_argument(
        "--combined",
        action="store_true",
        help="Write one chronological CSV: fills + settlements (see COMBINED_FIELDNAMES in script).",
    )
    p.add_argument(
        "--combined-output",
        type=Path,
        default=None,
        help="Combined CSV path (default: ~/Desktop/kalshi_combined_<utc>.csv)",
    )
    p.add_argument(
        "--also-split",
        action="store_true",
        help="With --combined, also write separate fills and settlements CSVs (same paths as --with-settlements).",
    )
    args = p.parse_args()
    repo = args.repo.resolve()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    fills_out: Optional[Path] = None
    settlements_out: Optional[Path] = None
    combined_out: Optional[Path] = None

    if args.combined:
        if args.combined_output:
            combined_out = args.combined_output.expanduser().resolve()
        elif args.output and not args.also_split:
            combined_out = args.output.expanduser().resolve()
        elif args.also_split and args.output:
            o = args.output.expanduser().resolve()
            combined_out = o.parent / f"{o.stem}_combined{o.suffix}"
        else:
            combined_out = Path.home() / "Desktop" / f"kalshi_combined_{stamp}.csv"
        if args.also_split:
            if args.output:
                fills_out = args.output.expanduser().resolve()
            else:
                fills_out = Path.home() / "Desktop" / f"kalshi_fills_{stamp}.csv"
            if args.settlements_output:
                settlements_out = args.settlements_output.expanduser().resolve()
            elif args.output:
                settlements_out = fills_out.parent / f"{fills_out.stem}_settlements{fills_out.suffix}"
            else:
                settlements_out = Path.home() / "Desktop" / f"kalshi_settlements_{stamp}.csv"
    elif args.with_settlements:
        fills_out = args.output.expanduser().resolve() if args.output else Path.home() / "Desktop" / f"kalshi_fills_{stamp}.csv"
        if args.settlements_output:
            settlements_out = args.settlements_output.expanduser().resolve()
        elif args.output:
            settlements_out = fills_out.parent / f"{fills_out.stem}_settlements{fills_out.suffix}"
        else:
            settlements_out = Path.home() / "Desktop" / f"kalshi_settlements_{stamp}.csv"
    else:
        fills_out = args.output.expanduser().resolve() if args.output else Path.home() / "Desktop" / f"kalshi_fills_{stamp}.csv"

    return export_portfolio(
        repo=repo,
        env_file=args.env,
        days=args.days,
        fills_output=fills_out,
        settlements_output=settlements_out,
        combined_output=combined_out,
    )


if __name__ == "__main__":
    raise SystemExit(main())
