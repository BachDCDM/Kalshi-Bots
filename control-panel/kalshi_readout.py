"""Kalshi balance fetch + env-based paper/live + SQLite P&L for the control panel."""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Optional

import certifi
from dotenv import dotenv_values
from kalshi_python_sync import Configuration, KalshiClient

_DEFAULT_KALSHI_HOST = "https://api.elections.kalshi.com/trade-api/v2"


def _env_path(repo: Path, rel: str) -> Path:
    p = (repo / rel).resolve()
    try:
        p.relative_to(repo.resolve())
    except ValueError:
        raise ValueError("env path outside repo")
    return p


def kalshi_host_from_values(vals: dict[str, Optional[str]]) -> str:
    h = (vals.get("KALSHI_HOST") or "").strip()
    return h or _DEFAULT_KALSHI_HOST


def is_paper_host(host: str) -> bool:
    return "demo-api" in host.lower() or "demo" in host.lower()


def trading_mode_labels(host: str) -> tuple[str, str]:
    if is_paper_host(host):
        return "paper", "Paper"
    return "live", "Live"


def _read_pem(repo: Path, vals: dict[str, Optional[str]]) -> Optional[str]:
    raw = (vals.get("KALSHI_PRIVATE_KEY") or "").strip()
    if raw:
        pem = raw.replace("\\n", "\n")
        if "BEGIN" in pem and "PRIVATE KEY" in pem:
            return pem
    path_str = (vals.get("KALSHI_PRIVATE_KEY_PATH") or "").strip()
    candidates: list[Path] = []
    if path_str:
        p = Path(path_str).expanduser()
        candidates.append(p if p.is_absolute() else repo / p)
    candidates.append(repo / "kalshi.pem")
    for path in candidates:
        if path.is_file():
            text = path.read_text(encoding="utf-8")
            if "BEGIN" in text and "PRIVATE KEY" in text:
                return text
    return None


def load_strategy_env_map(repo: Path, kalshi_env_file: str) -> dict[str, Optional[str]]:
    path = _env_path(repo, kalshi_env_file)
    if not path.is_file():
        return {}
    return dict(dotenv_values(path))


def make_kalshi_client(repo: Path, vals: dict[str, Optional[str]]) -> Optional[KalshiClient]:
    key_id = (vals.get("KALSHI_API_KEY_ID") or "").strip()
    pem = _read_pem(repo, vals)
    if not key_id or not pem:
        return None
    host = kalshi_host_from_values(vals)
    cfg = Configuration(host=host, ssl_ca_cert=certifi.where())
    cfg.api_key_id = key_id
    cfg.private_key_pem = pem
    return KalshiClient(cfg)


def fetch_balance(client: KalshiClient) -> dict[str, Any]:
    r = client.get_balance()
    bal = getattr(r, "balance", None)
    pv = getattr(r, "portfolio_value", None)
    ts = getattr(r, "updated_ts", None)
    return {
        "balance_cents": int(bal) if bal is not None else None,
        "portfolio_value_cents": int(pv) if pv is not None else None,
        "updated_ts": int(ts) if ts is not None else None,
    }


def pnl_vol_surface_cents(db_path: Path) -> int:
    """Sum realized P&amp;L from vol surface ``trade_outcomes`` (settled rows)."""
    if not db_path.is_file():
        return 0
    try:
        conn = sqlite3.connect(db_path.as_uri() + "?mode=ro", uri=True, timeout=30)
    except sqlite3.Error:
        conn = sqlite3.connect(str(db_path), timeout=30)
    try:
        cur = conn.execute(
            """
            SELECT COALESCE(SUM(pnl_cents), 0) FROM trade_outcomes
            WHERE status = 'resolved' AND pnl_cents IS NOT NULL
            """
        )
        return int(cur.fetchone()[0] or 0)
    except sqlite3.Error:
        return 0
    finally:
        conn.close()


def pnl_weather_cents(db_path: Path) -> int:
    if not db_path.is_file():
        return 0
    p = db_path.resolve()
    try:
        conn = sqlite3.connect(p.as_uri() + "?mode=ro", uri=True, timeout=30)
    except sqlite3.Error:
        conn = sqlite3.connect(str(p), timeout=30)
    try:
        cur = conn.execute(
            "SELECT COALESCE(SUM(pnl_cents), 0) FROM trades WHERE pnl_cents IS NOT NULL"
        )
        row = cur.fetchone()
        return int(row[0] or 0)
    finally:
        conn.close()


def pnl_btc_approx_cents(db_path: Path) -> int:
    """
    Realized P&amp;L in cents: prefer stored ``realized_pnl_cents`` per session (authoritative).
    Otherwise estimate with (exit−entry) × min(entry_leg_fills, exit_fill_count) so fills
    cannot inflate the multiplier.
    """
    if not db_path.is_file():
        return 0
    p = db_path.resolve()
    try:
        conn = sqlite3.connect(p.as_uri() + "?mode=ro", uri=True, timeout=30)
    except sqlite3.Error:
        conn = sqlite3.connect(str(p), timeout=30)
    try:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='btc_sessions'"
        )
        if not cur.fetchone():
            return 0
        cur = conn.execute("PRAGMA table_info(btc_sessions)")
        cols = {row[1] for row in cur.fetchall()}
        has_rpc = "realized_pnl_cents" in cols
        has_ef = "exit_fill_count" in cols

        cur = conn.execute(
            f"""
            SELECT
                {"realized_pnl_cents" if has_rpc else "NULL"},
                entry_cents, exit_cents,
                yes_entry_fills, no_entry_fills,
                {"exit_fill_count" if has_ef else "NULL"},
                exit_handled
            FROM btc_sessions
            WHERE COALESCE(exit_handled, 0) = 1
            """
        )
        total = 0
        for row in cur.fetchall():
            rpc, ec, xc, yf, nf, ef, _eh = row
            if ec is None or xc is None:
                continue
            ec_i, xc_i = int(ec), int(xc)
            if has_rpc and rpc is not None:
                total += int(rpc)
                continue
            yf = float(yf or 0)
            nf = float(nf or 0)
            ef = float(ef or 0)
            if yf > 0 and nf <= 0:
                n = min(yf, ef if ef > 0 else yf)
                total += int(round((xc_i - ec_i) * n))
            elif nf > 0 and yf <= 0:
                n = min(nf, ef if ef > 0 else nf)
                total += int(round((xc_i - ec_i) * n))
        return int(total)
    finally:
        conn.close()


_balance_lock = threading.Lock()
_balance_state: dict[str, Any] = {
    "balance_cents": None,
    "portfolio_value_cents": None,
    "updated_ts": None,
    "fetched_at": None,
    "error": None,
}


def get_balance_cache() -> dict[str, Any]:
    with _balance_lock:
        return dict(_balance_state)


def refresh_balance_cache(repo: Path, env_file: str) -> None:
    vals = load_strategy_env_map(repo, env_file)
    err: Optional[str] = None
    payload: dict[str, Any] = {
        "balance_cents": None,
        "portfolio_value_cents": None,
        "updated_ts": None,
        "fetched_at": time.time(),
        "error": None,
    }
    try:
        client = make_kalshi_client(repo, vals)
        if not client:
            err = "missing KALSHI_API_KEY_ID or private key in env"
        else:
            b = fetch_balance(client)
            payload["balance_cents"] = b["balance_cents"]
            payload["portfolio_value_cents"] = b["portfolio_value_cents"]
            payload["updated_ts"] = b["updated_ts"]
    except Exception as e:
        err = str(e)
    payload["error"] = err
    with _balance_lock:
        _balance_state.clear()
        _balance_state.update(payload)
