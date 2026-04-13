"""
Local control dashboard: systemd start/stop/restart + journal + SQLite reads.
Bind to 127.0.0.1 only. Access remotely: ssh -L 8080:127.0.0.1:8080 user@vps
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
import sqlite3
import subprocess
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import yaml
from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse

from settlement_sync import (
    get_ledger_pnl_by_strategy,
    ledger_row_counts,
    sync_settlements_once,
    vol_surface_ledger_pnl_breakdown,
)
from btc15m_prefs import (
    MAX_CONTRACTS,
    MIN_CONTRACTS,
    contracts_pair_from_prefs,
    prefs_path,
    save_contracts,
    save_contracts_pair,
)
from kalshi_readout import (
    get_balance_cache,
    kalshi_host_from_values,
    load_strategy_env_map,
    pnl_btc_approx_cents,
    pnl_vol_surface_cents,
    pnl_weather_cents,
    refresh_balance_cache,
    trading_mode_labels,
)

_LOG = logging.getLogger("kalshi.control_panel")

_ROOT = Path(__file__).resolve().parent
_REPO = Path(
    os.environ.get("KALSHI_TRADING_ROOT", str(_ROOT.parent))
).resolve()

_STRATEGIES_PATH = Path(
    os.environ.get("STRATEGIES_CONFIG", str(_ROOT / "strategies.yaml"))
)

_STATIC_INDEX = _ROOT / "static" / "index.html"

_UNIT_SAFE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.-]*\.service$")

# SQLite table viewer: UI requests are capped (large tables still use CSV for unlimited export).
_SQLITE_UI_MAX_ROWS = 100_000
_SQLITE_FULL_EXPORT_MAX_ROWS = 10_000_000


def _strategies_yaml() -> dict[str, Any]:
    if not _STRATEGIES_PATH.is_file():
        return {}
    with open(_STRATEGIES_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _load_strategies() -> list[dict[str, Any]]:
    return list(_strategies_yaml().get("strategies") or [])


def _control_panel_unit() -> str:
    raw = _strategies_yaml()
    u = raw.get("control_panel_unit") or os.environ.get(
        "CONTROL_PANEL_SYSTEMD_UNIT", "kalshi-control-panel.service"
    )
    return str(u)


def _deploy_prefs_path() -> Path:
    return _ROOT / "data" / "deploy_prefs.json"


def _default_deploy_prefs() -> dict[str, Any]:
    ids = [s["id"] for s in _load_strategies() if s.get("id")]
    return {
        "restart_on_pull": {i: False for i in ids},
    }


def _load_deploy_prefs() -> dict[str, Any]:
    base = _default_deploy_prefs()
    path = _deploy_prefs_path()
    if not path.is_file():
        return base
    try:
        with open(path, encoding="utf-8") as f:
            disk = json.load(f)
    except (json.JSONDecodeError, OSError):
        return base
    rop = disk.get("restart_on_pull")
    if isinstance(rop, dict):
        valid = {s["id"] for s in _load_strategies() if s.get("id")}
        for k, v in rop.items():
            if k in valid:
                base["restart_on_pull"][k] = bool(v)
    return base


def _save_deploy_prefs(prefs: dict[str, Any]) -> None:
    path = _deploy_prefs_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(prefs, f, indent=2)
    tmp.replace(path)


def _schedule_panel_restart(unit: str) -> None:
    try:
        u = _validate_unit(unit)
    except HTTPException:
        return
    subprocess.Popen(
        ["bash", "-c", f"sleep 2 && systemctl --user restart {u}"],
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _strategy_by_id(sid: str) -> dict[str, Any]:
    for s in _load_strategies():
        if s.get("id") == sid:
            return s
    raise HTTPException(status_code=404, detail="Unknown strategy")


def _sqlite_block(s: dict[str, Any]) -> Optional[dict[str, Any]]:
    data = s.get("data") or {}
    typ = data.get("type")
    if typ == "sqlite":
        return {"path": data.get("path"), "tables": data.get("tables")}
    if typ == "mixed":
        inner = data.get("sqlite") or {}
        return {"path": inner.get("path"), "tables": inner.get("tables")}
    return None


def _has_journal(s: dict[str, Any]) -> bool:
    data = s.get("data") or {}
    return data.get("type") in ("journal", "mixed")


def _journal_line_cap(s: dict[str, Any]) -> int:
    data = s.get("data") or {}
    if data.get("type") == "journal":
        return int(data.get("lines") or 300)
    if data.get("type") == "mixed":
        return int(data.get("journal_lines") or 300)
    return 300


def _validate_unit(unit: str) -> str:
    if not unit or not _UNIT_SAFE.match(unit):
        raise HTTPException(status_code=400, detail="Invalid unit name")
    return unit


def _run_cmd(argv: list[str], timeout: float = 30) -> tuple[int, str, str]:
    try:
        p = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        return p.returncode, p.stdout or "", p.stderr or ""
    except subprocess.TimeoutExpired:
        return 124, "", "timeout"


def _systemctl_user(*args: str) -> tuple[int, str, str]:
    return _run_cmd(["systemctl", "--user", *args])


def _journal_user(unit: str, lines: int) -> str:
    unit = _validate_unit(unit)
    code, out, err = _run_cmd(
        [
            "journalctl",
            "--user",
            "-u",
            unit,
            "-n",
            str(lines),
            "--no-pager",
            "-o",
            "short-iso",
        ],
        timeout=15,
    )
    if code != 0:
        return f"(journalctl exit {code})\n{err}\n{out}"
    return out


def _unit_status(unit: str) -> dict[str, Any]:
    unit = _validate_unit(unit)
    code, out, _ = _systemctl_user(
        "show", unit, "-p", "ActiveState", "-p", "SubState", "-p", "MainPID", "--no-pager"
    )
    info: dict[str, Any] = {"unit": unit, "raw": out.strip()}
    for line in out.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            info[k.strip()] = v.strip()
    active = info.get("ActiveState", "unknown")
    info["summary"] = active
    return info


def _sqlite_connect_read(db_path: Path) -> sqlite3.Connection:
    """Open DB for reads. Prefer read-only URI; fall back to path (handles locks / quirky builds)."""
    path = db_path.resolve()
    uri = path.as_uri() + "?mode=ro"
    try:
        return sqlite3.connect(uri, uri=True, timeout=30)
    except sqlite3.Error:
        return sqlite3.connect(str(path), timeout=30)


def _sqlite_rows(db_path: Path, table: str, limit: int) -> tuple[list[str], list[tuple]]:
    forbidden = {";", " ", "\n", "\t", "\r"}
    if any(c in table for c in forbidden) or not re.match(
        r"^[a-zA-Z_][a-zA-Z0-9_]*$", table
    ):
        raise HTTPException(status_code=400, detail="Invalid table name")
    if not db_path.is_file():
        return [], []
    conn = _sqlite_connect_read(db_path)
    try:
        cur = conn.execute(f'PRAGMA table_info("{table}")')
        cols = [row[1] for row in cur.fetchall()]
        if not cols:
            return [], []
        lim = min(max(limit, 1), _SQLITE_FULL_EXPORT_MAX_ROWS)
        cur = conn.execute(
            f'SELECT * FROM "{table}" ORDER BY rowid DESC LIMIT ?',
            (lim,),
        )
        rows = cur.fetchall()
        return cols, rows
    finally:
        conn.close()


def _resolve_db_path(rel: str) -> Path:
    repo = _REPO.resolve()
    db_path = (repo / rel).resolve()
    try:
        db_path.relative_to(repo)
    except ValueError:
        raise HTTPException(status_code=400, detail="DB path outside repo")
    return db_path


HEARTBEAT_INTERVAL_SEC = 60
SETTLEMENT_SYNC_INTERVAL_SEC = int(
    os.environ.get("CONTROL_PANEL_SETTLEMENT_SYNC_SEC", "900")
)


def _balance_env_rel() -> str:
    return (os.environ.get("CONTROL_PANEL_BALANCE_ENV") or ".env").strip() or ".env"


def _strategy_kalshi_env_rel(s: dict[str, Any]) -> str:
    return (s.get("kalshi_env_file") or ".env").strip() or ".env"


def _strategy_trading_mode(s: dict[str, Any]) -> tuple[str, str]:
    vals = load_strategy_env_map(_REPO, _strategy_kalshi_env_rel(s))
    host = kalshi_host_from_values(vals)
    return trading_mode_labels(host)


def _strategy_pnl(
    s: dict[str, Any],
    *,
    ledger_pnl: Optional[dict[str, int]] = None,
    ledger_counts: Optional[dict[str, int]] = None,
) -> tuple[int, str]:
    sid = str(s.get("id") or "")
    sq = _sqlite_block(s)
    db_path: Optional[Path] = None
    tables: set[str] = set()
    if sq and sq.get("path"):
        try:
            db_path = _resolve_db_path(str(sq["path"]))
            tables = set(sq.get("tables") or [])
        except HTTPException:
            db_path = None

    # Prefer Kalshi settlement ledger when we have rows for this strategy (correct dual-sided
    # payout math). Bot ``trade_outcomes`` is session accounting and can diverge; use it only
    # when the ledger has not synced any settlements for this id yet.
    if (
        sid
        and ledger_counts is not None
        and ledger_pnl is not None
        and ledger_counts.get(sid, 0) > 0
    ):
        return int(ledger_pnl.get(sid, 0)), "kalshi_settlements_ledger"

    if not db_path:
        return 0, ""

    if "trade_outcomes" in tables:
        cents = pnl_vol_surface_cents(db_path)
        src = "trade_outcomes" if sid == "btc15m" else "vol_surface_settlements"
        return cents, src
    if "btc_sessions" in tables:
        return pnl_btc_approx_cents(db_path), "approx_logged_mids"
    if "trades" in tables:
        return pnl_weather_cents(db_path), "realized_trades"
    return 0, ""


async def _balance_heartbeat_loop() -> None:
    envf = _balance_env_rel()
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL_SEC)
        await asyncio.to_thread(refresh_balance_cache, _REPO, envf)


def _settlement_sync_job() -> None:
    try:
        sync_settlements_once(_REPO.resolve(), _load_strategies(), env_file=_balance_env_rel())
    except Exception:
        _LOG.exception("settlement sync failed")


def _settlement_full_sync_on_start_enabled() -> bool:
    v = os.environ.get("CONTROL_PANEL_SETTLEMENT_FULL_SYNC_ON_START", "1").strip().lower()
    return v not in ("0", "false", "no")


def _settlement_sync_startup_job() -> None:
    """Re-pull settlements so ledger rows match current payout rules (bounded sync skips old rows)."""
    try:
        full = _settlement_full_sync_on_start_enabled()
        r = sync_settlements_once(
            _REPO.resolve(),
            _load_strategies(),
            env_file=_balance_env_rel(),
            full_history=full,
        )
        if not r.get("ok"):
            _LOG.warning("startup settlement sync failed: %s", r.get("error"))
        else:
            _LOG.info(
                "startup settlement sync ok (full_history=%s) settlements_processed=%s min_ts=%s",
                full,
                r.get("settlements_processed"),
                r.get("min_ts_used"),
            )
    except Exception:
        _LOG.exception("startup settlement sync failed")


async def _settlement_sync_loop() -> None:
    while True:
        await asyncio.sleep(max(120, SETTLEMENT_SYNC_INTERVAL_SEC))
        await asyncio.to_thread(_settlement_sync_job)


@asynccontextmanager
async def _lifespan(app: FastAPI):
    envf = _balance_env_rel()
    await asyncio.to_thread(refresh_balance_cache, _REPO, envf)
    # Full settlement pull once before serving so existing ledger rows get corrected net_pnl.
    await asyncio.to_thread(_settlement_sync_startup_job)
    bal_task = asyncio.create_task(_balance_heartbeat_loop())
    st_task = asyncio.create_task(_settlement_sync_loop())
    try:
        yield
    finally:
        bal_task.cancel()
        st_task.cancel()
        for task in (bal_task, st_task):
            try:
                await task
            except asyncio.CancelledError:
                pass


app = FastAPI(
    title="Kalshi strategy control",
    version="1.0",
    lifespan=_lifespan,
)


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok", "repo": str(_REPO)}


@app.get("/api/strategies")
def list_strategies() -> JSONResponse:
    repo = _REPO.resolve()
    ledger_pnl = get_ledger_pnl_by_strategy(repo)
    ledger_n = ledger_row_counts(repo)
    out = []
    for s in _load_strategies():
        unit = s.get("systemd_unit", "")
        try:
            st = _unit_status(unit) if unit else {"summary": "no unit"}
        except HTTPException:
            st = {"summary": "error"}
        data = s.get("data") or {}
        sq = _sqlite_block(s)
        tables = list(sq["tables"]) if sq and sq.get("tables") else None
        mode_key, mode_label = _strategy_trading_mode(s)
        pnl_cents, pnl_src = _strategy_pnl(
            s, ledger_pnl=ledger_pnl, ledger_counts=ledger_n
        )
        out.append(
            {
                "id": s.get("id"),
                "title": s.get("title", s.get("id")),
                "unit": unit,
                "status": st.get("summary", "unknown"),
                "detail": st,
                "data_type": data.get("type"),
                "has_journal": _has_journal(s),
                "journal_lines": _journal_line_cap(s) if _has_journal(s) else None,
                "sqlite_tables": tables,
                "trading_mode": mode_key,
                "trading_mode_label": mode_label,
                "pnl_cents": pnl_cents,
                "pnl_source": pnl_src,
                "settlement_ledger_rows": ledger_n.get(str(s.get("id") or ""), 0),
            }
        )
    return JSONResponse(out)


@app.get("/api/overview")
def overview() -> dict[str, Any]:
    cache = get_balance_cache()
    repo = _REPO.resolve()
    ledger_pnl = get_ledger_pnl_by_strategy(repo)
    ledger_n = ledger_row_counts(repo)
    rows: list[dict[str, Any]] = []
    total_pnl = 0
    for s in _load_strategies():
        sid = s.get("id")
        if not sid:
            continue
        mode_key, mode_label = _strategy_trading_mode(s)
        pnl, pnl_src = _strategy_pnl(
            s, ledger_pnl=ledger_pnl, ledger_counts=ledger_n
        )
        total_pnl += pnl
        rows.append(
            {
                "id": sid,
                "title": s.get("title", sid),
                "trading_mode": mode_key,
                "trading_mode_label": mode_label,
                "pnl_cents": pnl,
                "pnl_source": pnl_src,
                "settlement_ledger_rows": ledger_n.get(str(sid), 0),
            }
        )
    bal_vals = load_strategy_env_map(_REPO, _balance_env_rel())
    acc_host = kalshi_host_from_values(bal_vals)
    acc_mode_key, acc_mode_label = trading_mode_labels(acc_host)
    return {
        "heartbeat_interval_sec": HEARTBEAT_INTERVAL_SEC,
        "balance_cents": cache.get("balance_cents"),
        "portfolio_value_cents": cache.get("portfolio_value_cents"),
        "balance_updated_ts": cache.get("updated_ts"),
        "balance_fetched_at": cache.get("fetched_at"),
        "balance_error": cache.get("error"),
        "account_trading_mode": acc_mode_key,
        "account_trading_mode_label": acc_mode_label,
        "strategies": rows,
        "total_pnl_cents": total_pnl,
    }


@app.post("/api/sync-settlements")
def trigger_settlement_sync(
    full: bool = Query(
        False,
        description="If true, re-fetch all settlements (omit min_ts). Slow; fixes stale ledger rows.",
    ),
) -> dict[str, Any]:
    """Pull Kalshi settlements into the local ledger (same job as the periodic sync)."""
    return sync_settlements_once(
        _REPO.resolve(),
        _load_strategies(),
        env_file=_balance_env_rel(),
        full_history=full,
    )


@app.post("/api/strategies/{sid}/start")
def start_strategy(sid: str) -> dict[str, str]:
    s = _strategy_by_id(sid)
    unit = _validate_unit(s["systemd_unit"])
    code, out, err = _systemctl_user("start", unit)
    if code != 0:
        raise HTTPException(status_code=500, detail=err or out or f"exit {code}")
    return {"ok": "true", "unit": unit}


@app.post("/api/strategies/{sid}/stop")
def stop_strategy(sid: str) -> dict[str, str]:
    s = _strategy_by_id(sid)
    unit = _validate_unit(s["systemd_unit"])
    code, out, err = _systemctl_user("stop", unit)
    if code != 0:
        raise HTTPException(status_code=500, detail=err or out or f"exit {code}")
    return {"ok": "true", "unit": unit}


@app.post("/api/strategies/{sid}/restart")
def restart_strategy(sid: str) -> dict[str, str]:
    s = _strategy_by_id(sid)
    unit = _validate_unit(s["systemd_unit"])
    code, out, err = _systemctl_user("restart", unit)
    if code != 0:
        raise HTTPException(status_code=500, detail=err or out or f"exit {code}")
    return {"ok": "true", "unit": unit}


def _btc_env_contract_pair(vals: dict[str, Any]) -> tuple[int, int, int]:
    """Returns (base, yes, no) from .env: optional KALSHI_CONTRACTS_YES/NO override base KALSHI_CONTRACTS."""
    raw = (vals.get("KALSHI_CONTRACTS") or "").strip()
    try:
        base = int(raw) if raw else 30
    except ValueError:
        base = 30
    cy = (vals.get("KALSHI_CONTRACTS_YES") or "").strip()
    cn = (vals.get("KALSHI_CONTRACTS_NO") or "").strip()
    try:
        env_yes = int(cy) if cy else base
    except ValueError:
        env_yes = base
    try:
        env_no = int(cn) if cn else base
    except ValueError:
        env_no = base
    return base, env_yes, env_no


@app.get("/api/strategies/{sid}/btc-contracts")
def btc_contracts_get(sid: str) -> dict[str, Any]:
    if sid != "btc15m":
        raise HTTPException(status_code=400, detail="Only btc15m supports contract prefs")
    repo = _REPO.resolve()
    vals = load_strategy_env_map(repo, ".env")
    base, env_yes, env_no = _btc_env_contract_pair(vals)
    fy, fn = contracts_pair_from_prefs(repo)
    if fy is not None and fn is not None:
        eff_y, eff_n = fy, fn
        from_panel = True
    else:
        eff_y, eff_n = env_yes, env_no
        from_panel = False
    return {
        "contracts_yes": eff_y,
        "contracts_no": eff_n,
        "contracts": eff_y,
        "contracts_legacy": eff_y,
        "from_panel_file": from_panel,
        "env_kalshi_contracts": base,
        "env_contracts_yes": env_yes,
        "env_contracts_no": env_no,
        "min": MIN_CONTRACTS,
        "max": MAX_CONTRACTS,
        "prefs_path": str(prefs_path(repo)),
    }


@app.post("/api/strategies/{sid}/btc-contracts")
def btc_contracts_post(sid: str, body: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    if sid != "btc15m":
        raise HTTPException(status_code=400, detail="Only btc15m supports contract prefs")
    repo = _REPO.resolve()
    cur = btc_contracts_get(sid)
    ey = int(cur["contracts_yes"])
    en = int(cur["contracts_no"])
    if "contracts_yes" in body:
        ey = body["contracts_yes"]
    if "contracts_no" in body:
        en = body["contracts_no"]
    if "contracts" in body and "contracts_yes" not in body and "contracts_no" not in body:
        ey = en = body["contracts"]
    try:
        save_contracts_pair(repo, int(ey), int(en))
    except (TypeError, ValueError) as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return btc_contracts_get(sid)


@app.get("/api/strategies/{sid}/logs")
def strategy_logs(sid: str, lines: int = 200) -> dict[str, str]:
    s = _strategy_by_id(sid)
    if not _has_journal(s):
        raise HTTPException(status_code=400, detail="No journal for this strategy")
    unit = _validate_unit(s["systemd_unit"])
    n = min(max(lines, 1), 2000)
    text = _journal_user(unit, n)
    return {"unit": unit, "log": text}


@app.get("/api/strategies/{sid}/tables/{table}")
def strategy_table(
    sid: str,
    table: str,
    limit: int = 100,
    full: bool = Query(
        False,
        description="Return up to all rows for CSV export (ignores UI cap).",
    ),
) -> dict[str, Any]:
    s = _strategy_by_id(sid)
    sq = _sqlite_block(s)
    if not sq or not sq.get("path"):
        raise HTTPException(status_code=400, detail="No SQLite for this strategy")
    rel = str(sq["path"])
    db_path = _resolve_db_path(rel)
    allowed = set(sq.get("tables") or [])
    if table not in allowed:
        raise HTTPException(status_code=404, detail="Table not configured")
    if full:
        lim = _SQLITE_FULL_EXPORT_MAX_ROWS
    else:
        lim = min(max(limit, 1), _SQLITE_UI_MAX_ROWS)
    try:
        rel_display = str(db_path.resolve().relative_to(_REPO.resolve()))
    except ValueError:
        rel_display = rel
    meta = {
        "config_path": rel,
        "resolved_path": rel_display,
        "resolved_exists": db_path.is_file(),
        "repo_root": str(_REPO.resolve()),
    }
    try:
        cols, rows = _sqlite_rows(db_path, table, lim)
    except sqlite3.Error as e:
        return {
            "columns": [],
            "rows": [],
            "db": meta,
            "sqlite_error": str(e),
        }
    return {
        "columns": cols,
        "rows": [list(r) for r in rows],
        "db": meta,
    }


@app.post("/api/strategies/{sid}/tables/{table}/clear")
def clear_table(sid: str, table: str) -> dict[str, Any]:
    s = _strategy_by_id(sid)
    sq = _sqlite_block(s)
    if not sq or not sq.get("path"):
        raise HTTPException(status_code=400, detail="No SQLite for this strategy")
    db_path = _resolve_db_path(str(sq["path"]))
    allowed = set(sq.get("tables") or [])
    if table not in allowed:
        raise HTTPException(status_code=404, detail="Table not configured")
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", table):
        raise HTTPException(status_code=400, detail="Invalid table name")
    if not db_path.is_file():
        raise HTTPException(status_code=404, detail="DB file does not exist")
    conn = sqlite3.connect(str(db_path.resolve()), timeout=30)
    try:
        conn.execute(f'DELETE FROM "{table}"')
        conn.commit()
        remaining = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    finally:
        conn.close()
    return {"ok": True, "table": table, "remaining_rows": remaining}


@app.get("/api/debug/sqlite")
def debug_sqlite() -> dict[str, Any]:
    """Where the panel looks for DB files (same logic as table views)."""
    out: list[dict[str, Any]] = []
    for s in _load_strategies():
        sid = s.get("id")
        sq = _sqlite_block(s)
        if not sq or not sq.get("path"):
            continue
        rel = str(sq["path"])
        try:
            db_path = _resolve_db_path(rel)
        except HTTPException as e:
            out.append(
                {
                    "strategy_id": sid,
                    "config_path": rel,
                    "error": e.detail,
                }
            )
            continue
        entry: dict[str, Any] = {
            "strategy_id": sid,
            "config_path": rel,
            "absolute_path": str(db_path.resolve()),
            "exists": db_path.is_file(),
            "size_bytes": db_path.stat().st_size if db_path.is_file() else None,
            "tables_in_file": [],
        }
        if db_path.is_file():
            try:
                conn = _sqlite_connect_read(db_path)
                try:
                    cur = conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
                    )
                    entry["tables_in_file"] = [r[0] for r in cur.fetchall()]
                finally:
                    conn.close()
            except sqlite3.Error as e:
                entry["sqlite_error"] = str(e)
        out.append(entry)
    return {"repo_root": str(_REPO.resolve()), "strategies": out}


def _weather_snapshot_payload() -> dict[str, Any]:
    wb = _REPO / "weather-bot"
    if not wb.is_dir():
        raise HTTPException(status_code=404, detail="weather-bot not found in repo")
    path_insert = str(wb.resolve())
    if path_insert not in sys.path:
        sys.path.insert(0, path_insert)
    try:
        import panel_snapshot as weather_panel_snapshot
    except ImportError as e:
        return {
            "ok": False,
            "error": f"import panel_snapshot: {e}",
            "cities": [],
            "next_steps": [],
            "constants": {},
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        }
    return weather_panel_snapshot.build_snapshot()


@app.get("/api/strategies/{sid}/vol-surface-dashboard")
def vol_surface_dashboard(sid: str) -> dict[str, Any]:
    s = _strategy_by_id(sid)
    if s.get("id") != "vol_surface":
        raise HTTPException(
            status_code=400, detail="Only the vol_surface strategy supports this endpoint"
        )
    repo = _REPO.resolve()
    path_insert = str(repo)
    if path_insert not in sys.path:
        sys.path.insert(0, path_insert)
    try:
        from vol_surface_strategy.panel_snapshot import build_dashboard_payload
    except ImportError as e:
        return {"ok": False, "error": str(e)}
    payload = build_dashboard_payload(repo)
    ledger_br = vol_surface_ledger_pnl_breakdown(repo)
    if ledger_br.get("has_ledger"):
        payload["cumulative_pnl_cents"] = ledger_br["cumulative_pnl_cents"]
        payload["pnl_by_market_type"] = ledger_br["pnl_by_market_type"]
        payload["pnl_source"] = "kalshi_settlement_ledger"
    else:
        payload["pnl_source"] = "trade_outcomes"
    return payload


@app.get("/api/strategies/{sid}/weather-snapshot")
def weather_snapshot(sid: str) -> dict[str, Any]:
    s = _strategy_by_id(sid)
    if s.get("id") != "weather":
        raise HTTPException(
            status_code=400, detail="Only the weather strategy supports this endpoint"
        )
    return _weather_snapshot_payload()


@app.get("/api/strategies/{sid}/btc-hourly")
def btc_hourly_success(sid: str) -> dict[str, Any]:
    s = _strategy_by_id(sid)
    sq = _sqlite_block(s)
    if not sq or "btc_sessions" not in set(sq.get("tables") or []):
        raise HTTPException(status_code=400, detail="No btc_sessions data for this strategy")
    db_path = _resolve_db_path(str(sq["path"]))
    pct = [0.0] * 24
    cnt = [0] * 24
    if not db_path.is_file():
        return {"hours": list(range(24)), "pct": pct, "count": cnt}
    conn = _sqlite_connect_read(db_path)
    try:
        cur = conn.execute(
            """
            SELECT ended_hour_utc, SUM(success), COUNT(*)
            FROM btc_sessions
            WHERE COALESCE(yes_entry_fills, 0) + COALESCE(no_entry_fills, 0) > 0
            GROUP BY ended_hour_utc
            """
        )
        for h, succ_sum, n in cur.fetchall():
            hi = int(h)
            if 0 <= hi <= 23:
                ni = int(n)
                cnt[hi] = ni
                pct[hi] = round(100.0 * int(succ_sum) / ni, 1) if ni else 0.0
    finally:
        conn.close()
    return {"hours": list(range(24)), "pct": pct, "count": cnt}


@app.get("/api/deploy/prefs")
def deploy_prefs_get() -> dict[str, Any]:
    p = _load_deploy_prefs()
    strat_meta = [
        {"id": s["id"], "title": s.get("title", s["id"])}
        for s in _load_strategies()
        if s.get("id")
    ]
    return {
        "restart_on_pull": dict(p["restart_on_pull"]),
        "strategies": strat_meta,
        "control_panel_unit": _control_panel_unit(),
    }


@app.post("/api/deploy/prefs")
def deploy_prefs_post(body: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    cur = _load_deploy_prefs()
    if isinstance(body.get("restart_on_pull"), dict):
        valid = {s["id"] for s in _load_strategies() if s.get("id")}
        for k, v in body["restart_on_pull"].items():
            if k in valid:
                cur["restart_on_pull"][k] = bool(v)
    _save_deploy_prefs(cur)
    return deploy_prefs_get()


@app.post("/api/deploy/git-pull")
def deploy_git_pull() -> dict[str, Any]:
    code, out, err = _run_cmd(["git", "-C", str(_REPO), "pull"], timeout=120)
    git_out = (out or "").rstrip()
    if err:
        git_out = f"{git_out}\n{err}".strip() if git_out else err.strip()
    prefs = _load_deploy_prefs()
    restarted: list[str] = []
    skipped: list[str] = []
    for s in _load_strategies():
        sid = s.get("id")
        unit = s.get("systemd_unit")
        if not sid or not unit:
            continue
        if prefs["restart_on_pull"].get(sid):
            try:
                u = _validate_unit(str(unit))
            except HTTPException:
                skipped.append(f"{sid} (invalid unit)")
                continue
            c2, o2, e2 = _systemctl_user("restart", u)
            if c2 == 0:
                restarted.append(u)
            else:
                restarted.append(f"{u} (exit {c2}: {e2 or o2})")
        else:
            skipped.append(f"{sid} (restart on pull off)")

    panel_sched = False
    if code == 0:
        pu = _control_panel_unit()
        try:
            _validate_unit(pu)
        except HTTPException:
            pass
        else:
            _schedule_panel_restart(pu)
            panel_sched = True

    return {
        "ok": code == 0,
        "git_exit": code,
        "git_output": git_out.strip(),
        "restarted": restarted,
        "skipped": skipped,
        "panel_restart_scheduled": panel_sched,
    }


@app.get("/")
def index() -> FileResponse:
    if not _STATIC_INDEX.is_file():
        raise HTTPException(status_code=500, detail="Missing control-panel/static/index.html")
    return FileResponse(_STATIC_INDEX)
