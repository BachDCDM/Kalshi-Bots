"""
Local control dashboard: systemd start/stop/restart + journal + SQLite reads.
Bind to 127.0.0.1 only. Access remotely: ssh -L 8080:127.0.0.1:8080 user@vps
"""

from __future__ import annotations

import asyncio
import json
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
from fastapi import Body, FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse

from btc15m_prefs import (
    MAX_CONTRACTS,
    MIN_CONTRACTS,
    contracts_from_prefs,
    prefs_path,
    save_contracts,
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

_ROOT = Path(__file__).resolve().parent
_REPO = Path(
    os.environ.get("KALSHI_TRADING_ROOT", str(_ROOT.parent))
).resolve()

_STRATEGIES_PATH = Path(
    os.environ.get("STRATEGIES_CONFIG", str(_ROOT / "strategies.yaml"))
)

_STATIC_INDEX = _ROOT / "static" / "index.html"

_UNIT_SAFE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.-]*\.service$")


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
        cur = conn.execute(
            f'SELECT * FROM "{table}" ORDER BY rowid DESC LIMIT ?',
            (limit,),
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


def _balance_env_rel() -> str:
    return (os.environ.get("CONTROL_PANEL_BALANCE_ENV") or ".env").strip() or ".env"


def _strategy_kalshi_env_rel(s: dict[str, Any]) -> str:
    return (s.get("kalshi_env_file") or ".env").strip() or ".env"


def _strategy_trading_mode(s: dict[str, Any]) -> tuple[str, str]:
    vals = load_strategy_env_map(_REPO, _strategy_kalshi_env_rel(s))
    host = kalshi_host_from_values(vals)
    return trading_mode_labels(host)


def _strategy_pnl(s: dict[str, Any]) -> tuple[int, str]:
    sq = _sqlite_block(s)
    if not sq or not sq.get("path"):
        return 0, ""
    try:
        db_path = _resolve_db_path(str(sq["path"]))
    except HTTPException:
        return 0, ""
    tables = set(sq.get("tables") or [])
    if "btc_sessions" in tables:
        return pnl_btc_approx_cents(db_path), "approx_logged_mids"
    if "trade_outcomes" in tables:
        return pnl_vol_surface_cents(db_path), "vol_surface_settlements"
    if "trades" in tables:
        return pnl_weather_cents(db_path), "realized_trades"
    return 0, ""


async def _balance_heartbeat_loop() -> None:
    envf = _balance_env_rel()
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL_SEC)
        await asyncio.to_thread(refresh_balance_cache, _REPO, envf)


@asynccontextmanager
async def _lifespan(app: FastAPI):
    envf = _balance_env_rel()
    await asyncio.to_thread(refresh_balance_cache, _REPO, envf)
    task = asyncio.create_task(_balance_heartbeat_loop())
    try:
        yield
    finally:
        task.cancel()
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
        pnl_cents, pnl_src = _strategy_pnl(s)
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
            }
        )
    return JSONResponse(out)


@app.get("/api/overview")
def overview() -> dict[str, Any]:
    cache = get_balance_cache()
    rows: list[dict[str, Any]] = []
    total_pnl = 0
    for s in _load_strategies():
        sid = s.get("id")
        if not sid:
            continue
        mode_key, mode_label = _strategy_trading_mode(s)
        pnl, pnl_src = _strategy_pnl(s)
        total_pnl += pnl
        rows.append(
            {
                "id": sid,
                "title": s.get("title", sid),
                "trading_mode": mode_key,
                "trading_mode_label": mode_label,
                "pnl_cents": pnl,
                "pnl_source": pnl_src,
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


@app.get("/api/strategies/{sid}/btc-contracts")
def btc_contracts_get(sid: str) -> dict[str, Any]:
    if sid != "btc15m":
        raise HTTPException(status_code=400, detail="Only btc15m supports contract prefs")
    repo = _REPO.resolve()
    vals = load_strategy_env_map(repo, ".env")
    raw = (vals.get("KALSHI_CONTRACTS") or "").strip()
    try:
        env_contracts = int(raw) if raw else 30
    except ValueError:
        env_contracts = 30
    file_contracts = contracts_from_prefs(repo)
    effective = file_contracts if file_contracts is not None else env_contracts
    return {
        "contracts": effective,
        "from_panel_file": file_contracts is not None,
        "env_kalshi_contracts": env_contracts,
        "min": MIN_CONTRACTS,
        "max": MAX_CONTRACTS,
        "prefs_path": str(prefs_path(repo)),
    }


@app.post("/api/strategies/{sid}/btc-contracts")
def btc_contracts_post(sid: str, body: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    if sid != "btc15m":
        raise HTTPException(status_code=400, detail="Only btc15m supports contract prefs")
    c = body.get("contracts")
    if c is None:
        raise HTTPException(status_code=400, detail="Missing contracts")
    try:
        save_contracts(_REPO.resolve(), int(c))
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
def strategy_table(sid: str, table: str, limit: int = 100) -> dict[str, Any]:
    s = _strategy_by_id(sid)
    sq = _sqlite_block(s)
    if not sq or not sq.get("path"):
        raise HTTPException(status_code=400, detail="No SQLite for this strategy")
    rel = str(sq["path"])
    db_path = _resolve_db_path(rel)
    allowed = set(sq.get("tables") or [])
    if table not in allowed:
        raise HTTPException(status_code=404, detail="Table not configured")
    lim = min(max(limit, 1), 500)
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
    return build_dashboard_payload(repo)


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
