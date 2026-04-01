"""
Local control dashboard: systemd start/stop/restart + journal + SQLite reads.
Bind to 127.0.0.1 only. Access remotely: ssh -L 8080:127.0.0.1:8080 user@vps
"""

from __future__ import annotations

import os
import re
import sqlite3
import subprocess
from pathlib import Path
from typing import Any, Optional

import yaml
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse

_ROOT = Path(__file__).resolve().parent
_REPO = Path(
    os.environ.get("KALSHI_TRADING_ROOT", str(_ROOT.parent))
).resolve()

_STRATEGIES_PATH = Path(
    os.environ.get("STRATEGIES_CONFIG", str(_ROOT / "strategies.yaml"))
)

_UNIT_SAFE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.-]*\.service$")


def _load_strategies() -> list[dict[str, Any]]:
    if not _STRATEGIES_PATH.is_file():
        return []
    with open(_STRATEGIES_PATH, encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return list(raw.get("strategies") or [])


def _strategy_by_id(sid: str) -> dict[str, Any]:
    for s in _load_strategies():
        if s.get("id") == sid:
            return s
    raise HTTPException(status_code=404, detail="Unknown strategy")


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
    code, out, _ = _systemctl_user("show", unit, "-p", "ActiveState", "-p", "SubState", "-p", "MainPID", "--no-pager")
    info: dict[str, Any] = {"unit": unit, "raw": out.strip()}
    for line in out.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            info[k.strip()] = v.strip()
    active = info.get("ActiveState", "unknown")
    info["summary"] = active
    return info


def _sqlite_rows(db_path: Path, table: str, limit: int) -> tuple[list[str], list[tuple]]:
    forbidden = {";", " ", "\n", "\t", "\r"}
    if any(c in table for c in forbidden) or not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", table):
        raise HTTPException(status_code=400, detail="Invalid table name")
    if not db_path.is_file():
        return [], []
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=5)
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


app = FastAPI(title="Kalshi strategy control", version="1.0")


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
        out.append(
            {
                "id": s.get("id"),
                "title": s.get("title", s.get("id")),
                "unit": unit,
                "status": st.get("summary", "unknown"),
                "detail": st,
                "data_type": data.get("type"),
                "sqlite_tables": (
                    list(data.get("tables") or [])
                    if data.get("type") == "sqlite"
                    else None
                ),
            }
        )
    return JSONResponse(out)


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


@app.get("/api/strategies/{sid}/logs")
def strategy_logs(sid: str, lines: int = 200) -> dict[str, str]:
    s = _strategy_by_id(sid)
    data = s.get("data") or {}
    if data.get("type") != "journal":
        raise HTTPException(status_code=400, detail="No journal for this strategy")
    unit = _validate_unit(s["systemd_unit"])
    n = min(max(lines, 1), 2000)
    text = _journal_user(unit, n)
    return {"unit": unit, "log": text}


@app.get("/api/strategies/{sid}/tables/{table}")
def strategy_table(sid: str, table: str, limit: int = 100) -> dict[str, Any]:
    s = _strategy_by_id(sid)
    data = s.get("data") or {}
    if data.get("type") != "sqlite":
        raise HTTPException(status_code=400, detail="No SQLite for this strategy")
    rel = data.get("path", "")
    repo = _REPO.resolve()
    db_path = (repo / rel).resolve()
    try:
        db_path.relative_to(repo)
    except ValueError:
        raise HTTPException(status_code=400, detail="DB path outside repo")
    allowed = set(data.get("tables") or [])
    if table not in allowed:
        raise HTTPException(status_code=404, detail="Table not configured")
    lim = min(max(limit, 1), 500)
    cols, rows = _sqlite_rows(db_path, table, lim)
    return {"columns": cols, "rows": [list(r) for r in rows]}


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Kalshi strategies</title>
  <style>
    :root { --bg:#0f1419; --card:#1a2332; --text:#e7ecf3; --muted:#8b9cb3; --accent:#3d8bfd; --ok:#3ecf8e; --bad:#f85149; }
    body { font-family: ui-sans-serif, system-ui, sans-serif; background: var(--bg); color: var(--text); margin: 0; padding: 1.25rem; line-height: 1.5; }
    h1 { font-size: 1.25rem; font-weight: 600; margin: 0 0 1rem; }
    .grid { display: grid; gap: 1rem; max-width: 72rem; }
    .card { background: var(--card); border-radius: 10px; padding: 1rem 1.1rem; }
    .row { display: flex; flex-wrap: wrap; align-items: center; gap: 0.5rem 0.75rem; margin-top: 0.75rem; }
    button { background: #2d3a4f; color: var(--text); border: none; padding: 0.45rem 0.85rem; border-radius: 6px; cursor: pointer; font-size: 0.875rem; }
    button:hover { background: #3d4d66; }
    button.primary { background: var(--accent); color: #fff; }
    .badge { font-size: 0.75rem; padding: 0.2rem 0.5rem; border-radius: 4px; background: #2d3a4f; }
    .badge.active { background: #1f4d3a; color: var(--ok); }
    .badge.inactive { background: #4d1f1f; color: var(--bad); }
    pre, .table-wrap { background: #0b0f14; border-radius: 8px; padding: 0.75rem; overflow: auto; font-size: 0.78rem; max-height: 22rem; margin: 0.5rem 0 0; white-space: pre-wrap; word-break: break-word; }
    table { border-collapse: collapse; width: 100%; font-size: 0.78rem; }
    th, td { border-bottom: 1px solid #2d3a4f; padding: 0.35rem 0.5rem; text-align: left; vertical-align: top; }
    th { color: var(--muted); font-weight: 500; }
    .tabs { display: flex; gap: 0.35rem; flex-wrap: wrap; margin-top: 0.5rem; }
    .tabs button { opacity: 0.85; }
    .tabs button.on { opacity: 1; outline: 1px solid var(--accent); }
    .hint { color: var(--muted); font-size: 0.8rem; margin-top: 1rem; }
  </style>
</head>
<body>
  <h1>Kalshi strategies</h1>
  <div class="grid" id="root"></div>
  <p class="hint">API: <code>/api/strategies</code> · Reload page to refresh status. Bind server to 127.0.0.1; use SSH tunnel from your laptop.</p>
<script>
async function api(path, opts) {
  const r = await fetch(path, opts);
  if (!r.ok) throw new Error(await r.text());
  const t = r.headers.get('content-type') || '';
  return t.includes('json') ? r.json() : r.text();
}
function badge(st) {
  const on = (st || '').toLowerCase() === 'active';
  return '<span class="badge ' + (on ? 'active">running' : 'inactive">stopped') + '</span>';
}
function stratCard(s) {
  const id = s.id;
  const actions = '<div class="row">' +
    '<button class="primary" data-a="start" data-id="'+id+'">Start</button>' +
    '<button data-a="stop" data-id="'+id+'">Stop</button>' +
    '<button data-a="restart" data-id="'+id+'">Restart</button>' +
    '</div>';
  let body = '';
  if (s.sqlite_tables && s.sqlite_tables.length) {
    body = '<div class="tabs" id="tabs-'+id+'"></div><div id="data-'+id+'"></div>';
  } else if (s.data_type === 'journal') {
    body = '<pre id="log-'+id+'">Loading log…</pre>';
  } else {
    body = '<p class="hint" style="margin:0.5rem 0 0">No data source configured.</p>';
  }
  return '<div class="card" data-sid="'+id+'"><strong>'+ (s.title||id) +'</strong> ' + badge(s.status) +
    '<div style="color:#8b9cb3;font-size:0.8rem">'+ (s.unit||'') +'</div>' + actions + body + '</div>';
}
async function loadLog(id) {
  const el = document.getElementById('log-'+id);
  if (!el) return;
  try {
    const j = await api('/api/strategies/'+id+'/logs?lines=300');
    el.textContent = j.log || '(empty)';
  } catch (e) { el.textContent = String(e); }
}
async function loadTable(id, table) {
  const el = document.getElementById('data-'+id);
  if (!el) return;
  try {
    const j = await api('/api/strategies/'+id+'/tables/'+table+'?limit=150');
    if (!j.columns || !j.columns.length) { el.innerHTML = '<pre>(no rows or DB missing)</pre>'; return; }
    let h = '<div class="table-wrap"><table><tr>' + j.columns.map(c=>'<th>'+c+'</th>').join('') + '</tr>';
    for (const row of j.rows) {
      h += '<tr>' + row.map(c=>'<td>'+ (c===null?'':String(c)) +'</td>').join('') + '</tr>';
    }
    h += '</table></div>';
    el.innerHTML = h;
  } catch (e) { el.innerHTML = '<pre>'+String(e)+'</pre>'; }
}
function sqliteTabs(id, tables) {
  const tabEl = document.getElementById('tabs-'+id);
  if (!tabEl) return;
  tabEl.innerHTML = tables.map((t,i) =>
    '<button class="'+(i===0?'on':'')+'" data-t="'+t+'">'+t+'</button>').join('');
  tabEl.querySelectorAll('button').forEach(btn => {
    btn.onclick = () => {
      tabEl.querySelectorAll('button').forEach(b => b.classList.remove('on'));
      btn.classList.add('on');
      loadTable(id, btn.dataset.t);
    };
  });
  loadTable(id, tables[0]);
}
async function refresh() {
  const list = await api('/api/strategies');
  const root = document.getElementById('root');
  root.innerHTML = list.map(stratCard).join('');
  for (const s of list) {
    if (s.sqlite_tables && s.sqlite_tables.length) sqliteTabs(s.id, s.sqlite_tables);
    else if (s.data_type === 'journal') loadLog(s.id);
  }
  root.querySelectorAll('button[data-a]').forEach(btn => {
    btn.onclick = async () => {
      const id = btn.dataset.id, a = btn.dataset.a;
      try {
        await api('/api/strategies/'+id+'/'+a, { method: 'POST' });
        const list2 = await api('/api/strategies');
        const cur = list2.find(x => x.id === id);
        const card = root.querySelector('[data-sid="'+id+'"]');
        if (card) {
          const b = card.querySelector('.badge');
          if (b && cur) { b.className = 'badge ' + (cur.status==='active'?'active':'inactive');
            b.textContent = cur.status==='active'?'running':'stopped';
          }
        }
        const cur2 = list2.find(x => x.id === id);
        if (cur2 && cur2.data_type === 'journal') loadLog(id);
      } catch (e) { alert(e); }
    };
  });
}
refresh();
</script>
</body>
</html>"""
