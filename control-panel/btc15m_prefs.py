"""Persist BTC 15m contract count; bot reads same file on startup."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

MIN_CONTRACTS = 1
MAX_CONTRACTS = 5000


def prefs_path(repo: Path) -> Path:
    p = repo / "control-panel" / "data" / "btc15m_prefs.json"
    return p


def load_prefs(repo: Path) -> dict[str, Any]:
    path = prefs_path(repo)
    if not path.is_file():
        return {}
    try:
        return dict(json.loads(path.read_text(encoding="utf-8")))
    except (json.JSONDecodeError, OSError, TypeError):
        return {}


def save_contracts(repo: Path, n: int) -> int:
    n = int(n)
    if n < MIN_CONTRACTS or n > MAX_CONTRACTS:
        raise ValueError(f"contracts must be {MIN_CONTRACTS}–{MAX_CONTRACTS}")
    path = prefs_path(repo)
    path.parent.mkdir(parents=True, exist_ok=True)
    cur = load_prefs(repo)
    cur["contracts"] = n
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(cur, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)
    return n


def contracts_from_prefs(repo: Path) -> Optional[int]:
    d = load_prefs(repo)
    c = d.get("contracts")
    if c is None:
        return None
    try:
        n = int(c)
        if n < MIN_CONTRACTS or n > MAX_CONTRACTS:
            return None
        return n
    except (TypeError, ValueError):
        return None
