"""Persist BTC 15m YES/NO contract sizes; bot reads same file on startup."""

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


def contracts_pair_from_prefs(repo: Path) -> tuple[Optional[int], Optional[int]]:
    """Return (yes, no) from panel file, or (None, None) to fall back to .env."""
    d = load_prefs(repo)
    if "contracts_yes" in d and "contracts_no" in d:
        try:
            y, n = int(d["contracts_yes"]), int(d["contracts_no"])
            if MIN_CONTRACTS <= y <= MAX_CONTRACTS and MIN_CONTRACTS <= n <= MAX_CONTRACTS:
                return y, n
        except (TypeError, ValueError):
            pass
    c = d.get("contracts")
    if c is not None:
        try:
            n = int(c)
            if MIN_CONTRACTS <= n <= MAX_CONTRACTS:
                return n, n
        except (TypeError, ValueError):
            pass
    return None, None


def contracts_from_prefs(repo: Path) -> Optional[int]:
    """Legacy: single int when panel only had ``contracts`` (same size both sides)."""
    y, n = contracts_pair_from_prefs(repo)
    if y is None:
        return None
    if y == n:
        return y
    return None


def save_contracts_pair(repo: Path, contracts_yes: int, contracts_no: int) -> tuple[int, int]:
    contracts_yes = int(contracts_yes)
    contracts_no = int(contracts_no)
    if not (MIN_CONTRACTS <= contracts_yes <= MAX_CONTRACTS):
        raise ValueError(f"contracts_yes must be {MIN_CONTRACTS}–{MAX_CONTRACTS}")
    if not (MIN_CONTRACTS <= contracts_no <= MAX_CONTRACTS):
        raise ValueError(f"contracts_no must be {MIN_CONTRACTS}–{MAX_CONTRACTS}")
    path = prefs_path(repo)
    path.parent.mkdir(parents=True, exist_ok=True)
    cur = load_prefs(repo)
    cur["contracts_yes"] = contracts_yes
    cur["contracts_no"] = contracts_no
    cur.pop("contracts", None)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(cur, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)
    return contracts_yes, contracts_no


def save_contracts(repo: Path, n: int) -> int:
    """Set both sides to the same size (backward compatible)."""
    y, _ = save_contracts_pair(repo, int(n), int(n))
    return y
