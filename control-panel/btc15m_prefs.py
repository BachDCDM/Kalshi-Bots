"""Persist BTC 15m YES/NO contract sizes; bot reads same file on startup."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from zoneinfo import ZoneInfo

MIN_CONTRACTS = 1
MAX_CONTRACTS = 5000

_ET = ZoneInfo("America/New_York")


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


def validate_and_normalize_hour_groups(groups: Any) -> list[dict[str, Any]]:
    """
    Each group: ``name`` (optional), ``hours`` list of int 0–23 (ET, market open hour),
    ``contracts_yes``, ``contracts_no``. No hour may appear in more than one group.
    """
    if groups is None:
        return []
    if not isinstance(groups, list):
        raise ValueError("hour_groups must be a list")
    seen: set[int] = set()
    out: list[dict[str, Any]] = []
    for i, g in enumerate(groups):
        if not isinstance(g, dict):
            raise ValueError(f"hour_groups[{i}] must be an object")
        hours_raw = g.get("hours", [])
        if not isinstance(hours_raw, list) or not hours_raw:
            raise ValueError(f"hour_groups[{i}].hours must be a non-empty list")
        parsed: list[int] = []
        for x in hours_raw:
            h = int(x)
            if h < 0 or h > 23:
                raise ValueError(f"hour_groups[{i}]: hour must be 0–23, got {h}")
            parsed.append(h)
        hs = sorted(set(parsed))
        if not hs:
            raise ValueError(f"hour_groups[{i}].hours must contain at least one valid hour")
        for h in hs:
            if h in seen:
                raise ValueError(f"hour {h} ET cannot appear in more than one group")
            seen.add(h)
        cy = int(g["contracts_yes"])
        cn = int(g["contracts_no"])
        if not (MIN_CONTRACTS <= cy <= MAX_CONTRACTS):
            raise ValueError(f"hour_groups[{i}].contracts_yes must be {MIN_CONTRACTS}–{MAX_CONTRACTS}")
        if not (MIN_CONTRACTS <= cn <= MAX_CONTRACTS):
            raise ValueError(f"hour_groups[{i}].contracts_no must be {MIN_CONTRACTS}–{MAX_CONTRACTS}")
        raw_name = g.get("name")
        name = (str(raw_name).strip() if raw_name is not None else "") or f"group_{i + 1}"
        out.append(
            {
                "name": name,
                "hours": hs,
                "contracts_yes": cy,
                "contracts_no": cn,
            }
        )
    return out


def effective_contracts_for_market_open(
    open_t_utc: datetime,
    prefs: dict[str, Any],
    base_yes: int,
    base_no: int,
) -> tuple[int, int, Optional[str]]:
    """
    Pick YES/NO contract counts using ``hour_groups`` by **market open** time in America/New_York
    (hour-of-day 0–23). If no group matches, return base sizes and ``None`` label.
    """
    groups = prefs.get("hour_groups")
    if not isinstance(groups, list) or not groups:
        return base_yes, base_no, None
    try:
        ot = open_t_utc
        if ot.tzinfo is None:
            ot = ot.replace(tzinfo=timezone.utc)
        et_h = int(ot.astimezone(_ET).hour)
    except Exception:
        return base_yes, base_no, None

    for g in groups:
        if not isinstance(g, dict):
            continue
        hours = g.get("hours")
        if not isinstance(hours, list):
            continue
        hs: set[int] = set()
        for x in hours:
            try:
                hi = int(x)
                if 0 <= hi <= 23:
                    hs.add(hi)
            except (TypeError, ValueError):
                continue
        if et_h not in hs:
            continue
        try:
            cy = int(g.get("contracts_yes", base_yes))
            cn = int(g.get("contracts_no", base_no))
        except (TypeError, ValueError):
            continue
        cy = max(MIN_CONTRACTS, min(MAX_CONTRACTS, cy))
        cn = max(MIN_CONTRACTS, min(MAX_CONTRACTS, cn))
        label = str(g.get("name") or "").strip() or "hour_group"
        return cy, cn, label
    return base_yes, base_no, None


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


def save_btc15m_prefs(
    repo: Path,
    *,
    contracts_yes: int,
    contracts_no: int,
    hour_groups: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    """Write base sizes and optional hour groups (replaces ``hour_groups`` when provided)."""
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
    if hour_groups is not None:
        cur["hour_groups"] = validate_and_normalize_hour_groups(hour_groups)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(cur, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)
    return cur


def save_contracts(repo: Path, n: int) -> int:
    """Set both sides to the same size (backward compatible)."""
    y, _ = save_contracts_pair(repo, int(n), int(n))
    return y
