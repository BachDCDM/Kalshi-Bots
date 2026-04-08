"""Daily log files under vol_surface_data/logs/."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = _ROOT / "vol_surface_data" / "logs"


def setup_logging(name: str = "vol_surface") -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = LOG_DIR / f"{day}_vol_surface.log"
    log = logging.getLogger(name)
    log.setLevel(logging.INFO)
    log.handlers.clear()
    fh = logging.FileHandler(path, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    log.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    log.addHandler(ch)
    return log


def prune_old_logs(days: int = 90) -> None:
    if not LOG_DIR.is_dir():
        return
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    for p in LOG_DIR.glob("*_vol_surface.log"):
        try:
            daypart = p.name.replace("_vol_surface.log", "")
            d = datetime.strptime(daypart, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if d < cutoff:
                p.unlink(missing_ok=True)
        except (ValueError, OSError):
            pass
