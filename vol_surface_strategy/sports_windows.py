"""Pre-game scan windows: minutes before scheduled start (US/Eastern) until T−5."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from zoneinfo import ZoneInfo

from vol_surface_strategy.sports_model import SportCode

ET = ZoneInfo("America/New_York")

# Kalshi ``occurrence_datetime`` is expected resolution (~ game end). Back off by league when
# ``rules_primary`` has no parseable wall-clock start.
_GAME_END_OFFSET_HOURS: dict[SportCode, float] = {
    "MLB": 3.5,
    "NBA": 2.5,
    "NHL": 2.5,
    "MLS": 2.0,
    "NFL": 3.0,
}

_MONTH_ABBREV = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}

# Kalshi copy: "originally scheduled for Apr 17, 2026 at 9:40 PM EDT"
_RULES_SCHEDULED_RE = re.compile(
    r"(?:originally\s+)?scheduled\s+for\s+"
    r"(?P<mon>[A-Za-z]{3})\s+"
    r"(?P<day>\d{1,2}),\s*"
    r"(?P<year>\d{4})\s+at\s+"
    r"(?P<h>\d{1,2}):(?P<mi>\d{2})\s*"
    r"(?P<ap>AM|PM)\s+"
    r"(?P<tz>[A-Z][A-Za-z]*(?:\s+Time)?)",
    re.IGNORECASE,
)


def _zone_for_kalshi_tz_label(label: str) -> ZoneInfo:
    raw = label.replace("Time", "").strip().upper()
    if raw in ("EDT", "EST", "ET", "EASTERN"):
        return ET
    if raw in ("CDT", "CST", "CT", "CENTRAL"):
        return ZoneInfo("America/Chicago")
    if raw in ("MDT", "MST", "MT", "MOUNTAIN"):
        return ZoneInfo("America/Denver")
    if raw in ("PDT", "PST", "PT", "PACIFIC"):
        return ZoneInfo("America/Los_Angeles")
    if raw in ("UTC", "GMT"):
        return ZoneInfo("UTC")
    return ET


def _wall_hour_24(h12: int, ap: str) -> int:
    ap_u = ap.strip().upper()
    if ap_u == "AM":
        return 0 if h12 == 12 else h12
    if ap_u == "PM":
        return 12 if h12 == 12 else h12 + 12
    return h12


def _parse_scheduled_start_from_rules(text: str) -> Optional[datetime]:
    if not text or not isinstance(text, str):
        return None
    m = _RULES_SCHEDULED_RE.search(text)
    if not m:
        return None
    mon_tok = m.group("mon").lower()[:3]
    month = _MONTH_ABBREV.get(mon_tok)
    if month is None:
        return None
    try:
        day = int(m.group("day"))
        year = int(m.group("year"))
        h12 = int(m.group("h"))
        minute = int(m.group("mi"))
    except (TypeError, ValueError):
        return None
    h24 = _wall_hour_24(h12, m.group("ap"))
    z = _zone_for_kalshi_tz_label(m.group("tz"))
    try:
        return datetime(year, month, day, h24, minute, tzinfo=z)
    except ValueError:
        return None


def _coerce_iso_utc(val: Any) -> Optional[datetime]:
    if val is None or isinstance(val, bool):
        return None
    if isinstance(val, (int, float)):
        try:
            ts = float(val)
            if ts > 1e12:
                ts /= 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc).replace(microsecond=0)
        except (OSError, ValueError, OverflowError):
            return None
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        if s.isdigit():
            try:
                ts = int(s)
                if ts > 1e12:
                    ts //= 1000
                return datetime.fromtimestamp(ts, tz=timezone.utc).replace(microsecond=0)
            except (OSError, ValueError, OverflowError):
                return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            cdt = datetime.fromisoformat(s)
        except ValueError:
            return None
        if cdt.tzinfo is None:
            cdt = cdt.replace(tzinfo=timezone.utc)
        return cdt.astimezone(timezone.utc).replace(microsecond=0)
    if isinstance(val, datetime):
        cdt = val
        if cdt.tzinfo is None:
            cdt = cdt.replace(tzinfo=timezone.utc)
        return cdt.astimezone(timezone.utc).replace(microsecond=0)
    return None


def parse_sports_game_start_utc(m: Any, sport: SportCode) -> Optional[datetime]:
    """
    Estimated **first pitch / tip / puck drop** in UTC.

    1. Parse ``rules_primary`` / ``rules_secondary`` for Kalshi's
       ``scheduled for Mon DD, YYYY at H:MM AM/PM TZ`` line.
    2. Else ``occurrence_datetime`` minus a league-typical game length (resolution proxy ≈ end).
    3. Does **not** use ``expiration_time`` / ``close_time`` — those are administrative buffers,
       not game start.
    """
    for attr in ("rules_primary", "rules_secondary"):
        raw = getattr(m, attr, None)
        if not raw:
            continue
        dt_et = _parse_scheduled_start_from_rules(str(raw))
        if dt_et is not None:
            return dt_et.astimezone(timezone.utc)

    occ = _coerce_iso_utc(getattr(m, "occurrence_datetime", None))
    if occ is not None:
        off = timedelta(hours=float(_GAME_END_OFFSET_HOURS.get(sport, 3.0)))
        return occ - off
    return None


def earliest_sports_game_start_utc(markets: list[Any], sport: SportCode) -> Optional[datetime]:
    """Earliest per-market game start among ``markets`` (UTC)."""
    best: Optional[datetime] = None
    for m in markets:
        u = parse_sports_game_start_utc(m, sport)
        if u is None:
            continue
        if best is None or u < best:
            best = u
    return best


# Pre-game trading only: window opens 3h before listed start, ends 10m before start (no in-game orders).
SPORTS_WINDOW_START_BEFORE_MINUTES = 180.0
SPORTS_WINDOW_END_BEFORE_MINUTES = 10.0


@dataclass(frozen=True)
class SportsScanWindow:
    """Legacy shape kept for imports; bounds are overridden by module constants above."""

    start_minutes_before: int
    hard_stop_minutes_before: int


_DEFAULTS: dict[SportCode, SportsScanWindow] = {
    "MLB": SportsScanWindow(int(SPORTS_WINDOW_START_BEFORE_MINUTES), int(SPORTS_WINDOW_END_BEFORE_MINUTES)),
    "NBA": SportsScanWindow(int(SPORTS_WINDOW_START_BEFORE_MINUTES), int(SPORTS_WINDOW_END_BEFORE_MINUTES)),
    "NHL": SportsScanWindow(int(SPORTS_WINDOW_START_BEFORE_MINUTES), int(SPORTS_WINDOW_END_BEFORE_MINUTES)),
    "MLS": SportsScanWindow(int(SPORTS_WINDOW_START_BEFORE_MINUTES), int(SPORTS_WINDOW_END_BEFORE_MINUTES)),
    "NFL": SportsScanWindow(int(SPORTS_WINDOW_START_BEFORE_MINUTES), int(SPORTS_WINDOW_END_BEFORE_MINUTES)),
}


def scan_window_for_sport(sport: SportCode) -> SportsScanWindow:
    return _DEFAULTS[sport]


def sports_trading_window_open_et(game_start_et: datetime) -> tuple[datetime, datetime]:
    """Return ``(window_open_et, window_close_et)`` inclusive-open, exclusive-close style for checks."""
    gs = game_start_et.astimezone(ET)
    open_et = gs - timedelta(minutes=SPORTS_WINDOW_START_BEFORE_MINUTES)
    close_et = gs - timedelta(minutes=SPORTS_WINDOW_END_BEFORE_MINUTES)
    return open_et, close_et


def minutes_until_sports_trading_window_opens(now_et: datetime, game_start_et: datetime) -> float:
    """Minutes until ``window_open`` (T−3h). Negative if the window already opened."""
    open_et, _ = sports_trading_window_open_et(game_start_et)
    loc = now_et.astimezone(ET)
    return (open_et - loc).total_seconds() / 60.0


def game_start_et_from_markets(
    markets: list[object],
    *,
    sport: SportCode,
    fallback_utc: Optional[datetime] = None,
) -> Optional[datetime]:
    """
    Best-effort **scheduled game start** (US/Eastern) from Kalshi market rows.

    Uses ``rules_primary`` wall time when present, else ``occurrence_datetime`` minus a league
    duration offset. Ignores ``expiration_time`` / ``close_time`` for start estimation.
    """
    best = earliest_sports_game_start_utc(list(markets), sport)
    if best is not None:
        return best.astimezone(ET)
    if fallback_utc is not None:
        fu = fallback_utc if fallback_utc.tzinfo else fallback_utc.replace(tzinfo=ET)
        return fu.astimezone(ET)
    return None


def in_pre_game_order_window(
    sport: SportCode,
    now_et: datetime,
    game_start_et: datetime,
) -> tuple[bool, float]:
    """
    Returns (allowed, minutes_until_game_start).

    **Pre-game only:** allowed when now is in ``(T − 3h, T − 10m]`` wall time (US/Eastern
    comparison), i.e. ``SPORTS_WINDOW_END_BEFORE_MINUTES < minutes_to_start <=
    SPORTS_WINDOW_START_BEFORE_MINUTES``. No orders at or after T−10m (game underway or
    imminently so). ``sport`` is kept for API compatibility; bounds are uniform across leagues.
    """
    _ = sport
    loc = now_et.astimezone(ET)
    gs = game_start_et.astimezone(ET)
    mins_to_start = (gs - loc).total_seconds() / 60.0
    if mins_to_start <= SPORTS_WINDOW_END_BEFORE_MINUTES:
        return False, mins_to_start
    if mins_to_start > SPORTS_WINDOW_START_BEFORE_MINUTES:
        return False, mins_to_start
    return True, mins_to_start


def describe_sports_trading_window(
    sport: SportCode,
    now_utc: datetime,
    game_start_et: Optional[datetime],
) -> dict[str, Any]:
    """Panel / dashboard hints for a sports ladder (pre-game window only)."""
    _ = sport
    now_et = now_utc.astimezone(ET)
    if game_start_et is None:
        return {
            "kind": "sports_pre_game",
            "in_trading_window": False,
            "monitor_tick": False,
            "minutes_to_game_start": None,
            "minutes_until_window_opens": None,
            "minutes_until_window_closes": None,
            "window_hint": "No parsable game start (rules / occurrence)",
            "next_window_hint": "",
        }
    gs = game_start_et.astimezone(ET)
    open_et, close_et = sports_trading_window_open_et(gs)
    allowed, mins_to_start = in_pre_game_order_window(sport, now_et, gs)
    mins_until_open = (open_et - now_et).total_seconds() / 60.0
    mins_until_close = (close_et - now_et).total_seconds() / 60.0
    return {
        "kind": "sports_pre_game",
        "in_trading_window": allowed,
        "monitor_tick": allowed,
        "minutes_to_game_start": round(mins_to_start, 1),
        "minutes_until_window_opens": max(0.0, round(mins_until_open, 1)),
        "minutes_until_window_closes": round(mins_until_close, 1),
        "window_hint": (
            f"Pre-game: trade only after T−{int(SPORTS_WINDOW_START_BEFORE_MINUTES)}m ET "
            f"and before T−{int(SPORTS_WINDOW_END_BEFORE_MINUTES)}m (bot polls every 5m in-window)"
        ),
        "next_window_hint": (
            "Window open — 5m poll" if allowed else (f"Window opens in {max(0.0, mins_until_open):.0f} min" if mins_until_open > 0 else "Window closed (too close to start or live)")
        ),
    }


def any_sports_trading_window_open(
    now_utc: datetime,
    games: list[tuple[str, SportCode, list[Any]]],
    *,
    start_overrides: Optional[dict[str, str]] = None,
) -> bool:
    """True if any discovered event is inside the pre-game trading window (T−3h … T−10m)."""
    so = start_overrides or {}
    now_et = now_utc.astimezone(ET)
    for et, sport, mk in games:
        if not mk:
            continue
        gs_et: Optional[datetime] = None
        if et in so:
            try:
                dt = datetime.fromisoformat(so[et].replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=ET)
                gs_et = dt.astimezone(ET)
            except ValueError:
                pass
        if gs_et is None:
            gs_et = game_start_et_from_markets(mk, sport=sport, fallback_utc=None)
        if gs_et is None:
            continue
        ok, _ = in_pre_game_order_window(sport, now_et, gs_et)
        if ok:
            return True
    return False


def order_expiration_ts(game_start_et: datetime) -> int:
    """Unix seconds: shortly after scheduled start (same spirit as BTC :45 cap)."""
    gs = game_start_et.astimezone(ET)
    cap = gs + timedelta(minutes=15)
    return int(cap.timestamp())
