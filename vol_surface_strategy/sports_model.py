"""Map league + ladder text to vol-surface distribution (Normal / Poisson / NegBinom)."""

from __future__ import annotations

import re
from typing import Any, Literal, Optional, Tuple

from vol_surface_strategy.surface_math import ModelKind

SportCode = Literal["MLB", "NBA", "NHL", "MLS", "NFL"]


def _blob_for_markets(markets: list[Any]) -> str:
    parts: list[str] = []
    for m in markets[:8]:
        for a in ("title", "subtitle", "series_ticker", "yes_sub_title"):
            parts.append(str(getattr(m, a, "") or ""))
    return " ".join(parts).lower()


def infer_sports_distribution(
    sport: SportCode,
    markets: list[Any],
    *,
    nhl_nb_r: float = 8.0,
    mls_nb_r: float = 6.0,
) -> Tuple[ModelKind, float]:
    """
    Return ``(model, nbinom_r_disp)`` for LOO / fair-value math.

    ``nbinom_r_disp`` is used only when ``model == "negbinom"`` (shape / dispersion).
    Heuristics follow product copy; refine with Kalshi ``series_ticker`` rules as needed.
    """
    b = _blob_for_markets(markets)
    st = str(getattr(markets[0], "series_ticker", "") or "").lower() if markets else ""

    if sport == "NHL":
        if "spread" in b or "margin" in b or "wins by" in b or "puck line" in b:
            return "normal", nhl_nb_r
        if "team" in b and ("goal" in b or "score" in b) and "total" in b:
            return "poisson", nhl_nb_r
        if "player" in b and ("goal" in b or "assist" in b or "point" in b):
            return "poisson", nhl_nb_r
        if "total" in b and ("goal" in b or "game" in b):
            return "negbinom", nhl_nb_r
        return "normal", nhl_nb_r

    if sport == "MLS":
        if "spread" in b or "margin" in b:
            return "normal", mls_nb_r
        if "team" in b and ("goal" in b or "score" in b):
            return "poisson", mls_nb_r
        if "total" in b and ("goal" in b or "match" in b):
            return "negbinom", mls_nb_r
        return "normal", mls_nb_r

    # MLB / NBA / default: Normal ladders (totals, spreads, team totals, most props).
    _ = st
    return "normal", nhl_nb_r


def parse_sport_code(raw: str) -> Optional[SportCode]:
    u = (raw or "").strip().upper()
    if u in ("MLB", "NBA", "NHL", "MLS", "NFL"):
        return u  # type: ignore[return-value]
    return None


def sport_from_market(m: Any) -> Optional[SportCode]:
    """
    Infer pro league from the same text Kalshi exposes on markets (ticker, event, series, titles).

    Mirrors the spirit of weather discovery (broad blob match), with exclusions for temp/macro
    lines that are not sports games.
    """
    parts = [
        getattr(m, "ticker", None),
        getattr(m, "event_ticker", None),
        getattr(m, "series_ticker", None),
        getattr(m, "title", None),
        getattr(m, "subtitle", None),
        getattr(m, "yes_sub_title", None),
        getattr(m, "category", None),
    ]
    b = " ".join(str(p or "") for p in parts).upper()

    if re.search(r"\bNCAA(N|F)?\b", b) or "COLLEGE FOOTBALL" in b or "COLLEGE BASKETBALL" in b:
        return None

    weatherish = any(
        x in b
        for x in (
            "KXHIGH",
            "KXLOW",
            "HIGHTEMP",
            "LOWTEMP",
            "KXTEMP",
            "DAILY HIGH",
            "DAILY LOW",
        )
    )
    sports_hint = any(
        x in b
        for x in ("NBA", "NHL", "MLB", "MLS", "WNBA", "GAME", " VS.", " VS ", "HOSTS", "WINNER")
    )
    if weatherish and not sports_hint:
        return None

    compact = re.sub(r"\s+", "", b)
    if re.search(r"\bNFL\b", b) or "NFLGAME" in b or re.match(r"^KX(NFL|NFC|AFC)", compact):
        return "NFL"
    if re.search(r"\b(NBA|WNBA)\b", b) or "NBAGAME" in b:
        return "NBA"
    if re.search(r"\bNHL\b", b) or "NHLGAME" in b:
        return "NHL"
    if re.search(r"\bMLB\b", b) or "MLBGAME" in b or "KXMLB" in b:
        return "MLB"
    if re.search(r"\bMLS\b", b) or "MLSGAME" in b or "KXMLS" in b:
        return "MLS"
    return None


def sport_from_series_ticker(series_ticker: str) -> Optional[SportCode]:
    """Best-effort league from Kalshi ``series_ticker`` (multivariate game series, props, etc.)."""
    s = (series_ticker or "").upper()
    if re.match(r"^KX(NFL|NFC|AFC)", s) or ("NFL" in s and "GAME" in s):
        return "NFL"
    # ``KXMLB`` does not contain substring ``MLB`` — include common Kalshi prefixes.
    if any(x in s for x in ("NBA", "WNBA", "NBAGAME", "KXNB")):
        return "NBA"
    if any(x in s for x in ("NHL", "NHLGAME", "KXNHL", "HOCKEY")):
        return "NHL"
    if any(x in s for x in ("MLB", "MLBGAME", "KXMLB")):
        return "MLB"
    if any(x in s for x in ("MLS", "MLSGAME", "KXMLS")):
        return "MLS"
    return None
