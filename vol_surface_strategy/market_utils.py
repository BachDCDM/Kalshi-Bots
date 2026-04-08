"""Strike extraction and contract snapshots from Kalshi market objects."""

from __future__ import annotations

import re
from typing import Any, Optional

from vol_surface_strategy.surface_math import BucketMode, ContractInput


def _fc(x: Any) -> float:
    try:
        return float(x or 0)
    except (TypeError, ValueError):
        return 0.0


def _dollars_to_cents(s: Any) -> Optional[int]:
    if s is None or s == "":
        return None
    try:
        return int(round(float(s) * 100))
    except (TypeError, ValueError):
        return None


def extract_strike_btc(m: Any) -> Optional[float]:
    for attr in ("floor_strike", "cap_strike", "functional_strike"):
        v = getattr(m, attr, None)
        if v is not None and v != "":
            try:
                return float(v)
            except (TypeError, ValueError):
                pass
    t = str(getattr(m, "ticker", "") or "")
    nums = re.findall(r"\d{5,}", t)
    if nums:
        return float(nums[-1])
    return None


def parse_weather_bucket_text(text: str) -> tuple[Optional[float], Optional[float], BucketMode]:
    """Parse Kalshi-style bucket labels into (low, high, mode)."""
    if not text:
        return None, None, "unknown"
    t = text.lower().replace("℉", "f").replace("°", "")
    if "or below" in t or "or lower" in t:
        m = re.search(r"(\d+)", t)
        if m:
            hi = float(m.group(1))
            return None, hi, "below"
        return None, None, "unknown"
    if "or above" in t or "or higher" in t:
        m = re.search(r"(\d+)", t)
        if m:
            lo = float(m.group(1))
            return lo, None, "above"
        return None, None, "unknown"
    m = re.search(r"(\d+)\s*f?\s*(?:to|-|–)\s*(\d+)\s*f?", t)
    if m:
        return float(m.group(1)), float(m.group(2)), "range"
    return None, None, "unknown"


def weather_sort_strike(low: Optional[float], high: Optional[float], mode: BucketMode) -> float:
    """Single key for ordering buckets left-to-right on the thermometer."""
    if mode == "below" and high is not None:
        return high - 0.001
    if mode == "above" and low is not None:
        return low + 0.001
    if mode == "range" and low is not None:
        return low
    return 0.0


def extract_strike_weather(m: Any) -> Optional[float]:
    fl_raw = getattr(m, "floor_strike", None)
    cap_raw = getattr(m, "cap_strike", None)
    if fl_raw is not None and str(fl_raw) != "":
        try:
            fl = float(fl_raw)
            try:
                capf = float(cap_raw) if cap_raw not in (None, "") else None
            except (TypeError, ValueError):
                capf = None
            if capf is not None and capf > fl:
                return (fl + capf) / 2
            return fl
        except (TypeError, ValueError):
            pass
    fn = getattr(m, "functional_strike", None)
    if fn is not None and str(fn) != "":
        try:
            return float(fn)
        except (TypeError, ValueError):
            pass
    # subtitle "72°F or above" etc.
    sub = str(getattr(m, "subtitle", "") or getattr(m, "yes_sub_title", "") or "")
    m1 = re.search(r"(\d+)\s*°?\s*F", sub, re.I)
    if m1:
        return float(m1.group(1))
    return None


def _yes_no_book_cents(m: Any) -> tuple[Optional[float], Optional[float], bool]:
    """
    YES bid/ask in cents. When the API omits YES bid but lists NO ask, use YES bid = 100 − NO ask;
    same for YES ask from NO bid. If YES bid is still missing but YES ask exists, flag one-sided
    (mid = YES ask / 2 in :func:`contract_from_market`).
    """
    yb = _dollars_to_cents(getattr(m, "yes_bid_dollars", None))
    ya = _dollars_to_cents(getattr(m, "yes_ask_dollars", None))
    no_ask = _dollars_to_cents(getattr(m, "no_ask_dollars", None))
    no_bid = _dollars_to_cents(getattr(m, "no_bid_dollars", None))
    if yb is None and no_ask is not None:
        yb = 100.0 - float(no_ask)
    if ya is None and no_bid is not None:
        ya = 100.0 - float(no_bid)
    if ya is None:
        return None, None, False
    if yb is None:
        return 0.0, float(ya), True
    return float(yb), float(ya), False


def contract_from_market(m: Any, *, kind: str) -> Optional[ContractInput]:
    yb, ya, one_sided = _yes_no_book_cents(m)
    if ya is None:
        return None
    if one_sided:
        mid = ya / 2.0
    else:
        mid = (yb + ya) / 2.0  # type: ignore[operator]
    vol = _fc(getattr(m, "volume_fp", 0))
    ybsz = _fc(getattr(m, "yes_bid_size_fp", 0))
    yasz = _fc(getattr(m, "yes_ask_size_fp", 0))
    bl: Optional[float] = None
    bh: Optional[float] = None
    bm: BucketMode = "unknown"
    if kind == "btc":
        k = extract_strike_btc(m)
    else:
        # Prefer subtitle lines: full title often contains a year (e.g. 2026) that breaks digit regexes.
        sub = str(getattr(m, "subtitle", "") or "").strip()
        yss = str(getattr(m, "yes_sub_title", "") or "").strip()
        title = str(getattr(m, "title", "") or "").strip()
        blob_prio = " ".join(x for x in (sub, yss) if x)
        blob_full = " ".join(x for x in (title, sub, yss) if x)
        bl, bh, bm = parse_weather_bucket_text(blob_prio)
        if bm == "unknown":
            bl, bh, bm = parse_weather_bucket_text(blob_full)
        k = extract_strike_weather(m)
        if k is None and bm != "unknown":
            if bm == "range" and bl is not None and bh is not None:
                k = (bl + bh) / 2.0
            elif bh is not None:
                k = float(bh)
            elif bl is not None:
                k = float(bl)
    if k is None:
        return None
    sk = weather_sort_strike(bl, bh, bm) if kind == "weather" else k
    tick = str(getattr(m, "ticker", "") or "")
    return ContractInput(
        ticker=tick,
        strike=sk,
        mid_cents=mid,
        yes_bid_cents=float(yb),
        yes_ask_cents=float(ya),
        volume_fp=vol,
        yes_bid_size_fp=ybsz,
        yes_ask_size_fp=yasz,
        bucket_low=bl,
        bucket_high=bh,
        bucket_mode=bm,
        one_sided=one_sided,
    )
