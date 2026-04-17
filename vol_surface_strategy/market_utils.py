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


def _mget(m: Any, name: str) -> Any:
    if isinstance(m, dict):
        return m.get(name)
    return getattr(m, name, None)


def normalize_top_of_book_cents(val: Any) -> Optional[int]:
    """
    Kalshi list endpoints may return prices as integer cents (1–99) or decimal dollars (0.01–0.99).
    Returns integer cents in [1, 99] or None.
    """
    if val is None or val == "":
        return None
    if isinstance(val, bool):
        return None
    try:
        x = float(val)
    except (TypeError, ValueError):
        return None
    if x <= 0:
        return None
    if x < 1.0:
        c = int(round(x * 100))
    else:
        c = int(round(x))
    if 1 <= c <= 99:
        return c
    return None


def get_yes_ask_cents(m: Any) -> Optional[int]:
    for field in ("yes_ask", "yes_ask_price", "best_yes_ask", "ask_yes", "ask"):
        c = normalize_top_of_book_cents(_mget(m, field))
        if c is not None:
            return c
    d = _dollars_to_cents(_mget(m, "yes_ask_dollars"))
    if d is not None and 1 <= d <= 99:
        return int(d)
    return None


def get_yes_bid_cents(m: Any) -> Optional[int]:
    for field in ("yes_bid", "yes_bid_price", "best_yes_bid", "bid_yes"):
        c = normalize_top_of_book_cents(_mget(m, field))
        if c is not None:
            return c
    d = _dollars_to_cents(_mget(m, "yes_bid_dollars"))
    if d is not None and 1 <= d <= 99:
        return int(d)
    return None


def get_no_ask_cents(m: Any) -> Optional[int]:
    for field in ("no_ask", "no_ask_price", "best_no_ask", "ask_no"):
        c = normalize_top_of_book_cents(_mget(m, field))
        if c is not None:
            return c
    d = _dollars_to_cents(_mget(m, "no_ask_dollars"))
    if d is not None and 1 <= d <= 99:
        return int(d)
    return None


def get_no_bid_cents(m: Any) -> Optional[int]:
    for field in ("no_bid", "no_bid_price", "best_no_bid", "bid_no"):
        c = normalize_top_of_book_cents(_mget(m, field))
        if c is not None:
            return c
    d = _dollars_to_cents(_mget(m, "no_bid_dollars"))
    if d is not None and 1 <= d <= 99:
        return int(d)
    return None


_BTC_USD_RANGE_RE = re.compile(
    r"\$?\s*([\d,]+(?:\.\d+)?)\s*(?:to|-|–)\s*\$?\s*([\d,]+(?:\.\d+)?)",
    re.I,
)


def _parse_usd_num_token(tok: str) -> float:
    return float(tok.replace(",", "").replace("$", "").strip())


def parse_btc_bucket_text(blob: str) -> tuple[Optional[float], Optional[float], BucketMode]:
    """Parse BTC hourly copy like \"$71,300 to 71,399.99\" or \"… or above\"."""
    if not blob:
        return None, None, "unknown"
    low_s = blob.lower()
    if "or above" in low_s or "or higher" in low_s:
        m = re.search(
            r"\$?\s*([\d,]+(?:\.\d+)?)\s*(?:or above|or higher)",
            blob,
            re.I,
        )
        if m:
            return _parse_usd_num_token(m.group(1)), None, "above"
    m = _BTC_USD_RANGE_RE.search(blob)
    if m:
        return (
            _parse_usd_num_token(m.group(1)),
            _parse_usd_num_token(m.group(2)),
            "range",
        )
    return None, None, "unknown"


def parse_btc_bucket_from_market(m: Any) -> tuple[Optional[float], Optional[float], BucketMode]:
    """Prefer API floor/cap when present; else subtitle/title text."""
    fl_raw = getattr(m, "floor_strike", None)
    cap_raw = getattr(m, "cap_strike", None)
    if fl_raw not in (None, "") and cap_raw not in (None, ""):
        try:
            fl, cap = float(fl_raw), float(cap_raw)
            if cap > fl:
                return fl, cap, "range"
        except (TypeError, ValueError):
            pass
    blob = " ".join(
        str(getattr(m, a, "") or "")
        for a in ("yes_sub_title", "subtitle", "title", "no_sub_title")
    )
    return parse_btc_bucket_text(blob)


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
    """Parse Kalshi-style bucket labels into (low, high, mode).

    Numeric tokens allow decimals so the same parser works for sports props
    (e.g. ``220.5 or above``) and weather (integer °F still matches).
    """
    if not text:
        return None, None, "unknown"
    t = text.lower().replace("℉", "f").replace("°", "")
    num = r"(\d+\.?\d*)"
    if "or below" in t or "or lower" in t:
        m = re.search(num, t)
        if m:
            hi = float(m.group(1))
            return None, hi, "below"
        return None, None, "unknown"
    if "or above" in t or "or higher" in t:
        m = re.search(num, t)
        if m:
            lo = float(m.group(1))
            return lo, None, "above"
        return None, None, "unknown"
    m = re.search(rf"{num}\s*f?\s*(?:to|-|–)\s*{num}\s*f?", t)
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
    m1 = re.search(r"(\d+\.?\d*)\s*°?\s*F", sub, re.I)
    if m1:
        return float(m1.group(1))
    return None


def _yes_no_book_cents(m: Any) -> tuple[Optional[float], Optional[float], bool]:
    """
    YES bid/ask in cents. Resolves several Kalshi field names and cent vs dollar scalars.

    When the API omits YES bid but lists NO ask, use YES bid = 100 − NO ask;
    same for YES ask from NO bid. If YES bid is still missing but YES ask exists, flag one-sided
    (mid = YES ask / 2 in :func:`contract_from_market`).
    """
    ya_i = get_yes_ask_cents(m)
    yb_i = get_yes_bid_cents(m)
    no_ask_i = get_no_ask_cents(m)
    no_bid_i = get_no_bid_cents(m)

    yb: Optional[float] = float(yb_i) if yb_i is not None else None
    ya: Optional[float] = float(ya_i) if ya_i is not None else None
    if yb is None and no_ask_i is not None:
        yb = 100.0 - float(no_ask_i)
    if ya is None and no_bid_i is not None:
        ya = 100.0 - float(no_bid_i)
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
        bl, bh, bm = parse_btc_bucket_from_market(m)
        if bm == "range" and bl is not None and bh is not None:
            k = float(bl)
        elif bm == "above" and bl is not None:
            k = float(bl) + 0.001
        else:
            k = extract_strike_btc(m)
        if k is None:
            return None
        tick = str(getattr(m, "ticker", "") or "")
        return ContractInput(
            ticker=tick,
            strike=float(k),
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

    if kind not in ("weather", "sports"):
        raise ValueError(f"contract_from_market: unsupported kind {kind!r} (use weather|sports|btc)")

    # Weather + sports: same YES/NO book inference; same subtitle / ``or above`` / range parsing as weather.
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
    if kind == "sports" and k is None:
        k = extract_strike_sports(m)
    if k is None:
        return None
    if bm != "unknown":
        sk = weather_sort_strike(bl, bh, bm)
    else:
        sk = float(k)
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


_SPORTS_STRIKE_NUM = re.compile(
    r"(?:over|at least|more than|scores?|records?)\s*(\d+\.?\d*)",
    re.I,
)


def extract_strike_sports(m: Any) -> Optional[float]:
    """Strike / threshold line for sports props (Kalshi floor_strike or copy)."""
    for attr in ("floor_strike", "cap_strike", "functional_strike"):
        v = _mget(m, attr)
        if v is not None and str(v) != "":
            try:
                return float(v)
            except (TypeError, ValueError):
                pass
    blob = " ".join(
        str(_mget(m, x) or "") for x in ("subtitle", "yes_sub_title", "title")
    )
    ma = _SPORTS_STRIKE_NUM.search(blob)
    if ma:
        return float(ma.group(1))
    m2 = re.search(r"(?:^|\s)(\d+\.\d+)(?:\s|\+|\?)", blob)
    if m2:
        return float(m2.group(1))
    return None


def contract_from_sports_market(m: Any) -> Optional[ContractInput]:
    """Sports ladder row: same book + subtitle threshold parsing as weather, then sports regex fallback."""
    return contract_from_market(m, kind="sports")
