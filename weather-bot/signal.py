"""Forecast vs market: bucket probabilities, trade evaluation, early exit."""

from __future__ import annotations

from typing import Any, Optional

from config import (
    EARLY_EXIT_MIN_HOURS_REMAINING,
    EARLY_EXIT_PROFIT_CENTS,
    MAX_SPREAD_CENTS,
    MIN_DISTANCE_FROM_BUCKET_EDGE,
    MIN_EDGE,
)


def _bucket_sort_key(b: dict[str, Any]) -> tuple[float, float]:
    lo, hi = b["low"], b["high"]
    return (
        lo if lo is not None else float("-inf"),
        hi if hi is not None else float("inf"),
    )


def _find_target_bucket(projected_high: float, buckets: list[dict]) -> Optional[dict]:
    for b in buckets:
        low, high = b["low"], b["high"]
        if low is None and high is not None and projected_high <= high:
            return b
        if high is None and low is not None and projected_high >= low:
            return b
        if low is not None and high is not None and low <= projected_high <= high:
            return b
    return None


def _distance_from_bucket_edges(projected_high: float, b: dict) -> float:
    low, high = b["low"], b["high"]
    if low is None and high is not None:
        return float(high - projected_high)
    if high is None and low is not None:
        return float(projected_high - low)
    if low is not None and high is not None:
        return float(min(projected_high - low, high - projected_high))
    return 0.0


def projected_high_to_bucket_probabilities(
    projected_high: float, buckets: list[dict]
) -> dict[str, float]:
    if not buckets:
        return {}

    ordered = sorted(buckets, key=_bucket_sort_key)
    target = _find_target_bucket(projected_high, ordered)
    if target is None:
        n = len(ordered)
        return {b["ticker"]: 1.0 / n for b in ordered}

    dist = _distance_from_bucket_edges(projected_high, target)
    target_idx = ordered.index(target)
    n = len(ordered)
    denom = max(1, n - 3)

    probs: dict[str, float] = {}
    if dist >= MIN_DISTANCE_FROM_BUCKET_EDGE:
        for i, b in enumerate(ordered):
            t = b["ticker"]
            if i == target_idx:
                probs[t] = 0.70
            elif abs(i - target_idx) == 1:
                probs[t] = 0.12
            else:
                probs[t] = 0.02 / denom
    else:
        for i, b in enumerate(ordered):
            t = b["ticker"]
            if i == target_idx:
                probs[t] = 0.45
            elif abs(i - target_idx) == 1:
                probs[t] = 0.22
            elif abs(i - target_idx) == 2:
                probs[t] = 0.05
            else:
                probs[t] = 0.01

    total = sum(probs.values())
    return {t: p / total for t, p in probs.items()}


def evaluate_trades(
    projected_high_data: dict,
    buckets_with_orderbooks: list[dict],
    now_local,
    close_time_local,
) -> list[dict]:
    trades: list[dict] = []

    if projected_high_data.get("is_stale"):
        return []

    hours_to_close = (close_time_local - now_local).total_seconds() / 3600.0
    if hours_to_close < 1 or hours_to_close > 6:
        return []

    projected_high = float(projected_high_data["projected_high"])
    ordered = sorted(buckets_with_orderbooks, key=_bucket_sort_key)
    target = _find_target_bucket(projected_high, ordered)
    if target is None:
        return []

    dist = _distance_from_bucket_edges(projected_high, target)
    if dist < MIN_DISTANCE_FROM_BUCKET_EDGE:
        return []

    model_probs = projected_high_to_bucket_probabilities(projected_high, ordered)

    for b in buckets_with_orderbooks:
        ticker = b["ticker"]
        if b.get("spread") is None or b.get("midpoint") is None:
            continue
        if b["spread"] > MAX_SPREAD_CENTS:
            continue

        model_prob = model_probs.get(ticker, 0.0)
        market_prob = (b["midpoint"] or 0) / 100.0
        edge = model_prob - market_prob

        if abs(edge) < MIN_EDGE:
            continue

        if edge > 0:
            side = "yes"
            yes_ask = b.get("yes_ask")
            if yes_ask is None:
                continue
            limit_price_cents = int(yes_ask)
        else:
            side = "no"
            yes_bid = b.get("yes_bid")
            if yes_bid is None:
                continue
            limit_price_cents = 100 - int(yes_bid)

        trades.append(
            {
                "ticker": ticker,
                "title": b.get("title", ""),
                "side": side,
                "limit_price_cents": limit_price_cents,
                "edge": round(abs(edge), 3),
                "model_prob": round(model_prob, 3),
                "market_prob": round(market_prob, 3),
                "hours_to_close": round(hours_to_close, 1),
            }
        )

    trades.sort(key=lambda x: x["edge"], reverse=True)
    return trades


def check_early_exits(
    tracked: list[dict],
    buckets_with_orderbooks: list[dict],
    now_local,
    close_time_local,
) -> list[dict]:
    exits: list[dict] = []
    hours_to_close = (close_time_local - now_local).total_seconds() / 3600.0

    if hours_to_close < EARLY_EXIT_MIN_HOURS_REMAINING:
        return []

    ob_map = {b["ticker"]: b for b in buckets_with_orderbooks}

    for tr in tracked:
        ticker = tr["ticker"]
        if ticker not in ob_map:
            continue
        ob = ob_map[ticker]
        entry = int(tr["entry_price_cents"])
        side = tr["side"]

        if side == "yes":
            yb = ob.get("yes_bid")
            if yb is None:
                continue
            profit = int(yb) - entry
            exit_price = int(yb)
        else:
            ya = ob.get("yes_ask")
            if ya is None:
                continue
            no_bid = 100 - int(ya)
            profit = no_bid - entry
            exit_price = no_bid

        if profit >= EARLY_EXIT_PROFIT_CENTS:
            exits.append(
                {
                    "ticker": ticker,
                    "side": side,
                    "limit_price_cents": exit_price,
                    "profit_cents": profit,
                    "reason": (
                        f"early_exit: up {profit}¢ with {hours_to_close:.1f}h remaining"
                    ),
                }
            )

    return exits
