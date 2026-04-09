"""Limit-order edge: fair value minus actual entry at best_ask − 1¢ (YES or NO side)."""

from __future__ import annotations

from typing import Any, Literal

from vol_surface_strategy.config import MAX_SPREAD_ENTRY_CENTS, MIN_EDGE_CENTS

Side = Literal["yes", "no"]


def yes_spread_cents(c: Any) -> float:
    return float(c.yes_ask_cents) - float(c.yes_bid_cents)


def trade_side_from_mid_vs_fair(mid_yes: float, p_fair_yes: float) -> Side:
    """YES overpriced vs model → fade by buying NO; else buy YES."""
    return "no" if mid_yes > p_fair_yes else "yes"


def limit_entry_cents(side: Side, c: Any) -> int | None:
    """
    Maker buy at best ask on the entry side minus 1¢ (clamped to [2, 98]).
    NO ask = 100 − YES bid.
    """
    if side == "yes":
        ya = float(c.yes_ask_cents)
        if ya <= 0:
            return None
        return int(max(2, min(98, round(ya - 1))))
    yb = float(c.yes_bid_cents)
    if yb <= 0:
        return None
    best_no_ask = 100.0 - yb
    return int(max(2, min(98, round(best_no_ask - 1))))


def edge_cents_limit_order(side: Side, p_fair_yes: float, entry_cents: int) -> float:
    """Model value of what you buy minus price paid, in cents."""
    ep = entry_cents / 100.0
    if side == "yes":
        return (p_fair_yes - ep) * 100.0
    fair_no = 1.0 - p_fair_yes
    return (fair_no - ep) * 100.0


def yes_implied_prob_for_stability(side: Side, entry_cents: int) -> float:
    """YES probability implied by the limit price on the contract (for σ-stability checks)."""
    ep = entry_cents / 100.0
    if side == "yes":
        return ep
    return 1.0 - ep


def spread_blocks_entry(c: Any) -> bool:
    """Hard gate: YES bid–ask spread (cents) must be ≤ MAX_SPREAD_ENTRY_CENTS."""
    return yes_spread_cents(c) > MAX_SPREAD_ENTRY_CENTS


__all__ = [
    "MIN_EDGE_CENTS",
    "MAX_SPREAD_ENTRY_CENTS",
    "Side",
    "yes_spread_cents",
    "trade_side_from_mid_vs_fair",
    "limit_entry_cents",
    "edge_cents_limit_order",
    "yes_implied_prob_for_stability",
    "spread_blocks_entry",
]
