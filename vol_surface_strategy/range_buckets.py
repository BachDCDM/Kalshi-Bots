"""Range-bucket weather markets: detect type, CDF→threshold, reverse-map to real contracts."""

from __future__ import annotations

from typing import Literal, Optional, Tuple

from scipy import stats

from vol_surface_strategy.config import yes_mid_tradeable
from vol_surface_strategy.entry_edge import (
    edge_cents_limit_order,
    limit_entry_cents,
    trade_side_from_mid_vs_fair,
)
from vol_surface_strategy.surface_math import ContractInput, fair_yes_lognormal_interval

MarketType = Literal["threshold", "range_bucket", "unknown"]


def upper_boundary_f(c: ContractInput) -> float:
    """Upper °F boundary for cumulative mass (spec: interior bucket_high + 0.5)."""
    if c.bucket_mode == "below" and c.bucket_high is not None:
        return float(c.bucket_high) + 0.5
    if c.bucket_mode == "range" and c.bucket_high is not None:
        return float(c.bucket_high) + 0.5
    if c.bucket_mode == "above" and c.bucket_low is not None:
        return float(c.bucket_low) + 0.5
    return float(c.strike) + 0.5


def integration_extents_f(c: ContractInput) -> tuple[float, float]:
    """(low, high) °F for Φ((hi-μ)/σ) − Φ((lo-μ)/σ)."""
    if c.bucket_mode == "below" and c.bucket_high is not None:
        u = float(c.bucket_high) + 0.5
        return (-80.0, u)
    if c.bucket_mode == "range" and c.bucket_low is not None and c.bucket_high is not None:
        return float(c.bucket_low) - 0.5, float(c.bucket_high) + 0.5
    if c.bucket_mode == "above" and c.bucket_low is not None:
        lo = float(c.bucket_low) - 0.5
        return lo, 140.0
    return float(c.strike) - 1.0, float(c.strike) + 1.0


def fair_interval_prob(mu: float, sigma: float, lo: float, hi: float) -> float:
    if sigma <= 0:
        return 0.0
    return float(stats.norm.cdf((hi - mu) / sigma) - stats.norm.cdf((lo - mu) / sigma))


def detect_market_type(mids: list[float]) -> MarketType:
    """
    mids: contract mid prices in cents, ordered left-to-right on the thermometer.
    """
    n = len(mids)
    if n < 3:
        return "unknown"
    if all(mids[i] > mids[i + 1] for i in range(n - 1)):
        return "threshold"
    imax = max(range(n), key=lambda i: mids[i])
    if imax == n - 1:
        return "unknown"
    if imax == 0:
        return "threshold" if all(mids[i] > mids[i + 1] for i in range(n - 1)) else "unknown"
    inc_left = all(mids[i] <= mids[i + 1] for i in range(imax))
    # Weak ≥ on the right tail so equal far-tail bucket mids (e.g. 1¢, 1¢) still classify.
    dec_right = all(mids[i] >= mids[i + 1] for i in range(imax, n - 1))
    if inc_left and dec_right:
        return "range_bucket"
    return "unknown"


def sort_range_contracts(raw: list[ContractInput]) -> list[ContractInput]:
    return sorted(raw, key=upper_boundary_f)


def marginal_partition_mass(raw: list[ContractInput]) -> float:
    """Sum of marginal YES probabilities (as fractions); should be ~1 for a full ladder."""
    if not raw:
        return 0.0
    return sum(c.mid_cents / 100.0 for c in sort_range_contracts(raw))


def marginal_partition_mass_btc(raw: list[ContractInput]) -> float:
    """Same as :func:`marginal_partition_mass` but USD bucket order (no °F +0.5)."""
    if not raw:
        return 0.0
    return sum(c.mid_cents / 100.0 for c in sort_btc_range_contracts(raw))


def convert_range_to_threshold(
    raw: list[ContractInput],
) -> Optional[tuple[list[ContractInput], list[ContractInput]]]:
    """
    Cumulative sum of bucket mids → derived P(T ≥ K) contracts.
    Returns (derived_contracts, raw_sorted_same_order_as_cumulative) or None if invalid sum.
    """
    for c in raw:
        if c.bucket_mode == "unknown":
            return None
    sraw = sort_range_contracts(raw)
    mass = sum(c.mid_cents / 100.0 for c in sraw)
    # Stale / inconsistent books: raw mids can sum to 85–115% before renormalization.
    if mass < 0.85:
        return None
    if mass > 1.15:
        return None
    scale = 1.0 / mass
    cum = 0.0
    derived: list[ContractInput] = []
    for ri, c in enumerate(sraw):
        w = (c.mid_cents / 100.0) * scale
        cum += w
        k = upper_boundary_f(c)
        p_ge = 1.0 - cum
        midc = p_ge * 100.0
        derived.append(
            ContractInput(
                ticker=f"derived|{c.ticker}",
                strike=k,
                mid_cents=midc,
                yes_bid_cents=max(1.0, midc - 0.5),
                yes_ask_cents=min(99.0, midc + 0.5),
                volume_fp=c.volume_fp,
                bucket_mode="unknown",
                is_derived_threshold=True,
                source_raw_index=ri,
                one_sided=c.one_sided,
            )
        )
    return derived, sraw


def plausibility_bucket_mass(mu: float, sigma: float, raw_sorted: list[ContractInput]) -> float:
    s = 0.0
    for c in raw_sorted:
        lo, hi = integration_extents_f(c)
        s += fair_interval_prob(mu, sigma, lo, hi)
    return s


def reverse_map_to_bucket(
    outlier_raw_bucket_idx: int,
    mu: float,
    sigma: float,
    raw_sorted: list[ContractInput],
) -> Optional[tuple[ContractInput, float, float, Literal["yes", "no"], int]]:
    """
    Pick the real bucket with largest |edge| at limit price (best_ask − 1¢ on entry side).
    outlier_raw_bucket_idx indexes raw_sorted (same order as CDF build).
    Only buckets with YES mid in yes_mid_tradeable() (excludes extreme tails).
    Returns (contract, p_fair_yes, edge_cents, side, entry_cents) or None if none qualify.
    """
    n = len(raw_sorted)
    if n < 2:
        raise ValueError("need ≥2 buckets")
    k = max(0, min(outlier_raw_bucket_idx, n - 1))
    candidates: list[int] = []
    if k == 0:
        candidates = [0, 1]
    elif k >= n - 1:
        candidates = [n - 2, n - 1]
    else:
        candidates = [k, k + 1]

    best_abs = -1.0
    best_spread = 1e9
    best_out: Optional[Tuple[ContractInput, float, float, Literal["yes", "no"], int]] = None
    for j in candidates:
        if j < 0 or j >= n:
            continue
        c = raw_sorted[j]
        if not yes_mid_tradeable(c.mid_cents):
            continue
        lo, hi = integration_extents_f(c)
        pf = fair_interval_prob(mu, sigma, lo, hi)
        m = c.mid_cents / 100.0
        side = trade_side_from_mid_vs_fair(m, pf)
        ent = limit_entry_cents(side, c)
        if ent is None:
            continue
        edge = edge_cents_limit_order(side, pf, ent)
        abs_e = abs(edge)
        spread = c.yes_ask_cents - c.yes_bid_cents
        if abs_e > best_abs + 1e-9 or (abs(abs_e - best_abs) <= 1e-9 and spread < best_spread):
            best_abs = abs_e
            best_spread = spread
            best_out = (c, pf, edge, side, ent)
    return best_out


def edge_stability_interval(
    market_p: float,
    mu: float,
    sigma: float,
    lo: float,
    hi: float,
) -> bool:
    def e_at(s: float) -> float:
        pf = fair_interval_prob(mu, s, lo, hi)
        return market_p - pf

    e0 = e_at(sigma)
    if abs(e0) < 1e-12:
        return True
    for mult in (0.90, 1.10):
        e = e_at(sigma * mult)
        if (e > 0) != (e0 > 0):
            return False
    return True


# --- BTC hourly: $100 range buckets → synthetic cumulative thresholds (lognormal downstream) ---


def upper_boundary_btc_usd(c: ContractInput) -> float:
    """Upper USD bound for CDF accumulation (bucket_high; no °F-style +0.5)."""
    if c.bucket_mode == "range" and c.bucket_high is not None:
        return float(c.bucket_high)
    if c.bucket_mode == "below" and c.bucket_high is not None:
        return float(c.bucket_high)
    if c.bucket_mode == "above" and c.bucket_low is not None:
        return float(c.bucket_low)
    return float(c.strike)


def sort_btc_range_contracts(raw: list[ContractInput]) -> list[ContractInput]:
    return sorted(raw, key=upper_boundary_btc_usd)


def convert_btc_range_to_threshold(
    raw: list[ContractInput],
) -> Optional[tuple[list[ContractInput], list[ContractInput]]]:
    """
    Marginal bucket mids (same as weather) → derived P(S > K) at each bucket's upper USD bound.
    """
    for c in raw:
        if c.bucket_mode == "unknown":
            return None
    sraw = sort_btc_range_contracts(raw)
    mass = sum(c.mid_cents / 100.0 for c in sraw)
    # One-sided / thin tail quotes inflate per-bucket mids; sum often >1 — renormalize like weather.
    if mass < 0.40:
        return None
    if mass > 3.0:
        return None
    scale = 1.0 / mass
    cum = 0.0
    derived: list[ContractInput] = []
    for ri, c in enumerate(sraw):
        w = (c.mid_cents / 100.0) * scale
        cum += w
        k = upper_boundary_btc_usd(c)
        p_ge = 1.0 - cum
        midc = p_ge * 100.0
        derived.append(
            ContractInput(
                ticker=f"derived|{c.ticker}",
                strike=k,
                mid_cents=midc,
                yes_bid_cents=max(1.0, midc - 0.5),
                yes_ask_cents=min(99.0, midc + 0.5),
                volume_fp=c.volume_fp,
                bucket_mode="unknown",
                is_derived_threshold=True,
                source_raw_index=ri,
                one_sided=c.one_sided,
            )
        )
    return derived, sraw


def plausibility_btc_bucket_mass(
    s_star: float, sigma: float, t_years: float, raw_sorted: list[ContractInput]
) -> float:
    s = 0.0
    for c in raw_sorted:
        if c.bucket_mode != "range" or c.bucket_low is None or c.bucket_high is None:
            continue
        s += fair_yes_lognormal_interval(
            s_star, float(c.bucket_low), float(c.bucket_high), sigma, t_years
        )
    return s


def edge_stability_btc_interval(
    market_p: float,
    s_star: float,
    sigma: float,
    t_years: float,
    k_lo: float,
    k_hi: float,
) -> bool:
    def e_at(sig: float) -> float:
        pf = fair_yes_lognormal_interval(s_star, k_lo, k_hi, sig, t_years)
        return market_p - pf

    e0 = e_at(sigma)
    if abs(e0) < 1e-12:
        return True
    for mult in (0.90, 1.10):
        e = e_at(sigma * mult)
        if (e > 0) != (e0 > 0):
            return False
    return True


def best_btc_bucket_trade_by_edge(
    s_star: float,
    sigma: float,
    t_years: float,
    raw_sorted: list[ContractInput],
) -> tuple[ContractInput, float, float, Literal["yes", "no"], int]:
    """
    Among all real range buckets, pick the largest |limit-order edge| vs lognormal interval fair YES.
    Returns (contract, p_fair_yes_interval, edge_cents, side, entry_cents).
    """
    best: Optional[Tuple[ContractInput, float, float, Literal["yes", "no"], int]] = None
    best_abs = -1.0
    best_spread = 1e9
    for c in raw_sorted:
        if c.bucket_mode != "range" or c.bucket_low is None or c.bucket_high is None:
            continue
        if not yes_mid_tradeable(c.mid_cents):
            continue
        lo, hi = float(c.bucket_low), float(c.bucket_high)
        pf = fair_yes_lognormal_interval(s_star, lo, hi, sigma, t_years)
        m = c.mid_cents / 100.0
        side = trade_side_from_mid_vs_fair(m, pf)
        ent = limit_entry_cents(side, c)
        if ent is None:
            continue
        edge = edge_cents_limit_order(side, pf, ent)
        abs_e = abs(edge)
        spread = c.yes_ask_cents - c.yes_bid_cents
        if abs_e > best_abs + 1e-9 or (abs(abs_e - best_abs) <= 1e-9 and spread < best_spread):
            best_abs = abs_e
            best_spread = spread
            best = (c, pf, edge, side, ent)
    if best is None:
        raise ValueError("no BTC range buckets for edge scan (or none in YES-mid trade band)")
    return best
