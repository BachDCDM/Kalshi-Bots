"""Pairwise vol extraction, outlier detection, fair value, edge, Kelly — pure math."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal, Optional

BucketMode = Literal["unknown", "below", "above", "range"]

import numpy as np
from scipy import stats

ModelKind = Literal["lognormal", "normal"]


def clamp_prob(p: float) -> float:
    return max(0.001, min(0.999, p))


def z_from_prob(p: float) -> float:
    return float(stats.norm.ppf(clamp_prob(p)))


def phi(x: float) -> float:
    return float(stats.norm.cdf(x))


def years_from_seconds(sec: float) -> float:
    return max(sec, 60.0) / (365.25 * 24 * 3600)


@dataclass
class ContractInput:
    ticker: str
    strike: float  # K: USD for BTC, °F sort key / midpoint for weather
    mid_cents: float
    yes_bid_cents: float
    yes_ask_cents: float
    volume_fp: float
    yes_bid_size_fp: float = 0.0
    yes_ask_size_fp: float = 0.0
    bucket_low: Optional[float] = None
    bucket_high: Optional[float] = None
    bucket_mode: BucketMode = "unknown"
    is_derived_threshold: bool = False
    source_raw_index: Optional[int] = None  # maps derived row → range-bucket index
    # True when YES bid was missing and mid was inferred from YES ask only (thin NO book).
    one_sided: bool = False


@dataclass
class PairResult:
    i: int
    j: int
    sigma: float
    valid: bool
    reason: str = ""


def _pair_valid_prices(
    pi: float, pj: float, zi: float, zj: float, *, min_dz: float = 0.05
) -> tuple[bool, str]:
    if pi <= pj:
        return False, "not_monotone_P"
    if zi <= zj:
        return False, "not_monotone_z"
    dz = zi - zj
    if dz <= min_dz:
        return False, "dz_too_small"
    return True, ""


def sigma_pair_lognormal(ki: float, kj: float, zi: float, zj: float, t_years: float) -> Optional[float]:
    if zi <= zj:
        return None
    dz = zi - zj
    if dz <= 0.05:
        return None
    sqrt_t = math.sqrt(t_years)
    if sqrt_t <= 0:
        return None
    num = math.log(kj / ki)
    sig = num / (dz * sqrt_t)
    if sig <= 0.01 or sig >= 20.0:
        return None
    return sig


def sigma_pair_normal(ki: float, kj: float, zi: float, zj: float) -> Optional[float]:
    if zi <= zj:
        return None
    dz = zi - zj
    if dz <= 0.05:
        return None
    sig = (kj - ki) / dz
    if sig <= 0.3 or sig >= 30.0:
        return None
    return sig


def implied_S_lognormal(k: float, z: float, sigma: float, t_years: float) -> float:
    st = math.sqrt(t_years)
    return k * math.exp(z * sigma * st + (sigma * sigma) * t_years / 2)


def implied_mu_normal(k: float, z: float, sigma: float) -> float:
    return k + sigma * z


def fair_yes_lognormal(s_star: float, k: float, sigma: float, t_years: float) -> float:
    st = math.sqrt(t_years)
    if st <= 0 or sigma <= 0:
        return 0.5
    d2 = (math.log(s_star / k) - (sigma * sigma) * t_years / 2) / (sigma * st)
    return phi(d2)


def fair_yes_lognormal_interval(
    s_star: float,
    k_lo: float,
    k_hi: float,
    sigma: float,
    t_years: float,
) -> float:
    """
    Risk-neutral probability that terminal spot lies in (k_lo, k_hi], with k_lo < k_hi.
    Equals P(S > k_lo) − P(S > k_hi) under the same lognormal used for digital calls.
    """
    if k_hi <= k_lo:
        return 0.0
    a = fair_yes_lognormal(s_star, k_lo, sigma, t_years)
    b = fair_yes_lognormal(s_star, k_hi, sigma, t_years)
    return max(0.0, min(1.0, a - b))


def fair_yes_normal(mu_star: float, k: float, sigma: float) -> float:
    if sigma <= 0:
        return 0.5
    return phi((mu_star - k) / sigma)


def build_pair_matrix(
    contracts: list[ContractInput],
    model: ModelKind,
    t_years: float,
    *,
    adjacent_min_dz: Optional[float] = None,
) -> list[PairResult]:
    """
    contracts must be strike-sorted (as after _monotone_subset).

    If adjacent_min_dz is set (range-bucket derived surfaces), consecutive indices (i, i+1)
    require z_i - z_j > adjacent_min_dz; non-consecutive pairs still use min_dz=0.05.
    """
    n = len(contracts)
    out: list[PairResult] = []
    for i in range(n):
        for j in range(i + 1, n):
            ki = contracts[i].strike
            kj = contracts[j].strike
            if kj <= ki:
                out.append(PairResult(i, j, 0.0, False, "strike_order"))
                continue
            pi = contracts[i].mid_cents / 100.0
            pj = contracts[j].mid_cents / 100.0
            zi = z_from_prob(pi)
            zj = z_from_prob(pj)
            min_dz = (
                adjacent_min_dz
                if adjacent_min_dz is not None and j == i + 1
                else 0.05
            )
            ok, reason = _pair_valid_prices(pi, pj, zi, zj, min_dz=min_dz)
            if not ok:
                out.append(PairResult(i, j, 0.0, False, reason))
                continue
            if model == "lognormal":
                sig = sigma_pair_lognormal(ki, kj, zi, zj, t_years)
            else:
                sig = sigma_pair_normal(ki, kj, zi, zj)
            if sig is None:
                out.append(PairResult(i, j, 0.0, False, "sigma_bounds"))
            else:
                out.append(PairResult(i, j, sig, True, ""))
    return out


def leave_one_out_sigmas(
    pair_results: list[PairResult],
    n: int,
) -> tuple[Optional[int], list[float], list[float]]:
    """Returns (outlier_idx, consistency_per_k, raw_scores). If ambiguous, outlier_idx None."""
    valid_by_pair = [(pr.i, pr.j, pr.sigma) for pr in pair_results if pr.valid]
    if len(valid_by_pair) < 3:
        return None, [], []

    scores: list[float] = []
    for k in range(n):
        sigs = [s for (i, j, s) in valid_by_pair if i != k and j != k]
        nk = len(sigs)
        if nk < 2:
            scores.append(float("inf"))
        else:
            scores.append(float(np.std(sigs, ddof=1)))
    sorted_scores = sorted(scores)
    if len(sorted_scores) < 2:
        return None, scores, scores
    best = sorted_scores[0]
    second = sorted_scores[1]
    if second <= 0:
        return None, scores, scores
    gap = (second - best) / second
    if gap < 0.25:
        return None, scores, scores
    outlier = int(np.argmin(scores))
    return outlier, scores, sorted_scores


def infection_outlier(pair_results: list[PairResult], n: int) -> Optional[int]:
    valid = [(pr.i, pr.j, pr.sigma) for pr in pair_results if pr.valid]
    if len(valid) < 3:
        return None
    sigs = [s for _, _, s in valid]
    med = float(np.median(sigs))
    infections = [0.0] * n
    for i, j, s in valid:
        infections[i] += abs(s - med)
        infections[j] += abs(s - med)
    return int(np.argmax(infections))


def consensus_sigma_star(pair_results: list[PairResult], outlier_idx: int) -> Optional[float]:
    sigs = [pr.sigma for pr in pair_results if pr.valid and pr.i != outlier_idx and pr.j != outlier_idx]
    if not sigs:
        return None
    return float(np.median(sigs))


def consensus_underlying(
    contracts: list[ContractInput],
    pair_results: list[PairResult],
    outlier_idx: int,
    model: ModelKind,
    sigma_star: float,
    t_years: float,
) -> Optional[float]:
    estimates: list[float] = []
    for a, c in enumerate(contracts):
        if a == outlier_idx:
            continue
        p = c.mid_cents / 100.0
        z = z_from_prob(p)
        if model == "lognormal":
            estimates.append(implied_S_lognormal(c.strike, z, sigma_star, t_years))
        else:
            estimates.append(implied_mu_normal(c.strike, z, sigma_star))
    if not estimates:
        return None
    return float(np.median(estimates))


def plausibility_sigma(sigma_star: float, model: ModelKind) -> bool:
    if model == "lognormal":
        return 0.20 <= sigma_star <= 3.00
    return 0.5 <= sigma_star <= 15.0


def edge_stability(
    market_mid_yes: float,
    s_or_mu: float,
    k_out: float,
    sigma_star: float,
    t_years: float,
    model: ModelKind,
) -> bool:
    def edge_at(sig: float) -> float:
        if model == "lognormal":
            pf = fair_yes_lognormal(s_or_mu, k_out, sig, t_years)
        else:
            pf = fair_yes_normal(s_or_mu, k_out, sig)
        return market_mid_yes - pf

    e0 = edge_at(sigma_star)
    if abs(e0) < 1e-12:
        return True
    for mult in (0.90, 1.10):
        e = edge_at(sigma_star * mult)
        if (e > 0) != (e0 > 0):
            return False
    return True


def kelly_fraction(p_win: float, entry_prob: float) -> float:
    """entry_prob = price paid as fraction of $1 (e.g. 0.52)."""
    if entry_prob <= 0 or entry_prob >= 1:
        return 0.0
    p_lose = 1.0 - p_win
    win_payout = 1.0 - entry_prob
    if win_payout <= 0:
        return 0.0
    return p_win - p_lose * (entry_prob / win_payout)


def contracts_to_buy(
    portfolio_cents: int,
    f_trade: float,
    entry_cents: int,
) -> int:
    if entry_cents <= 0 or f_trade <= 0:
        return 0
    dollars = (portfolio_cents / 100.0) * f_trade
    ec = entry_cents / 100.0
    n = int(math.floor(dollars / ec))
    return max(0, n)
