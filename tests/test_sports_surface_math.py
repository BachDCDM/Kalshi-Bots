"""Tests for Poisson / negative-binomial tail helpers used in sports vol surfaces."""

from __future__ import annotations

import math

from vol_surface_strategy.surface_math import (
    fair_yes_nbinom,
    fair_yes_poisson,
    implied_mean_nbinom_pair,
    implied_rate_poisson_pair,
    tail_prob_nbinom,
    tail_prob_poisson,
)


def test_poisson_tail_monotone_in_lambda() -> None:
    ks = [0.5, 1.5, 2.5]
    for k in ks:
        p1 = tail_prob_poisson(2.0, k)
        p2 = tail_prob_poisson(3.0, k)
        assert p1 > p2, (k, p1, p2)


def test_poisson_tail_decreases_in_strike() -> None:
    lam = 2.5
    assert tail_prob_poisson(lam, 0.5) > tail_prob_poisson(lam, 1.5) > tail_prob_poisson(lam, 2.5)


def test_implied_poisson_pair_reasonable() -> None:
    lam = 2.2
    ki, kj = 0.5, 2.5
    pi = tail_prob_poisson(lam, ki)
    pj = tail_prob_poisson(lam, kj)
    est = implied_rate_poisson_pair(ki, kj, pi, pj)
    assert est is not None
    assert abs(est - lam) < 0.35


def test_nbinom_tail_and_pair() -> None:
    mu, r = 3.2, 8.0
    p_hi = tail_prob_nbinom(mu, r, 1.5)
    p_lo = tail_prob_nbinom(mu, r, 4.5)
    assert p_hi > p_lo
    est = implied_mean_nbinom_pair(1.5, 4.5, p_hi, p_lo, r)
    assert est is not None
    assert abs(est - mu) < 0.6


def test_fair_yes_poisson_clamped() -> None:
    f = fair_yes_poisson(1.0, 0.5)
    assert 0.0 <= f <= 1.0
    assert math.isfinite(f)


def test_fair_yes_nbinom_clamped() -> None:
    f = fair_yes_nbinom(4.0, 8.0, 2.5)
    assert 0.0 <= f <= 1.0
