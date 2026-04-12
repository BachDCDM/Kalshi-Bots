"""Tests for settlement gross payout (reconstructed for yes/no; API revenue can be net)."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

_CP = Path(__file__).resolve().parents[1] / "control-panel"
if str(_CP) not in sys.path:
    sys.path.insert(0, str(_CP))

from settlement_sync import settlement_gross_payout_cents, settlement_net_pnl_cents


def test_gross_payout_reconstructs_from_fp_counts_yes_wins() -> None:
    st = SimpleNamespace(
        revenue=0,
        market_result="yes",
        yes_count_fp="5.00",
        no_count_fp="0.00",
        value=100,
    )
    assert settlement_gross_payout_cents(st) == 500


def test_gross_payout_reconstructs_no_wins() -> None:
    """Kalshi uses ``value=0`` when the market resolves NO (YES leg pays 0¢, NO pays 100¢)."""
    st = SimpleNamespace(
        revenue=0,
        market_result="no",
        yes_count_fp="0.00",
        no_count_fp="3.00",
        value=0,
    )
    assert settlement_gross_payout_cents(st) == 300


def test_no_result_defaults_value_when_missing() -> None:
    st = SimpleNamespace(
        revenue=0,
        market_result="no",
        yes_count_fp="0.00",
        no_count_fp="2.00",
        value=None,
    )
    assert settlement_gross_payout_cents(st) == 200


def test_yes_result_defaults_value_when_missing() -> None:
    st = SimpleNamespace(
        revenue=0,
        market_result="yes",
        yes_count_fp="4.00",
        no_count_fp="0.00",
        value=None,
    )
    assert settlement_gross_payout_cents(st) == 400


def test_yes_no_always_reconstructs_even_if_api_revenue_differs() -> None:
    """Nonzero API revenue must not override gross reconstruction (revenue can be net)."""
    st = SimpleNamespace(
        revenue=2500,
        market_result="yes",
        yes_count_fp="1.00",
        no_count_fp="0.00",
        value=100,
    )
    assert settlement_gross_payout_cents(st) == 100


def test_dual_sided_ignores_net_api_revenue() -> None:
    """Dual-sided: API revenue = net position payout; gross = full winning leg."""
    st = SimpleNamespace(
        revenue=100,
        market_result="yes",
        yes_count_fp="10.00",
        no_count_fp="9.00",
        value=100,
        yes_total_cost=250,
        no_total_cost=450,
        fee_cost="0",
    )
    assert settlement_gross_payout_cents(st) == 1000
    gp, yc, nc, fc, net = settlement_net_pnl_cents(st)
    assert gp == 1000
    assert yc == 250
    assert nc == 450
    assert net == 1000 - 250 - 450 - fc


def test_net_pnl_uses_reconstructed_payout() -> None:
    st = SimpleNamespace(
        revenue=0,
        market_result="yes",
        yes_count_fp="2.00",
        no_count_fp="0.00",
        value=100,
        yes_total_cost=None,
        yes_total_cost_dollars="2.500000",
        no_total_cost=None,
        no_total_cost_dollars="0.000000",
        fee_cost="0.010000",
    )
    gp, yc, nc, fc, net = settlement_net_pnl_cents(st)
    assert gp == 200
    assert yc == 250
    assert nc == 0
    assert fc == 1
    assert net == 200 - 250 - 0 - 1


def test_void_uses_api_only() -> None:
    st = SimpleNamespace(revenue=42, market_result="void", yes_count_fp="9.00", no_count_fp="9.00")
    assert settlement_gross_payout_cents(st) == 42
