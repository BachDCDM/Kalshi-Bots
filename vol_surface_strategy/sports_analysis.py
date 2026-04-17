"""Pre-game sports vol surface: same LOO / gates / sizing as BTC threshold path, discrete models optional."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from vol_surface_strategy.analysis import (
    MIN_EDGE_CENTS,
    ScanResult,
    _filter_informative,
    _monotone_subset,
    _passes_gate2_liquidity,
    _volume_dollars,
    yes_mid_tradeable,
)
from vol_surface_strategy.config import SPORTS_GATE2_MIN_BOOK_SZ, SPORTS_SURFACE_MAX_YES_SPREAD_CENTS
from vol_surface_strategy.entry_edge import (
    edge_cents_limit_order,
    limit_entry_cents,
    spread_blocks_entry,
    trade_side_from_mid_vs_fair,
    yes_implied_prob_for_stability,
    yes_spread_cents,
)
from vol_surface_strategy.surface_math import (
    ModelKind,
    build_pair_matrix,
    consensus_sigma_star,
    consensus_underlying,
    contracts_to_buy,
    edge_stability,
    edge_stability_nbinom,
    edge_stability_poisson,
    fair_yes_nbinom,
    fair_yes_normal,
    fair_yes_poisson,
    infection_outlier,
    kelly_fraction,
    leave_one_out_sigmas,
    plausibility_sigma,
    years_from_seconds,
)


def _filter_sports_surface_contracts(contracts: list[Any]) -> list[Any]:
    """
    Sports threshold surface inputs: same 5–95¢ mid band as ``_filter_informative``, then drop
    contracts whose YES bid–ask spread exceeds ``SPORTS_SURFACE_MAX_YES_SPREAD_CENTS`` so wide,
    near-certain tails do not distort monotone selection or the LOO fit.
    """
    mid_ok = _filter_informative(contracts)
    return [c for c in mid_ok if yes_spread_cents(c) <= SPORTS_SURFACE_MAX_YES_SPREAD_CENTS]


def sports_pipeline_debug_lines(
    raw_contracts: list[Any],
    *,
    use_weather_liquidity_gate: bool = False,
    min_informative: int = 4,
) -> list[str]:
    """Printable ladder / gate breakdown for sports (threshold + gate2), mirrors BTC debug style."""
    lines: list[str] = []
    n = len(raw_contracts)
    lines.append(f"--- sports pipeline debug: raw parsable contracts n={n} (need ≥{min_informative} informative) ---")
    if n == 0:
        return lines

    def row(c: Any) -> str:
        os1 = getattr(c, "one_sided", False)
        return (
            f"  {c.ticker[:52]:<52} K={c.strike:>12.2f}  "
            f"yb={c.yes_bid_cents:>5.1f} ya={c.yes_ask_cents:>5.1f}  "
            f"mid={c.mid_cents:>5.2f}¢  one_sided={os1!s:5}  vol_fp={getattr(c, 'volume_fp', 0)}"
        )

    lines.append("(all raw, strike-sorted)")
    s_all = sorted(raw_contracts, key=lambda x: x.strike)
    for c in s_all:
        lines.append(row(c))

    inf = _filter_informative(raw_contracts)
    lines.append(f"--- informative 5–95¢ mid: n={len(inf)} ---")
    s_inf = sorted(inf, key=lambda x: x.strike)
    for c in s_inf:
        lines.append(row(c))

    surf = _filter_sports_surface_contracts(raw_contracts)
    lines.append(
        f"--- surface inputs (5–95¢ mid AND YES spread≤{int(SPORTS_SURFACE_MAX_YES_SPREAD_CENTS)}¢ "
        f"for monotone / LOO): n={len(surf)} (need ≥{min_informative}) ---"
    )
    s_surf = sorted(surf, key=lambda x: x.strike)
    for c in s_surf:
        lines.append(row(c))
    dropped_spread = [c for c in s_inf if c.ticker not in {x.ticker for x in surf}]
    if dropped_spread:
        lines.append("--- dropped: YES spread too wide for surface (still usable elsewhere) ---")
        for c in dropped_spread:
            lines.append(row(c) + f"  spread={yes_spread_cents(c):.1f}¢")

    mono = _monotone_subset(s_surf)
    mono_tickers = {c.ticker for c in mono}
    lines.append(f"--- monotone subset (strict mid decrease with strike↑): n={len(mono)} ---")
    for c in mono:
        lines.append(row(c))
    dropped = [c for c in s_surf if c.ticker not in mono_tickers]
    if dropped:
        lines.append("--- dropped by monotone ---")
        for c in dropped:
            lines.append(row(c))

    mono_2s = _monotone_subset([c for c in mono if not getattr(c, "one_sided", False)])
    lines.append(f"--- after removing one_sided from monotone chain: n={len(mono_2s)} ---")
    for c in mono_2s:
        lines.append(row(c))

    ok_g2, mid_vol, vol_c, book_c = _passes_gate2_liquidity(
        raw_contracts,
        is_weather=use_weather_liquidity_gate,
        is_sports=not use_weather_liquidity_gate,
    )
    if use_weather_liquidity_gate:
        g2_need = "weather-style 1k proxies"
    else:
        g2_need = f"sports book-only (need book_sz≥{int(SPORTS_GATE2_MIN_BOOK_SZ)}; mid×vol/vol_fp ignored)"
    lines.append(
        f"--- gate2 liquidity (full raw ladder): ok={ok_g2}  "
        f"mid×vol=${mid_vol:.0f}  vol_fp={vol_c:.0f}  book_sz={book_c:.0f} ({g2_need}) ---"
    )
    lines.append("--- end sports pipeline debug ---")
    return lines


def run_scan_sports(
    raw_contracts: list[Any],
    *,
    model: ModelKind,
    t_resolve: datetime,
    now: Optional[datetime] = None,
    portfolio_cents: int = 100_000,
    deployed_fraction: float = 0.0,
    nbinom_r_disp: float = 8.0,
    min_informative: int = 4,
    use_weather_liquidity_gate: bool = False,
) -> ScanResult:
    """
    Threshold-ladder vol scan for sports: ≥``min_informative`` informative mids (default 4),
    crypto-style gate2 unless ``use_weather_liquidity_gate`` (stricter MLS-style thin book).

    ``model``: ``normal`` | ``poisson`` | ``negbinom`` (not ``lognormal``).
    """
    now = now or datetime.now(timezone.utc)
    if t_resolve.tzinfo is None:
        t_resolve = t_resolve.replace(tzinfo=timezone.utc)
    sec = max(0.0, (t_resolve - now).total_seconds())
    t_years = years_from_seconds(sec)

    if model == "lognormal":
        return ScanResult(
            False,
            "abort",
            "sports_no_lognormal",
            model=model,
            contracts=raw_contracts,
            gate_log=["sports ladder uses normal/poisson/negbinom only"],
        )

    gate_log: list[str] = []

    ok_g2, mid_vol, vol_c, book_c = _passes_gate2_liquidity(
        raw_contracts,
        is_weather=use_weather_liquidity_gate,
        is_sports=not use_weather_liquidity_gate,
    )
    if not ok_g2:
        if use_weather_liquidity_gate:
            need = "weather-style thin thresholds"
        else:
            need = f"sports book_sz≥{int(SPORTS_GATE2_MIN_BOOK_SZ)} (mid×vol/vol_fp ignored)"
        return ScanResult(
            False,
            "abort",
            "gate2_total_volume",
            model=model,
            contracts=raw_contracts,
            gate_log=[
                f"gate2: mid×vol=${mid_vol:.0f} vol_fp={vol_c:.0f} book_sz={book_c:.0f} ({need})"
            ],
        )

    c0 = _filter_sports_surface_contracts(raw_contracts)
    gate_log.append(
        "gate1_informative "
        f"raw_n={len(raw_contracts)} mid_band_n={len(_filter_informative(raw_contracts))} "
        f"surface_n={len(c0)} (5–95¢ mid & YES spread≤{int(SPORTS_SURFACE_MAX_YES_SPREAD_CENTS)}¢, "
        f"need ≥{min_informative})"
    )
    if len(c0) < min_informative:
        return ScanResult(
            False,
            "abort",
            "gate1_informative_count",
            model=model,
            contracts=raw_contracts,
            gate_log=gate_log + ["gate1 fail"],
        )

    c_sorted = sorted(c0, key=lambda x: x.strike)
    c1 = _monotone_subset(c_sorted)
    if len(c1) < min_informative:
        return ScanResult(
            False,
            "abort",
            "gate6_monotone",
            model=model,
            contracts=c0,
            gate_log=gate_log
            + [f"gate6_monotone failed (monotone subset n={len(c1)} from informative_n={len(c0)})"],
        )
    c1 = _monotone_subset([c for c in c1 if not getattr(c, "one_sided", False)])
    if len(c1) < min_informative:
        return ScanResult(
            False,
            "abort",
            "insufficient_two_sided_anchors",
            model=model,
            contracts=c0,
            gate_log=gate_log + [f"need ≥{min_informative} two-sided strikes after monotone"],
        )

    n = len(c1)
    pairs = build_pair_matrix(c1, model, t_years, nbinom_r_disp=nbinom_r_disp)
    valid_count = sum(1 for p in pairs if p.valid)
    if valid_count < 3:
        return ScanResult(
            False,
            "abort",
            "pair_count",
            model=model,
            contracts=c1,
            pair_debug=[{"i": p.i, "j": p.j, "valid": p.valid, "reason": p.reason} for p in pairs],
            gate_log=gate_log + [f"valid pairs {valid_count} < 3"],
        )

    used_infection = False
    if n >= 4:
        outlier_idx, _, _ = leave_one_out_sigmas(pairs, n)
        if outlier_idx is None:
            outlier_idx = infection_outlier(pairs, n)
            if outlier_idx is not None:
                used_infection = True
                gate_log = gate_log + ["loo_ambiguous_using_infection_outlier"]
        if outlier_idx is None:
            return ScanResult(
                False,
                "abort",
                "ambiguous_surface",
                model=model,
                contracts=c1,
                pair_debug=[{"i": p.i, "j": p.j, "sigma": p.sigma, "v": p.valid} for p in pairs],
                gate_log=gate_log + ["leave-one-out gap < 0.25"],
                weather_market_type="threshold",
                from_range_buckets=False,
            )
    else:
        outlier_idx = infection_outlier(pairs, n)
        if outlier_idx is None:
            return ScanResult(
                False, "abort", "infection_fail", model=model, contracts=c1, gate_log=gate_log
            )

    sig_star = consensus_sigma_star(pairs, outlier_idx)
    if sig_star is None:
        return ScanResult(False, "abort", "no_sigma_star", model=model, contracts=c1, outlier_idx=outlier_idx)

    if not plausibility_sigma(sig_star, model):
        return ScanResult(
            False,
            "abort",
            "plausibility_sigma",
            model=model,
            contracts=c1,
            outlier_idx=outlier_idx,
            sigma_star=sig_star,
            gate_log=gate_log + [f"pair-implied param out of band: {sig_star}"],
        )

    und = consensus_underlying(
        c1, pairs, outlier_idx, model, sig_star, t_years, nbinom_r_disp=nbinom_r_disp
    )
    if und is None:
        return ScanResult(
            False, "abort", "underlying", model=model, outlier_idx=outlier_idx, sigma_star=sig_star
        )

    loo_idx = outlier_idx
    scored: list[tuple[float, float, int, Any, float, Any, int, float]] = []
    for i in range(n):
        c = c1[i]
        if not yes_mid_tradeable(c.mid_cents):
            continue
        if model == "normal":
            p_fair_i = fair_yes_normal(und, c.strike, sig_star)
        elif model == "poisson":
            p_fair_i = fair_yes_poisson(und, c.strike)
        elif model == "negbinom":
            p_fair_i = fair_yes_nbinom(und, nbinom_r_disp, c.strike)
        else:
            continue
        mid_yes = c.mid_cents / 100.0
        side_i = trade_side_from_mid_vs_fair(mid_yes, p_fair_i)
        ent_i = limit_entry_cents(side_i, c)
        if ent_i is None:
            continue
        edge_i = edge_cents_limit_order(side_i, p_fair_i, ent_i)
        if spread_blocks_entry(c):
            continue
        abs_e = abs(edge_i)
        spread_v = float(c.yes_ask_cents - c.yes_bid_cents)
        scored.append((abs_e, spread_v, i, c, p_fair_i, side_i, ent_i, edge_i))

    if not scored:
        return ScanResult(
            False,
            "abort",
            "mid_tail_band",
            model=model,
            contracts=c1,
            outlier_idx=loo_idx,
            sigma_star=sig_star,
            underlying=und,
            gate_log=gate_log + ["no contract in YES-mid trade band after gates"],
            weather_market_type="threshold",
            from_range_buckets=False,
        )

    scored.sort(key=lambda t: (-t[0], t[1]))
    abs_e_pick, _spr, trade_i, out_c, p_fair, side, ent, edge = scored[0]
    outlier_idx = trade_i
    gate_log = gate_log + [
        f"picked_contract idx={trade_i} |edge|={abs_e_pick:.2f}¢ (best in YES-mid band)"
    ]

    anchor_vols = [c1[i].volume_fp for i in range(n) if i != loo_idx]
    low_anchor = any(_volume_dollars(c1[i]) < 1000 for i in range(n) if i != loo_idx)

    if abs(edge) < MIN_EDGE_CENTS:
        return ScanResult(
            True,
            "skip",
            "below_edge_threshold",
            model=model,
            contracts=c1,
            outlier_idx=outlier_idx,
            outlier_ticker=out_c.ticker,
            sigma_star=sig_star,
            underlying=und,
            p_fair_yes=p_fair,
            edge_cents=edge,
            gate_log=gate_log + [f"edge={edge:.2f}¢ below threshold"],
            low_anchor_volume_flag=low_anchor,
            weather_market_type="threshold",
            from_range_buckets=False,
        )

    mkt_yes = yes_implied_prob_for_stability(side, ent)
    if model == "normal":
        stable = edge_stability(mkt_yes, und, out_c.strike, sig_star, t_years, "normal")
    elif model == "poisson":
        stable = edge_stability_poisson(mkt_yes, und, out_c.strike)
    else:
        stable = edge_stability_nbinom(mkt_yes, und, nbinom_r_disp, out_c.strike)

    if not stable:
        return ScanResult(
            False,
            "abort",
            "edge_unstable",
            model=model,
            outlier_idx=outlier_idx,
            sigma_star=sig_star,
            underlying=und,
            edge_cents=edge,
            gate_log=gate_log,
            weather_market_type="threshold",
            from_range_buckets=False,
        )

    entry = ent

    if side == "no":
        p_win = 1.0 - p_fair
    else:
        p_win = p_fair
    entry_prob = entry / 100.0

    fk = kelly_fraction(p_win, entry_prob)
    if fk <= 0:
        return ScanResult(False, "abort", "kelly_nonpositive", edge_cents=edge, gate_log=gate_log)

    f_trade = min(fk / 4.0, 0.05)
    if low_anchor:
        f_trade *= 0.5
    cap_remain = max(0.0, 0.50 - deployed_fraction)
    f_trade = min(f_trade, cap_remain)

    n_contracts = contracts_to_buy(portfolio_cents, f_trade, entry)
    if n_contracts < 1:
        n_contracts = 1
        gate_log = gate_log + [
            "sizing: Kelly implied <1 contract; using minimum 1 "
            f"(portfolio={portfolio_cents}¢ f_trade={f_trade:.4f} entry={entry}¢)"
        ]

    _ = anchor_vols
    _ = used_infection

    return ScanResult(
        True,
        "trade",
        "ok",
        model=model,
        contracts=c1,
        outlier_idx=outlier_idx,
        outlier_ticker=out_c.ticker,
        sigma_star=sig_star,
        underlying=und,
        p_fair_yes=p_fair,
        edge_cents=edge,
        side=side,
        entry_cents=entry,
        contracts_to_buy=n_contracts,
        f_trade=f_trade,
        low_anchor_volume_flag=low_anchor,
        pair_debug=[{"i": p.i, "j": p.j, "sigma": p.sigma, "v": p.valid} for p in pairs],
        weather_market_type="threshold",
        from_range_buckets=False,
        gate_log=gate_log,
    )
