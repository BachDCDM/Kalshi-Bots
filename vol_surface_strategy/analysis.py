"""End-to-end vol surface scan: liquidity gates → outlier → edge → sizing hints."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal, Optional

from vol_surface_strategy.config import (
    RANGE_BUCKET_ADJACENT_MIN_DZ,
    RANGE_BUCKET_DERIVED_MID_MAX_CENTS,
    RANGE_BUCKET_DERIVED_MID_MIN_CENTS,
    RANGE_BUCKET_RAW_MID_MAX_CENTS,
    RANGE_BUCKET_RAW_MID_MIN_CENTS,
    WEATHER_GATE2_MIN_BOOK_SZ,
    WEATHER_GATE2_MIN_MID_VOL,
    WEATHER_GATE2_MIN_VOL_FP,
    climatology_mean_high,
)
from vol_surface_strategy.range_buckets import (
    MarketType,
    convert_range_to_threshold,
    detect_market_type,
    edge_stability_interval,
    integration_extents_f,
    plausibility_bucket_mass,
    reverse_map_to_bucket,
    sort_range_contracts,
)
from vol_surface_strategy.surface_math import (
    ModelKind,
    build_pair_matrix,
    consensus_sigma_star,
    consensus_underlying,
    contracts_to_buy,
    edge_stability,
    fair_yes_lognormal,
    fair_yes_normal,
    infection_outlier,
    kelly_fraction,
    leave_one_out_sigmas,
    plausibility_sigma,
    years_from_seconds,
)

Side = Literal["yes", "no"]


def _filter_informative(contracts: list[Any]) -> list[Any]:
    return [c for c in contracts if 5 <= c.mid_cents <= 95]


def _filter_range_derived_for_surface(derived: list[Any], raw_sorted: list[Any]) -> list[Any]:
    """
    Range ladder: CDF used full raw; surface uses derived rows where
    - marginal bucket mid ∈ [3¢, 97¢], and
    - derived P(T≥K) mid ∈ [3¢, 97¢] (wide band when distribution is collapsed).
    """
    out: list[Any] = []
    for d in derived:
        if not (RANGE_BUCKET_DERIVED_MID_MIN_CENTS <= d.mid_cents <= RANGE_BUCKET_DERIVED_MID_MAX_CENTS):
            continue
        ri = d.source_raw_index
        if ri is None or ri < 0 or ri >= len(raw_sorted):
            continue
        rm = raw_sorted[ri].mid_cents
        if not (RANGE_BUCKET_RAW_MID_MIN_CENTS <= rm <= RANGE_BUCKET_RAW_MID_MAX_CENTS):
            continue
        out.append(d)
    return out


def _monotone_subset(contracts: list[Any]) -> list[Any]:
    """Sort by strike ascending; greedily keep strict mid decrease."""
    s = sorted(contracts, key=lambda x: x.strike)
    out: list[Any] = []
    last_mid: Optional[float] = None
    for c in s:
        if last_mid is None:
            out.append(c)
            last_mid = c.mid_cents
            continue
        if c.mid_cents < last_mid:
            out.append(c)
            last_mid = c.mid_cents
    return out


def _volume_dollars(c: Any) -> float:
    """Rough notional: contracts × mid (Kalshi `volume_fp` is contracts traded)."""
    return (c.volume_fp or 0) * (c.mid_cents / 100.0)


def _volume_contracts(c: Any) -> float:
    try:
        return float(c.volume_fp or 0)
    except (TypeError, ValueError):
        return 0.0


def _passes_gate2_liquidity(contracts: list[Any], *, is_weather: bool = False) -> tuple[bool, float, float, float]:
    """
    Gate 2: pass if any liquidity proxy clears the bar. Crypto uses $5k / 5k contracts / 5k book;
    weather uses $1k thresholds — thin per-city ladders rarely hit 5k.
    """
    mid_vol = sum(_volume_dollars(c) for c in contracts)
    vol_c = sum(_volume_contracts(c) for c in contracts)
    book_c = sum(
        float(getattr(c, "yes_bid_size_fp", 0) or 0) + float(getattr(c, "yes_ask_size_fp", 0) or 0)
        for c in contracts
    )
    if is_weather:
        if (
            mid_vol >= WEATHER_GATE2_MIN_MID_VOL
            or vol_c >= WEATHER_GATE2_MIN_VOL_FP
            or book_c >= WEATHER_GATE2_MIN_BOOK_SZ
        ):
            return True, mid_vol, vol_c, book_c
    elif mid_vol >= 5000 or vol_c >= 5000 or book_c >= 5000:
        return True, mid_vol, vol_c, book_c
    return False, mid_vol, vol_c, book_c


def _marginal_mass_collapse(raw_sorted: list[Any]) -> bool:
    """True if a single bucket holds ≥85% marginal YES mass (near-resolved / not a vol surface)."""
    if not raw_sorted:
        return False
    mx = max((c.mid_cents / 100.0) for c in raw_sorted)
    return mx >= 0.85


def _edge_threshold(
    n: int,
    anchor_volumes: list[float],
    outlier_vol: float,
    used_infection: bool,
) -> float:
    thr = 3.5 if used_infection else 2.5
    if any(v < 10 for v in anchor_volumes):
        thr = max(thr, 4.0)
    if anchor_volumes:
        med = sorted(anchor_volumes)[len(anchor_volumes) // 2]
        if outlier_vol >= med:
            thr = max(thr, 4.0)
    return thr


def _observable_btc_proxy(anchors: list[Any]) -> float:
    best = min(anchors, key=lambda c: abs(c.mid_cents / 100.0 - 0.5))
    return float(best.strike)


@dataclass
class ScanResult:
    ok: bool
    action: str
    reason: str
    model: ModelKind = "normal"
    contracts: list[Any] = field(default_factory=list)
    pair_debug: list[dict] = field(default_factory=list)
    outlier_idx: Optional[int] = None
    outlier_ticker: Optional[str] = None
    sigma_star: Optional[float] = None
    underlying: Optional[float] = None
    p_fair_yes: Optional[float] = None
    edge_cents: Optional[float] = None
    side: Optional[Side] = None
    entry_cents: Optional[int] = None
    contracts_to_buy: int = 0
    f_trade: float = 0.0
    low_anchor_volume_flag: bool = False
    gate_log: list[str] = field(default_factory=list)
    weather_market_type: str = "threshold"
    from_range_buckets: bool = False


def _classify_weather_and_prepare(
    raw: list[Any],
) -> tuple[MarketType, list[Any], bool, Optional[list[Any]], list[str]]:
    """
    Weather classification on the **full** raw ladder (CDF uses every bucket).

    Range buckets: marginal raw mid ∈ [3¢, 97¢]; derived P(T≥K) mids ∈ [3¢, 97¢] for surface rows.
    Threshold / BTC-style markets: native 5–95¢ filter unchanged.

    Returns (market_type, ordered_for_surface, from_range, raw_sorted_full_or_None, extra_logs).
    """
    logs: list[str] = []
    if len(raw) < 3:
        return "unknown", [], False, None, ["gate1_raw_count"]

    unknown_ct = sum(1 for c in raw if c.bucket_mode == "unknown")

    if unknown_ct == len(raw):
        c_sorted = sorted(raw, key=lambda x: x.strike)
        mids = [c.mid_cents for c in c_sorted]
        mt = detect_market_type(mids)
        if mt == "range_bucket":
            return "unknown", [], False, None, ["range_shape_but_no_bucket_parse"]
        if mt == "unknown":
            return "unknown", [], False, None, ["market_type_unknown"]
        inf = _filter_informative(c_sorted)
        if len(inf) < 3:
            return "unknown", [], False, None, ["gate1_informative_count"]
        return mt, inf, False, None, logs

    if unknown_ct > 0:
        return "unknown", [], False, None, ["mixed_bucket_metadata"]

    c_sorted = sort_range_contracts(raw)
    mids = [c.mid_cents for c in c_sorted]
    mt = detect_market_type(mids)
    if mt == "unknown":
        # LOW / illiquid ladders: bucket mids need not look unimodal; CDF sum is authoritative.
        conv_shape = convert_range_to_threshold(c_sorted)
        if conv_shape is not None:
            derived, raw_map = conv_shape
            derived_f = _filter_range_derived_for_surface(derived, raw_map)
            if len(derived_f) >= 3:
                logs.append("range_bucket_cdf_ok_mid_shape_unknown")
                return "range_bucket", derived_f, True, raw_map, logs
            if _marginal_mass_collapse(raw_map):
                return "unknown", [], False, None, ["distribution_collapsed"]
        return "unknown", [], False, None, ["market_type_unknown"]

    conv_result: Optional[tuple[list[Any], list[Any]]] = None
    if mt == "range_bucket":
        conv_result = convert_range_to_threshold(c_sorted)
        if conv_result is not None:
            derived, raw_map = conv_result
            derived_f = _filter_range_derived_for_surface(derived, raw_map)
            if len(derived_f) >= 3:
                logs.append("range_bucket_cdf_ok")
                return mt, derived_f, True, raw_map, logs
            if _marginal_mass_collapse(raw_map):
                return "unknown", [], False, None, ["distribution_collapsed"]

    if mt == "threshold":
        inf = _filter_informative(c_sorted)
        if len(inf) < 3:
            return "unknown", [], False, None, ["gate1_informative_count"]
        return mt, inf, False, None, logs

    # range_bucket with invalid CDF or too few derived rows: sparse / illiquid ladders often
    # still support a 3+ strike monotone threshold subset.
    inf = _filter_informative(c_sorted)
    mono = _monotone_subset(sorted(inf, key=lambda x: x.strike))
    if len(mono) >= 3 and detect_market_type([c.mid_cents for c in mono]) == "threshold":
        logs.append("sparse_ladder_threshold_fallback")
        return "threshold", mono, False, None, logs

    if mt == "range_bucket":
        if conv_result is None:
            return "unknown", [], False, None, ["cdf_sum_outside_0.95_1.05"]
        return "unknown", [], False, None, ["gate1_derived_informative_count"]

    return "unknown", [], False, None, ["market_type_unknown"]


def run_scan(
    raw_contracts: list[Any],
    *,
    model: ModelKind,
    t_resolve: datetime,
    now: Optional[datetime] = None,
    city_id: Optional[str] = None,
    portfolio_cents: int = 100_000,
    deployed_fraction: float = 0.0,
) -> ScanResult:
    now = now or datetime.now(timezone.utc)
    if t_resolve.tzinfo is None:
        t_resolve = t_resolve.replace(tzinfo=timezone.utc)
    sec = max(0.0, (t_resolve - now).total_seconds())
    t_years = years_from_seconds(sec)

    gate_log: list[str] = []
    from_range = False
    raw_sorted_map: Optional[list[Any]] = None
    weather_mt: MarketType = "threshold"

    if model == "lognormal":
        c0 = _filter_informative(raw_contracts)
        if len(c0) < 3:
            return ScanResult(False, "abort", "gate1_informative_count", model=model, gate_log=["gate1 fail"])
        # Liquidity: use full ladder (all strikes), not only 5–95¢ mids — tails dominate matched vol.
        ok_g2, mid_vol, vol_c, book_c = _passes_gate2_liquidity(raw_contracts, is_weather=False)
        if not ok_g2:
            return ScanResult(
                False,
                "abort",
                "gate2_total_volume",
                model=model,
                contracts=c0,
                gate_log=[
                    f"gate2: mid×vol=${mid_vol:.0f} vol_fp={vol_c:.0f} book_sz={book_c:.0f} (need ≥5000 in any)"
                ],
            )
        c_sorted = sorted(c0, key=lambda x: x.strike)
        c1 = _monotone_subset(c_sorted)
        if len(c1) < 3:
            return ScanResult(
                False,
                "abort",
                "gate6_monotone",
                model=model,
                contracts=c0,
                gate_log=gate_log,
            )
        c1 = _monotone_subset([c for c in c1 if not getattr(c, "one_sided", False)])
        if len(c1) < 3:
            return ScanResult(
                False,
                "abort",
                "insufficient_two_sided_anchors",
                model=model,
                contracts=c0,
                gate_log=gate_log + ["need ≥3 strikes with two-sided YES books for σ pairs"],
            )
        weather_mt = "threshold"
    else:
        if len(raw_contracts) < 3:
            return ScanResult(
                False,
                "abort",
                "gate1_raw_count",
                model=model,
                contracts=raw_contracts,
                gate_log=["need >= 3 raw contracts"],
            )
        ok_g2, mid_vol, vol_c, book_c = _passes_gate2_liquidity(raw_contracts, is_weather=True)
        if not ok_g2:
            return ScanResult(
                False,
                "abort",
                "gate2_total_volume",
                model=model,
                contracts=raw_contracts,
                gate_log=[
                    f"gate2: mid×vol=${mid_vol:.0f} vol_fp={vol_c:.0f} book_sz={book_c:.0f} "
                    f"(weather: need ≥{WEATHER_GATE2_MIN_MID_VOL:.0f} in any proxy)"
                ],
            )
        weather_mt, ordered, from_range, raw_sorted_map, cls_logs = _classify_weather_and_prepare(
            raw_contracts
        )
        gate_log.extend(cls_logs)
        if weather_mt == "unknown":
            cls_reason = (
                "distribution_collapsed"
                if any(x == "distribution_collapsed" for x in cls_logs)
                else "market_classification"
            )
            return ScanResult(
                False,
                "abort",
                cls_reason,
                model=model,
                contracts=raw_contracts,
                gate_log=gate_log,
            )
        c1 = _monotone_subset(ordered)
        if len(c1) < 3:
            return ScanResult(
                False,
                "abort",
                "gate6_monotone_derived" if from_range else "gate6_monotone",
                model=model,
                contracts=raw_contracts,
                gate_log=gate_log,
            )
        c1 = _monotone_subset([c for c in c1 if not getattr(c, "one_sided", False)])
        if len(c1) < 3:
            return ScanResult(
                False,
                "abort",
                "insufficient_two_sided_anchors",
                model=model,
                contracts=raw_contracts,
                gate_log=gate_log + ["need ≥3 strikes with two-sided YES books for σ pairs"],
            )

    n = len(c1)
    adjacent_dz = RANGE_BUCKET_ADJACENT_MIN_DZ if from_range else None
    pairs = build_pair_matrix(c1, model, t_years, adjacent_min_dz=adjacent_dz)
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

    used_infection = n == 3
    if n >= 4:
        outlier_idx, scores, _ = leave_one_out_sigmas(pairs, n)
        if outlier_idx is None:
            return ScanResult(
                False,
                "abort",
                "ambiguous_surface",
                model=model,
                contracts=c1,
                pair_debug=[{"i": p.i, "j": p.j, "sigma": p.sigma, "v": p.valid} for p in pairs],
                gate_log=gate_log + ["leave-one-out gap < 0.25"],
                weather_market_type=weather_mt,
                from_range_buckets=from_range,
            )
    else:
        outlier_idx = infection_outlier(pairs, n)
        if outlier_idx is None:
            return ScanResult(False, "abort", "infection_fail", model=model, contracts=c1, gate_log=gate_log)

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
            gate_log=gate_log + [f"sigma* out of band: {sig_star}"],
        )

    und = consensus_underlying(c1, pairs, outlier_idx, model, sig_star, t_years)
    if und is None:
        return ScanResult(False, "abort", "underlying", model=model, outlier_idx=outlier_idx, sigma_star=sig_star)

    if model == "normal" and city_id:
        mo = t_resolve.astimezone().month
        hm = climatology_mean_high(city_id, mo)
        if not (hm - 30 <= und <= hm + 30):
            return ScanResult(
                False,
                "abort",
                "plausibility_mu",
                model=model,
                sigma_star=sig_star,
                underlying=und,
                gate_log=gate_log + [f"mu*={und:.1f} vs climatology {hm}"],
            )

    if model == "lognormal":
        anchors = [c for i, c in enumerate(c1) if i != outlier_idx]
        obs = _observable_btc_proxy(anchors)
        if obs > 0 and abs(und - obs) / obs >= 0.05:
            return ScanResult(
                False,
                "abort",
                "plausibility_S",
                model=model,
                sigma_star=sig_star,
                underlying=und,
                gate_log=gate_log + [f"S*={und:.0f} vs atm_proxy={obs:.0f}"],
            )

    if from_range and raw_sorted_map is not None:
        mass = plausibility_bucket_mass(und, sig_star, raw_sorted_map)
        if not (0.97 <= mass <= 1.03):
            return ScanResult(
                False,
                "abort",
                "plausibility_bucket_mass",
                model=model,
                sigma_star=sig_star,
                underlying=und,
                contracts=c1,
                outlier_idx=outlier_idx,
                gate_log=gate_log + [f"bucket_mass={mass:.4f}"],
                weather_market_type=weather_mt,
                from_range_buckets=True,
            )
        out_raw = c1[outlier_idx].source_raw_index
        src_idx = out_raw if out_raw is not None else outlier_idx
        try:
            real_c, p_fair_b, edge = reverse_map_to_bucket(
                src_idx, und, sig_star, raw_sorted_map
            )
        except ValueError:
            return ScanResult(
                False,
                "abort",
                "reverse_map_fail",
                model=model,
                gate_log=gate_log,
                weather_market_type=weather_mt,
                from_range_buckets=True,
            )

        anchor_vols = [
            raw_sorted_map[i].volume_fp for i in range(len(raw_sorted_map)) if i != src_idx
        ]
        low_anchor = any(
            _volume_dollars(raw_sorted_map[i]) < 1000
            for i in range(len(raw_sorted_map))
            if i != src_idx
        )
        thr = _edge_threshold(n, anchor_vols, real_c.volume_fp, used_infection)
        if abs(edge) < thr:
            return ScanResult(
                True,
                "skip",
                "below_edge_threshold",
                model=model,
                contracts=c1,
                outlier_idx=outlier_idx,
                outlier_ticker=real_c.ticker,
                sigma_star=sig_star,
                underlying=und,
                p_fair_yes=p_fair_b,
                edge_cents=edge,
                gate_log=gate_log + [f"|edge|={abs(edge):.2f} < {thr} (bucket)"],
                low_anchor_volume_flag=low_anchor,
                weather_market_type=weather_mt,
                from_range_buckets=True,
            )

        lo_i, hi_i = integration_extents_f(real_c)
        mid_yes = real_c.mid_cents / 100.0
        if not edge_stability_interval(mid_yes, und, sig_star, lo_i, hi_i):
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
                weather_market_type=weather_mt,
                from_range_buckets=True,
            )

        spread = real_c.yes_ask_cents - real_c.yes_bid_cents
        if spread > 6:
            return ScanResult(
                False,
                "abort",
                "gate5_spread",
                gate_log=gate_log + [f"spread={spread}"],
                weather_market_type=weather_mt,
                from_range_buckets=True,
            )

        out_c = real_c
        p_fair = p_fair_b
    else:
        out_c = c1[outlier_idx]
        if model == "lognormal":
            p_fair = fair_yes_lognormal(und, out_c.strike, sig_star, t_years)
        else:
            p_fair = fair_yes_normal(und, out_c.strike, sig_star)
        mid_yes = out_c.mid_cents / 100.0
        edge = (mid_yes - p_fair) * 100.0

        anchor_vols = [c1[i].volume_fp for i in range(n) if i != outlier_idx]
        low_anchor = any(_volume_dollars(c1[i]) < 1000 for i in range(n) if i != outlier_idx)
        thr = _edge_threshold(n, anchor_vols, out_c.volume_fp, used_infection)
        if abs(edge) < thr:
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
                gate_log=gate_log + [f"|edge|={abs(edge):.2f} < {thr}"],
                low_anchor_volume_flag=low_anchor,
                weather_market_type=weather_mt,
                from_range_buckets=from_range,
            )

        if not edge_stability(mid_yes, und, out_c.strike, sig_star, t_years, model):
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
                weather_market_type=weather_mt,
                from_range_buckets=from_range,
            )

        spread = out_c.yes_ask_cents - out_c.yes_bid_cents
        if spread > 6:
            return ScanResult(False, "abort", "gate5_spread", gate_log=gate_log + [f"spread={spread}"])

    if edge > 0:
        side: Side = "no"
        best_no_ask = 100.0 - out_c.yes_bid_cents
        if out_c.yes_bid_cents <= 0:
            return ScanResult(False, "abort", "gate4_no_ask", gate_log=gate_log)
        entry = int(max(2, min(98, round(best_no_ask - 1))))
        p_win = 1.0 - p_fair
        entry_prob = entry / 100.0
    else:
        side = "yes"
        if out_c.yes_ask_cents <= 0:
            return ScanResult(False, "abort", "gate4_yes_ask", gate_log=gate_log)
        entry = int(max(2, min(98, round(out_c.yes_ask_cents - 1))))
        p_win = p_fair
        entry_prob = entry / 100.0

    fk = kelly_fraction(p_win, entry_prob)
    if fk <= 0:
        return ScanResult(False, "abort", "kelly_nonpositive", edge_cents=edge, gate_log=gate_log)

    f_trade = min(fk / 4.0, 0.05)
    if low_anchor:
        f_trade *= 0.5
    if from_range:
        f_trade *= 0.7
    cap_remain = max(0.0, 0.50 - deployed_fraction)
    f_trade = min(f_trade, cap_remain)

    n_contracts = contracts_to_buy(portfolio_cents, f_trade, entry)
    if n_contracts < 1:
        return ScanResult(
            False,
            "abort",
            "zero_contracts",
            side=side,
            entry_cents=entry,
            f_trade=f_trade,
            gate_log=gate_log,
        )

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
        weather_market_type=weather_mt,
        from_range_buckets=from_range,
    )
