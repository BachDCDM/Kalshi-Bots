"""End-to-end vol surface scan: liquidity gates → outlier → edge → sizing hints."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal, Optional

from vol_surface_strategy.config import (
    MIN_EDGE_CENTS,
    RANGE_BUCKET_ADJACENT_MIN_DZ,
    RANGE_BUCKET_DERIVED_MID_MAX_CENTS,
    RANGE_BUCKET_DERIVED_MID_MIN_CENTS,
    RANGE_BUCKET_RAW_MID_MAX_CENTS,
    RANGE_BUCKET_RAW_MID_MIN_CENTS,
    TRADE_YES_MID_MAX_CENTS,
    TRADE_YES_MID_MIN_CENTS,
    WEATHER_GATE2_MIN_BOOK_SZ,
    WEATHER_GATE2_MIN_MID_VOL,
    WEATHER_GATE2_MIN_VOL_FP,
    climatology_mean_high,
    climatology_mean_low,
    yes_mid_tradeable,
)
from vol_surface_strategy.entry_edge import (
    edge_cents_limit_order,
    limit_entry_cents,
    spread_blocks_entry,
    trade_side_from_mid_vs_fair,
    yes_implied_prob_for_stability,
    yes_spread_cents,
)
from vol_surface_strategy.range_buckets import (
    MarketType,
    best_btc_bucket_trade_by_edge,
    convert_btc_range_to_threshold,
    convert_range_to_threshold,
    detect_market_type,
    edge_stability_btc_interval,
    edge_stability_interval,
    integration_extents_f,
    marginal_partition_mass,
    marginal_partition_mass_btc,
    plausibility_btc_bucket_mass,
    plausibility_bucket_mass,
    reverse_map_to_bucket,
    sort_btc_range_contracts,
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
WeatherTempKind = Literal["HIGH", "LOW"]


def _filter_informative(contracts: list[Any]) -> list[Any]:
    return [c for c in contracts if 5 <= c.mid_cents <= 95]


def _filter_range_derived_for_surface(derived: list[Any], raw_sorted: list[Any]) -> list[Any]:
    """
    Range ladder: CDF used full raw; surface rows use derived P(T≥K) mids in band.
    Raw tail buckets can sit at ~0.5¢ marginal — still informative for CDF — so raw mid
    floor is 0.5¢ (not 3¢) as long as derived mid is in range.
    """
    out: list[Any] = []
    for d in derived:
        if not (RANGE_BUCKET_DERIVED_MID_MIN_CENTS <= d.mid_cents <= RANGE_BUCKET_DERIVED_MID_MAX_CENTS):
            continue
        ri = d.source_raw_index
        if ri is None or ri < 0 or ri >= len(raw_sorted):
            continue
        rm = raw_sorted[ri].mid_cents
        if not (0.5 <= rm <= RANGE_BUCKET_RAW_MID_MAX_CENTS):
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


def btc_pipeline_debug_lines(raw_contracts: list[Any]) -> list[str]:
    """
    Printable ladder / gate breakdown for BTC (lognormal) debugging.
    Does not mutate contracts.
    """
    lines: list[str] = []
    n = len(raw_contracts)
    lines.append(f"--- BTC pipeline debug: raw parsable contracts n={n} ---")
    if n == 0:
        return lines

    def row(c: Any) -> str:
        os1 = getattr(c, "one_sided", False)
        return (
            f"  {c.ticker[:48]:<48} K={c.strike:>12.2f}  "
            f"yb={c.yes_bid_cents:>5.1f} ya={c.yes_ask_cents:>5.1f}  "
            f"mid={c.mid_cents:>5.2f}¢  one_sided={os1!s:5}  vol_fp={getattr(c, 'volume_fp', 0)}"
        )

    lines.append("(all raw, strike-sorted)")
    s_all = sorted(raw_contracts, key=lambda x: x.strike)
    if n <= 30:
        for c in s_all:
            lines.append(row(c))
    else:
        for c in s_all[:25]:
            lines.append(row(c))
        lines.append(f"  ... ({n - 30} contracts omitted) ...")
        for c in s_all[-5:]:
            lines.append(row(c))

    inf = _filter_informative(raw_contracts)
    lines.append(f"--- informative 5–95¢ mid: n={len(inf)} ---")
    s_inf = sorted(inf, key=lambda x: x.strike)
    for c in s_inf:
        lines.append(row(c))

    mono = _monotone_subset(s_inf)
    mono_tickers = {c.ticker for c in mono}
    lines.append(f"--- monotone subset (strict mid decrease with strike↑): n={len(mono)} ---")
    for c in mono:
        lines.append(row(c))
    dropped = [c for c in s_inf if c.ticker not in mono_tickers]
    if dropped:
        lines.append(f"--- dropped by monotone (n={len(dropped)}): mid did not fall vs running max ---")
        for c in dropped:
            lines.append(row(c))

    mono_2s = _monotone_subset([c for c in mono if not getattr(c, "one_sided", False)])
    lines.append(f"--- after removing one_sided from monotone chain: n={len(mono_2s)} ---")
    for c in mono_2s:
        lines.append(row(c))

    ok_g2, mid_vol, vol_c, book_c = _passes_gate2_liquidity(raw_contracts, is_weather=False)
    lines.append(
        f"--- gate2 liquidity (full raw ladder): ok={ok_g2}  "
        f"mid×vol=${mid_vol:.0f}  vol_fp={vol_c:.0f}  book_sz={book_c:.0f} ---"
    )
    lines.append("--- end BTC pipeline debug ---")
    return lines


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
        mp = marginal_partition_mass(c_sorted)
        if mp < 0.85:
            return "unknown", [], False, None, ["distribution_collapsed"]
        if mp > 1.15:
            return "unknown", [], False, None, ["incoherent_bucket_mids"]
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
            mp = marginal_partition_mass(c_sorted)
            if mp < 0.85:
                return "unknown", [], False, None, ["distribution_collapsed"]
            if mp > 1.15:
                return "unknown", [], False, None, ["incoherent_bucket_mids"]
            return "unknown", [], False, None, ["cdf_sum_outside_0.95_1.05"]
        return "unknown", [], False, None, ["gate1_derived_informative_count"]

    return "unknown", [], False, None, ["market_type_unknown"]


def _classify_btc_range_buckets_and_prepare(
    raw: list[Any],
) -> tuple[MarketType, list[Any], bool, Optional[list[Any]], list[str]]:
    """
    BTC hourly $100 bands: marginal mids → synthetic cumulative thresholds (lognormal surface).
    """
    logs: list[str] = []
    if len(raw) < 3:
        return "unknown", [], False, None, ["btc_range_count"]
    bad = sum(
        1
        for c in raw
        if c.bucket_mode != "range" or c.bucket_low is None or c.bucket_high is None
    )
    if bad:
        return "unknown", [], False, None, ["btc_range_metadata_incomplete"]
    c_sorted = sort_btc_range_contracts(raw)
    mids = [c.mid_cents for c in c_sorted]
    mt = detect_market_type(mids)
    conv = convert_btc_range_to_threshold(c_sorted)
    if conv is None:
        mp = marginal_partition_mass_btc(c_sorted)
        if mp < 0.40:
            return "unknown", [], False, None, ["btc_distribution_collapsed"]
        if mp > 3.0:
            return "unknown", [], False, None, ["btc_incoherent_bucket_mids"]
        return "unknown", [], False, None, ["btc_cdf_sum_invalid"]
    derived, raw_map = conv
    derived_f = _filter_range_derived_for_surface(derived, raw_map)
    if len(derived_f) < 3:
        return "unknown", [], False, None, ["btc_gate1_derived_informative_count"]
    if mt == "range_bucket":
        logs.append("btc_range_bucket_cdf_ok")
    elif mt == "unknown":
        logs.append("btc_range_bucket_cdf_ok_mid_shape_unknown")
    else:
        logs.append("btc_range_bucket_cdf_ok_marginal_shape")
    return "range_bucket", derived_f, True, raw_map, logs


def _validate_btc_real_bucket_trade(
    real: Any,
    entry: int,
    side: Side,
    *,
    synthetic_tickers: set[str],
) -> None:
    """Hard checks: never trade synthetic CDF rows; entry from real YES/NO book only."""
    tk = str(getattr(real, "ticker", "") or "")
    assert tk.startswith("KXBTC"), f"BTC trade must be KXBTC*, got {tk!r}"
    assert "-B" in tk.upper(), f"BTC trade must be a B-style bucket contract, got {tk!r}"
    assert not tk.startswith("derived|"), f"BTC trade cannot be synthetic derived ticker {tk!r}"
    assert tk not in synthetic_tickers, f"BTC trade ticker must not be synthetic set {tk!r}"
    yb = float(real.yes_bid_cents)
    ya = float(real.yes_ask_cents)
    if side == "yes":
        exp = int(max(2, min(98, round(ya - 1))))
        assert abs(entry - exp) <= 1, (
            f"YES entry {entry}¢ must match real book (yes_ask−1 ≈ {exp}), not synthetic mid"
        )
    else:
        best_no_ask = 100.0 - yb
        exp = int(max(2, min(98, round(best_no_ask - 1))))
        assert abs(entry - exp) <= 1, (
            f"NO entry {entry}¢ must match real book (derived from YES bid), expected ≈ {exp}"
        )


def run_scan(
    raw_contracts: list[Any],
    *,
    model: ModelKind,
    t_resolve: datetime,
    now: Optional[datetime] = None,
    city_id: Optional[str] = None,
    weather_temp_kind: Optional[WeatherTempKind] = None,
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
    btc_range_cdf_trade = False

    if model == "lognormal":
        btc_b_lines = [
            c
            for c in raw_contracts
            if c.bucket_mode == "range"
            and c.bucket_low is not None
            and c.bucket_high is not None
            and "-B" in (c.ticker or "").upper()
        ]
        try_btc_cdf = len(btc_b_lines) >= 8

        ok_g2, mid_vol, vol_c, book_c = _passes_gate2_liquidity(raw_contracts, is_weather=False)
        if not ok_g2:
            return ScanResult(
                False,
                "abort",
                "gate2_total_volume",
                model=model,
                contracts=raw_contracts,
                gate_log=gate_log
                + [
                    f"gate2: mid×vol=${mid_vol:.0f} vol_fp={vol_c:.0f} book_sz={book_c:.0f} (need ≥5000 in any)"
                ],
            )

        if try_btc_cdf:
            wmt, ordered_b, _fr, raw_map_b, cls_b = _classify_btc_range_buckets_and_prepare(btc_b_lines)
            if wmt != "unknown":
                gate_log.extend(cls_b)
                gate_log.append(
                    f"btc_cdf_path B_buckets_n={len(btc_b_lines)} derived_surface_n={len(ordered_b)}"
                )
                c1_try = _monotone_subset(sorted(ordered_b, key=lambda x: x.strike))
                if len(c1_try) >= 3:
                    c1_try = _monotone_subset(
                        [c for c in c1_try if not getattr(c, "one_sided", False)]
                    )
                if len(c1_try) >= 3:
                    c1 = c1_try
                    from_range = True
                    raw_sorted_map = raw_map_b
                    weather_mt = wmt
                    btc_range_cdf_trade = True
                else:
                    gate_log.extend(
                        cls_b
                        + [
                            f"btc_cdf_monotone_fail derived_mono_n={len(c1_try)} → fallback raw ladder",
                        ]
                    )
            else:
                gate_log.extend(cls_b + ["btc_cdf_classify_fail_fallback_raw_ladder"])

        if not btc_range_cdf_trade:
            c0 = _filter_informative(raw_contracts)
            gate_log = gate_log + [
                f"gate1_informative raw_n={len(raw_contracts)} informative_n={len(c0)} (5–95¢ mid)"
            ]
            if len(c0) < 3:
                return ScanResult(
                    False,
                    "abort",
                    "gate1_informative_count",
                    model=model,
                    gate_log=gate_log + ["gate1 fail"],
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
                    gate_log=gate_log
                    + [
                        f"gate6_monotone failed after gate1 (monotone subset n={len(c1)} from informative_n={len(c0)})"
                    ],
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
            if any(x == "distribution_collapsed" for x in cls_logs):
                cls_reason = "distribution_collapsed"
            elif any(x == "incoherent_bucket_mids" for x in cls_logs):
                cls_reason = "incoherent_bucket_mids"
            else:
                cls_reason = "market_classification"
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
            if btc_range_cdf_trade and n >= 10:
                outlier_idx = infection_outlier(pairs, n)
                if outlier_idx is not None:
                    used_infection = True
                    gate_log = gate_log + ["btc_cdf_loo_ambiguous_using_infection_outlier"]
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
        if weather_temp_kind == "LOW":
            cref = climatology_mean_low(city_id, mo)
            clab = "climatology_low"
            lo, hi = cref - 12.0, cref + 30.0
        else:
            cref = climatology_mean_high(city_id, mo)
            clab = "climatology_high"
            lo, hi = cref - 30.0, cref + 30.0
        if not (lo <= und <= hi):
            return ScanResult(
                False,
                "abort",
                "plausibility_mu",
                model=model,
                sigma_star=sig_star,
                underlying=und,
                gate_log=gate_log + [f"mu*={und:.1f} vs {clab} {cref:.1f} (allowed {lo:.1f}–{hi:.1f})"],
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

    if btc_range_cdf_trade and raw_sorted_map is not None:
        rng_buckets = [
            c
            for c in raw_sorted_map
            if c.bucket_mode == "range"
            and c.bucket_low is not None
            and c.bucket_high is not None
        ]
        if rng_buckets:
            atm = max(rng_buckets, key=lambda c: float(c.mid_cents))
            lo_atm = float(atm.bucket_low)  # type: ignore[arg-type]
            hi_atm = float(atm.bucket_high)  # type: ignore[arg-type]
            center_atm = (lo_atm + hi_atm) / 2.0
            bucket_w = max(hi_atm - lo_atm, 1.0)
            max_dev = 2.0 * bucket_w
            dev = abs(float(und) - center_atm)
            if dev > max_dev:
                return ScanResult(
                    False,
                    "abort",
                    "implied_S_far_from_ATM",
                    model=model,
                    sigma_star=sig_star,
                    underlying=und,
                    contracts=c1,
                    outlier_idx=outlier_idx,
                    gate_log=gate_log
                    + [
                        f"S*={und:.2f} vs atm_bucket_center={center_atm:.2f} ({atm.ticker}) "
                        f"diff={dev:.2f} > 2*bucket_width={max_dev:.2f}"
                    ],
                    weather_market_type=weather_mt,
                    from_range_buckets=True,
                )

        mass = plausibility_btc_bucket_mass(und, sig_star, t_years, raw_sorted_map)
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
                gate_log=gate_log + [f"btc_bucket_mass={mass:.4f}"],
                weather_market_type=weather_mt,
                from_range_buckets=True,
            )
        try:
            real_c, p_fair_b, edge, side_btc, entry_btc = best_btc_bucket_trade_by_edge(
                und, sig_star, t_years, raw_sorted_map
            )
        except ValueError as e:
            if "YES-mid" in str(e) or "trade band" in str(e):
                return ScanResult(
                    False,
                    "abort",
                    "mid_tail_band",
                    model=model,
                    contracts=c1,
                    outlier_idx=outlier_idx,
                    sigma_star=sig_star,
                    underlying=und,
                    gate_log=gate_log
                    + [
                        f"BTC range buckets: none in YES-mid band "
                        f"({TRADE_YES_MID_MIN_CENTS:.0f}–{TRADE_YES_MID_MAX_CENTS:.0f}¢)"
                    ],
                    weather_market_type=weather_mt,
                    from_range_buckets=True,
                )
            return ScanResult(
                False,
                "abort",
                "btc_bucket_edge_fail",
                model=model,
                gate_log=gate_log,
                weather_market_type=weather_mt,
                from_range_buckets=True,
            )

        anchor_vols = [c.volume_fp for c in raw_sorted_map if c.ticker != real_c.ticker]
        low_anchor = any(
            _volume_dollars(c) < 1000 for c in raw_sorted_map if c.ticker != real_c.ticker
        )
        if spread_blocks_entry(real_c):
            sy = yes_spread_cents(real_c)
            return ScanResult(
                False,
                "abort",
                "spread_too_wide",
                model=model,
                contracts=c1,
                outlier_idx=outlier_idx,
                outlier_ticker=real_c.ticker,
                sigma_star=sig_star,
                underlying=und,
                p_fair_yes=p_fair_b,
                edge_cents=edge,
                gate_log=gate_log
                + [f"spread_too_wide spread={sy:.0f}¢ on {real_c.ticker}"],
                low_anchor_volume_flag=low_anchor,
                weather_market_type=weather_mt,
                from_range_buckets=True,
            )

        if abs(edge) < MIN_EDGE_CENTS:
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
                gate_log=gate_log
                + [f"edge={edge:.2f}¢ below 5¢ threshold"],
                low_anchor_volume_flag=low_anchor,
                weather_market_type=weather_mt,
                from_range_buckets=True,
            )

        if real_c.bucket_low is None or real_c.bucket_high is None:
            return ScanResult(
                False,
                "abort",
                "btc_bucket_bounds",
                model=model,
                gate_log=gate_log + ["real bucket missing bucket_low/bucket_high"],
                weather_market_type=weather_mt,
                from_range_buckets=True,
            )
        k_lo, k_hi = float(real_c.bucket_low), float(real_c.bucket_high)
        mkt_yes = yes_implied_prob_for_stability(side_btc, entry_btc)
        if not edge_stability_btc_interval(
            mkt_yes, und, sig_star, t_years, k_lo, k_hi
        ):
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

        out_c = real_c
        p_fair = p_fair_b
        side = side_btc
        entry = entry_btc
    elif from_range and raw_sorted_map is not None:
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
            mapped = reverse_map_to_bucket(src_idx, und, sig_star, raw_sorted_map)
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
        if mapped is None:
            return ScanResult(
                False,
                "abort",
                "mid_tail_band",
                model=model,
                contracts=c1,
                outlier_idx=outlier_idx,
                sigma_star=sig_star,
                underlying=und,
                gate_log=gate_log
                + [
                    f"range neighbors outside YES-mid band "
                    f"({TRADE_YES_MID_MIN_CENTS:.0f}–{TRADE_YES_MID_MAX_CENTS:.0f}¢)"
                ],
                weather_market_type=weather_mt,
                from_range_buckets=True,
            )
        real_c, p_fair_b, edge, side_r, entry_r = mapped

        anchor_vols = [
            raw_sorted_map[i].volume_fp for i in range(len(raw_sorted_map)) if i != src_idx
        ]
        low_anchor = any(
            _volume_dollars(raw_sorted_map[i]) < 1000
            for i in range(len(raw_sorted_map))
            if i != src_idx
        )
        if spread_blocks_entry(real_c):
            sy = yes_spread_cents(real_c)
            return ScanResult(
                False,
                "abort",
                "spread_too_wide",
                model=model,
                contracts=c1,
                outlier_idx=outlier_idx,
                outlier_ticker=real_c.ticker,
                sigma_star=sig_star,
                underlying=und,
                p_fair_yes=p_fair_b,
                edge_cents=edge,
                gate_log=gate_log
                + [f"spread_too_wide spread={sy:.0f}¢ on {real_c.ticker}"],
                low_anchor_volume_flag=low_anchor,
                weather_market_type=weather_mt,
                from_range_buckets=True,
            )

        if abs(edge) < MIN_EDGE_CENTS:
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
                gate_log=gate_log + [f"edge={edge:.2f}¢ below 5¢ threshold"],
                low_anchor_volume_flag=low_anchor,
                weather_market_type=weather_mt,
                from_range_buckets=True,
            )

        lo_i, hi_i = integration_extents_f(real_c)
        mkt_yes = yes_implied_prob_for_stability(side_r, entry_r)
        if not edge_stability_interval(mkt_yes, und, sig_star, lo_i, hi_i):
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

        out_c = real_c
        p_fair = p_fair_b
        side = side_r
        entry = entry_r
    else:
        # Threshold path: among contracts with YES mid in (tail) band, pick largest |edge|, then gates.
        loo_idx = outlier_idx
        scored: list[tuple[float, float, int, Any, float, Side, int, float]] = []
        for i in range(n):
            c = c1[i]
            if not yes_mid_tradeable(c.mid_cents):
                continue
            if model == "lognormal":
                p_fair_i = fair_yes_lognormal(und, c.strike, sig_star, t_years)
            else:
                p_fair_i = fair_yes_normal(und, c.strike, sig_star)
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
                gate_log=gate_log
                + [
                    f"no threshold contract in YES-mid band "
                    f"({TRADE_YES_MID_MIN_CENTS:.0f}–{TRADE_YES_MID_MAX_CENTS:.0f}¢)"
                ],
                weather_market_type=weather_mt,
                from_range_buckets=from_range,
            )
        scored.sort(key=lambda t: (-t[0], t[1]))
        abs_e_pick, _spr, trade_i, out_c, p_fair, side, ent, edge = scored[0]
        outlier_idx = trade_i
        gate_log = gate_log + [
            f"picked_contract idx={trade_i} |edge|={abs_e_pick:.2f}¢ "
            f"(best among YES-mid∈[{TRADE_YES_MID_MIN_CENTS:.0f},{TRADE_YES_MID_MAX_CENTS:.0f}]¢)"
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
                gate_log=gate_log + [f"edge={edge:.2f}¢ below 5¢ threshold"],
                low_anchor_volume_flag=low_anchor,
                weather_market_type=weather_mt,
                from_range_buckets=from_range,
            )

        mkt_yes = yes_implied_prob_for_stability(side, ent)
        if not edge_stability(mkt_yes, und, out_c.strike, sig_star, t_years, model):
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

        entry = ent

    if side == "no":
        p_win = 1.0 - p_fair
    else:
        p_win = p_fair
    entry_prob = entry / 100.0

    if btc_range_cdf_trade:
        try:
            _validate_btc_real_bucket_trade(
                out_c,
                entry,
                side,
                synthetic_tickers={str(x.ticker) for x in c1 if str(x.ticker).startswith("derived|")},
            )
        except AssertionError as err:
            return ScanResult(
                False,
                "abort",
                "btc_trade_validation",
                model=model,
                contracts=c1,
                outlier_idx=outlier_idx,
                sigma_star=sig_star,
                underlying=und,
                edge_cents=edge,
                side=side,
                entry_cents=entry,
                gate_log=gate_log + [str(err)],
                weather_market_type=weather_mt,
                from_range_buckets=True,
            )

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
