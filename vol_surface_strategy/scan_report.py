"""One-shot scan of all vol-surface trade universes: BTC hourly + per-city weather HIGH/LOW."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

from vol_surface_strategy.analysis import ScanResult, run_scan
from vol_surface_strategy.config import CITIES, DEFAULT_BTC_HOURLY_SERIES
from vol_surface_strategy.discovery import (
    discover_btc_hourly_markets,
    discover_weather_markets,
    resolution_date_for_high_scan,
    resolution_date_for_low_scan,
)
from vol_surface_strategy.kalshi_io import load_client, portfolio_total_cents
from vol_surface_strategy.market_utils import contract_from_market
from vol_surface_strategy.tracker import deployed_cents_total, init_db

# Short hints for ScanResult.reason (supplement gate_log).
_REASON_HINT: dict[str, str] = {
    "gate1_raw_count": "Fewer than 3 raw contracts",
    "gate1_informative_count": "Fewer than 3 contracts in 5–95¢ band (threshold path)",
    "gate1_derived_informative_count": "Fewer than 3 derived thresholds after filters (range path)",
    "gate1 fail": "Fewer than 3 informative contracts (BTC)",
    "gate2_total_volume": "Liquidity gate failed (crypto: 5k each proxy; weather: 1k each)",
    "distribution_collapsed": "≥85% mass in one bucket — near-resolved; no vol surface",
    "insufficient_two_sided_anchors": "Fewer than 3 strikes with two-sided YES books for σ pairs",
    "gate6_monotone": "Could not build strictly decreasing mid sequence (threshold)",
    "gate6_monotone_derived": "Could not build strictly decreasing derived mids (range)",
    "pair_count": "Fewer than 3 valid σ pairs",
    "ambiguous_surface": "LOO outlier gap too small",
    "infection_fail": "Infection outlier could not be chosen (N=3)",
    "no_sigma_star": "No median σ from anchor pairs",
    "plausibility_sigma": "σ* outside plausible band",
    "underlying": "Could not estimate μ / S*",
    "plausibility_mu": "μ* vs climatology failed",
    "plausibility_S": "S* vs ATM proxy failed (BTC)",
    "plausibility_bucket_mass": "Model interval masses do not sum to ~1",
    "reverse_map_fail": "Could not map derived outlier to a bucket",
    "below_edge_threshold": "|edge| below minimum for regime",
    "edge_unstable": "Edge sign flips under σ perturbation",
    "gate5_spread": "YES bid–ask spread too wide",
    "gate4_no_ask": "No valid NO price for entry",
    "gate4_yes_ask": "No valid YES price for entry",
    "kelly_nonpositive": "Kelly fraction non-positive",
    "zero_contracts": "Rounded position size is zero",
    "market_classification": "Could not classify weather ladder / CDF failed",
}


def _hint(reason: str) -> str:
    return _REASON_HINT.get(reason, "")


def _dollars_to_cents(d: Any) -> Optional[int]:
    if d is None or d == "":
        return None
    try:
        return int(round(float(d) * 100))
    except (TypeError, ValueError):
        return None


def yes_spread_cents(m: Any) -> Optional[float]:
    yb = _dollars_to_cents(getattr(m, "yes_bid_dollars", None))
    ya = _dollars_to_cents(getattr(m, "yes_ask_dollars", None))
    if yb is None or ya is None:
        return None
    return float(ya - yb)


def _market_by_ticker(markets: list[Any], ticker: str) -> Optional[Any]:
    for m in markets:
        if str(getattr(m, "ticker", "") or "") == ticker:
            return m
    return None


def _label_for_ticker(markets: list[Any], ticker: str) -> str:
    m = _market_by_ticker(markets, ticker)
    if not m:
        return ticker
    sub = str(getattr(m, "yes_sub_title", "") or getattr(m, "subtitle", "") or "").strip()
    if sub:
        return f"{ticker} — {sub[:80]}"
    return ticker


def _contracts_from_markets(markets: list[Any], kind: str) -> list[Any]:
    out = []
    for m in markets:
        c = contract_from_market(m, kind=kind)
        if c:
            out.append(c)
    return out


def _weather_t_resolve(markets: list[Any], now_utc: datetime) -> datetime:
    t_resolve = now_utc + timedelta(hours=12)
    if markets:
        ct = getattr(markets[0], "close_time", None)
        if isinstance(ct, str):
            if ct.endswith("Z"):
                ct = ct[:-1] + "+00:00"
            t_resolve = datetime.fromisoformat(ct)
            if t_resolve.tzinfo is None:
                t_resolve = t_resolve.replace(tzinfo=timezone.utc)
    return t_resolve


def _print_gate_log(res: ScanResult) -> None:
    for line in res.gate_log:
        print(f"    gate_log: {line}")


def _print_scan_block(
    title: str,
    subtitle: str,
    markets: list[Any],
    raw_count: int,
    res: ScanResult,
) -> None:
    print()
    print("=" * 88)
    print(title)
    if subtitle:
        print(subtitle)
    print("=" * 88)
    print(f"  Contracts loaded for scan: {raw_count}")
    print(f"  Result: action={res.action!r}  ok={res.ok}  reason={res.reason!r}")
    hint = _hint(res.reason)
    if hint:
        print(f"  ({hint})")
    if res.gate_log:
        _print_gate_log(res)
    print(f"  weather_market_type={res.weather_market_type!r}  from_range_buckets={res.from_range_buckets}")
    if res.sigma_star is not None:
        print(f"  sigma*={res.sigma_star:.4f}" + (f"  mu*/S*={res.underlying:.4f}" if res.underlying is not None else ""))

    if res.action == "trade" and res.side and res.outlier_ticker:
        tick = res.outlier_ticker
        sp = None
        m = _market_by_ticker(markets, tick)
        if m:
            sp = yes_spread_cents(m)
        lab = _label_for_ticker(markets, tick)
        side_human = "Buy YES" if res.side == "yes" else "Buy NO"
        sp_s = f"{sp:.1f}¢" if sp is not None else "n/a"
        edge_s = f"{res.edge_cents:.2f}¢" if res.edge_cents is not None else "n/a"
        print()
        print("  --- TRADE (hypothetical, from scan) ---")
        print(f"  {side_human}  |  sub-market: {lab}")
        print(f"  YES bid–ask spread: {sp_s}  |  model edge vs mid: {edge_s}")
        if res.entry_cents is not None:
            print(f"  Suggested limit entry: {res.entry_cents}¢  |  contracts: {res.contracts_to_buy}  |  f_trade={res.f_trade:.4f}")
    elif res.action == "skip":
        edge_s = f"{res.edge_cents:.2f}¢" if res.edge_cents is not None else "n/a"
        print(f"  Skip: |edge|={edge_s} (below threshold for this surface regime)")
        if res.outlier_ticker:
            print(f"  Outlier ticker (reference): {res.outlier_ticker!r}")


def _weather_max_pages() -> Optional[int]:
    """Env SCAN_REPORT_WEATHER_PAGES: unset → full catalog (same as production runner; can be slow).

    Set to N>0 to stop after N API pages (200 markets/page) for a faster smoke test.
    Set to 0 → full catalog explicitly.
    """
    raw = os.environ.get("SCAN_REPORT_WEATHER_PAGES", "").strip()
    if not raw:
        return None
    try:
        n = int(raw)
    except ValueError:
        return None
    return None if n <= 0 else n


def run_scan_report(
    *,
    city_ids: Optional[list[str]] = None,
    include_btc: bool = True,
    weather_discovery_pages: Optional[int] = None,
) -> None:
    """
    weather_discovery_pages: None → env SCAN_REPORT_WEATHER_PAGES; if unset, full catalog (like runner).
    0 → full catalog.  N>0 → cap at N pages (faster, may find zero weather markets).
    """
    init_db()
    client = load_client()
    now_utc = datetime.now(timezone.utc)
    total_pf = portfolio_total_cents(client)
    _, dep_frac = deployed_cents_total(total_pf)

    cities = city_ids if city_ids is not None else list(CITIES.keys())

    print("Vol surface — scan-all report (read-only, no orders)")
    print(f"Now (UTC): {now_utc.isoformat()}")
    print(f"Portfolio: ${total_pf/100:,.2f}  deployed_frac={dep_frac:.3f}")
    if weather_discovery_pages is None:
        wlim = _weather_max_pages()
    elif weather_discovery_pages == 0:
        wlim = None
    else:
        wlim = weather_discovery_pages
    print(f"Cities ({len(cities)}): {', '.join(cities)}")
    print(f"Include BTC hourly: {include_btc}")
    wp_disp = "full catalog (same as live runner)" if wlim is None else f"max {wlim} page(s)"
    print(f"Weather discovery: {wp_disp} (~200 markets/page).")
    print("  Tip: export SCAN_REPORT_WEATHER_PAGES=40 for a faster partial scan (may miss cities).")

    if include_btc:
        hour_start = now_utc.replace(minute=0, second=0, microsecond=0)
        hour_end = hour_start + timedelta(hours=1)
        series = os.environ.get("VOL_BTC_HOURLY_SERIES", DEFAULT_BTC_HOURLY_SERIES).strip()
        markets = discover_btc_hourly_markets(client, hour_end_utc=hour_end)
        raw = _contracts_from_markets(markets, "btc")
        title = f"BTC hourly  (series {series})"
        sub = f"Target hour end (UTC): {hour_end.isoformat()}  |  markets discovered: {len(markets)}"
        if not raw:
            print()
            print("=" * 88)
            print(title)
            print(sub)
            print("=" * 88)
            print("  No strikable contracts (missing bid/ask or strike). Skipping run_scan.")
        else:
            res = run_scan(
                raw,
                model="lognormal",
                t_resolve=hour_end,
                now=now_utc,
                portfolio_cents=total_pf,
                deployed_fraction=dep_frac,
            )
            _print_scan_block(title, sub, markets, len(raw), res)

    for cid in cities:
        if cid not in CITIES:
            print(f"\n[warn] Unknown city_id {cid!r}, skip", file=sys.stderr)
            continue
        city = CITIES[cid]
        tz = ZoneInfo(city.tz_name)
        local = now_utc.astimezone(tz)

        for kind, res_date_fn in (
            ("HIGH", resolution_date_for_high_scan),
            ("LOW", resolution_date_for_low_scan),
        ):
            res_date: date = res_date_fn(local)
            markets = discover_weather_markets(
                client, city, kind, res_date, tz, max_pages=wlim
            )  # type: ignore[arg-type]
            raw = _contracts_from_markets(markets, "weather")
            title = f"Weather {kind}  |  {cid}  |  resolution date (local): {res_date.isoformat()}"
            ev = ""
            if markets:
                ev = str(getattr(markets[0], "event_ticker", "") or getattr(markets[0], "title", "") or "")[:120]
            sub = f"Markets discovered: {len(markets)}  |  {ev}"
            if not raw:
                print()
                print("=" * 88)
                print(title)
                print(sub)
                print("=" * 88)
                if not markets:
                    print(
                        "  No markets matched (city + temp + date + HIGH/LOW)."
                    )
                    print(
                        "  If you capped pages: unset SCAN_REPORT_WEATHER_PAGES or use --weather-pages 0 "
                        "for full pagination."
                    )
                else:
                    print(
                        f"  {len(markets)} market(s) matched discovery but 0 parsable contracts "
                        "(missing bid/ask or strike/bucket parse). Skipping run_scan."
                    )
                continue
            t_resolve = _weather_t_resolve(markets, now_utc)
            res = run_scan(
                raw,
                model="normal",
                t_resolve=t_resolve,
                now=now_utc,
                city_id=cid,
                portfolio_cents=total_pf,
                deployed_fraction=dep_frac,
            )
            _print_scan_block(title, sub, markets, len(raw), res)

    print()
    print("=" * 88)
    print("End of report.")
    print("=" * 88)


def main() -> None:
    p = argparse.ArgumentParser(description="Scan all vol-surface markets once; print gates / trades.")
    p.add_argument(
        "--cities",
        type=str,
        default="",
        help="Comma-separated city ids (e.g. NYC,CHI). Default: all configured cities.",
    )
    p.add_argument("--no-btc", action="store_true", help="Skip BTC hourly scan.")
    p.add_argument(
        "--weather-pages",
        type=int,
        default=None,
        metavar="N",
        help="Weather: max get_markets pages. Omit = env SCAN_REPORT_WEATHER_PAGES or full catalog. 0=full.",
    )
    args = p.parse_args()
    city_ids = [x.strip() for x in args.cities.split(",") if x.strip()] or None
    run_scan_report(
        city_ids=city_ids,
        include_btc=not args.no_btc,
        weather_discovery_pages=args.weather_pages,
    )


if __name__ == "__main__":
    main()
