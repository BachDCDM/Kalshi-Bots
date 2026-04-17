"""One-shot sports scan: debug every raw Kalshi market + per-ladder vol pipeline + run_scan_sports."""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv

from vol_surface_strategy.discovery import parse_market_resolve_utc
from vol_surface_strategy.kalshi_io import load_client, portfolio_total_cents
from vol_surface_strategy.logutil import setup_logging
from vol_surface_strategy.market_utils import contract_from_sports_market
from vol_surface_strategy.scan_report import _hint, _print_scan_block
from vol_surface_strategy.sports_analysis import run_scan_sports, sports_pipeline_debug_lines
from vol_surface_strategy.sports_discovery import (
    group_markets_into_surface_ladders,
    iter_sports_game_targets,
    parse_event_overrides_with_start,
)
from vol_surface_strategy.sports_model import infer_sports_distribution
from vol_surface_strategy.sports_windows import ET, game_start_et_from_markets, in_pre_game_order_window
from vol_surface_strategy.tracker import deployed_cents_total, init_db


def _iter_public_attrs(m: Any) -> list[tuple[str, str]]:
    if isinstance(m, dict):
        pairs = list(m.items())
    elif hasattr(m, "__dict__"):
        pairs = list(vars(m).items())
    else:
        return []
    out: list[tuple[str, str]] = []
    for k, v in sorted(pairs, key=lambda x: str(x[0]).lower()):
        sk = str(k)
        if sk.startswith("_"):
            continue
        try:
            sv = repr(v)
        except Exception:
            sv = "<unrepr>"
        if len(sv) > 700:
            sv = sv[:700] + "…"
        out.append((sk, sv))
    return out


def _print_market_debug(m: Any, idx: int) -> None:
    print()
    print(f"  --- raw market index={idx} ---")
    for k, sv in _iter_public_attrs(m):
        print(f"      {k}={sv}")
    cc = contract_from_sports_market(m)
    if cc:
        print(
            f"      → ContractInput: strike={cc.strike} mid={cc.mid_cents:.2f}¢ "
            f"bid={cc.yes_bid_cents:.1f} ask={cc.yes_ask_cents:.1f} "
            f"one_sided={cc.one_sided} vol_fp={cc.volume_fp}"
        )
    else:
        print("      → ContractInput: None (strike or YES book parse failed)")


def _t_resolve_utc(markets: list[Any], now_utc: datetime) -> datetime:
    if markets:
        u = parse_market_resolve_utc(markets[0])
        if u is not None:
            return u
    return now_utc + timedelta(hours=6)


def run_sports_scan_report(
    *,
    per_market_debug: bool = True,
    ladder_pipeline_debug: bool = True,
    log_level: int = logging.INFO,
    ignore_time_window: bool = False,
    scan_debug: bool = False,
) -> None:
    """
    Read-only: load games from ``VOL_SPORTS_EVENTS`` **or** Kalshi multivariate autoscan, then print
    each API market (optional) and each ``(event_ticker, series_ticker)`` ladder via ``run_scan_sports``.

    ``ignore_time_window``: if True, suppress the outside-window note only; scans always run.

    ``scan_debug``: verbose catalog/multivariate diagnostics (also set ``VOL_SPORTS_SCAN_DEBUG=1``).
    """
    _root = Path(__file__).resolve().parent.parent
    load_dotenv(_root / ".env", override=False)
    setup_logging(level=log_level)
    init_db()
    client = load_client()
    games = iter_sports_game_targets(client, scan_debug=scan_debug)
    if not games:
        print(
            "[error] No games to scan. Options:",
            file=sys.stderr,
        )
        print(
            "  • Multivariate series: VOL_SPORTS_AUTOSCAN_SERIES (defaults include KXMVENBAGAME…).",
            file=sys.stderr,
        )
        print(
            "  • Catalog fallback: VOL_SPORTS_CATALOG_MAX_PAGES, VOL_SPORTS_CATALOG_HOURS, "
            "VOL_SPORTS_CATALOG_MAX_GAMES (open markets in close-time window).",
            file=sys.stderr,
        )
        print(
            "  • Explicit:  --events 'EVENT_TICKER:NBA'  or  VOL_SPORTS_EVENTS  in .env",
            file=sys.stderr,
        )
        print(
            "  • If autoscan is disabled: omit --no-autoscan and clear env VOL_SPORTS_NO_AUTOSCAN.",
            file=sys.stderr,
        )
        if not scan_debug:
            print(
                "  • Re-run with --scan-debug (or export VOL_SPORTS_SCAN_DEBUG=1) for longer "
                "catalog samples / multivariate field dump.",
                file=sys.stderr,
            )
        sys.exit(2)

    now_utc = datetime.now(timezone.utc)
    total_pf = portfolio_total_cents(client)
    _, dep_frac = deployed_cents_total(total_pf)
    start_overrides = parse_event_overrides_with_start()
    use_mls_weather = (
        os.environ.get("VOL_SPORTS_MLS_WEATHER_GATE", "").strip() in ("1", "true", "yes")
    )

    print("Sports vol surface — full scan report (read-only, no orders)")
    print(f"Now (UTC): {now_utc.isoformat()}")
    print(f"Portfolio: ${total_pf / 100:,.2f}  deployed_frac={dep_frac:.3f}")
    print(f"per_market_debug={per_market_debug}  ladder_pipeline_debug={ladder_pipeline_debug}")
    print(f"ignore_time_window={ignore_time_window}  scan_debug={scan_debug}")
    print(f"games_to_scan={len(games)} (explicit VOL_SPORTS_EVENTS or Kalshi autoscan)")

    for event_ticker, sport, all_mk in games:
        print()
        print("#" * 88)
        print(f"EVENT {event_ticker}  |  league={sport}")
        print("#" * 88)

        if not all_mk:
            print("  No markets returned (empty list or fetch failed).")
            continue

        print(f"  Total markets in event: {len(all_mk)}")

        if per_market_debug:
            print()
            print("  ========== PER-MARKET DEBUG (all fields on API object) ==========")
            for i, m in enumerate(all_mk):
                _print_market_debug(m, i)

        gs_et: datetime | None = None
        if event_ticker in start_overrides:
            raw = start_overrides[event_ticker]
            try:
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=ET)
                gs_et = dt.astimezone(ET)
            except ValueError:
                print(f"  [warn] bad start override ISO for {event_ticker}: {raw!r}")
        if gs_et is None:
            gs_et = game_start_et_from_markets(all_mk, sport=sport, fallback_utc=None)

        now_et = now_utc.astimezone(ET)
        if gs_et is not None:
            allowed, mins_left = in_pre_game_order_window(sport, now_et, gs_et)
            print()
            print(
                f"  Game start (ET proxy): {gs_et.isoformat()}  |  "
                f"in_pre_game_window={allowed}  minutes_to_start={mins_left:.1f}"
            )
            if not allowed and not ignore_time_window:
                print(
                    "  [note] Outside pre-game order window — live runner would skip; "
                    "scan still runs here for inspection.",
                    flush=True,
                )
        else:
            print()
            print("  Game start (ET): unknown (no close_time parse; set |ISO on VOL_SPORTS_EVENTS row)")

        ladders = group_markets_into_surface_ladders(all_mk)
        print()
        print(f"  Surface ladders (event × series × optional custom_strike shard): {len(ladders)}")

        for (et, st, shard), ms in sorted(
            ladders.items(), key=lambda x: (x[0][0], x[0][1], x[0][2])
        ):
            if et != event_ticker:
                continue
            print()
            print("-" * 88)
            shard_s = f"  ladder_shard={shard!r}" if shard else ""
            print(f"  LADDER  series_ticker={st!r}{shard_s}  n_markets={len(ms)}")
            print("-" * 88)

            raw: list[Any] = []
            for m in ms:
                c = contract_from_sports_market(m)
                if c:
                    raw.append(c)

            model, nb_r = infer_sports_distribution(sport, ms)
            use_wg = use_mls_weather and sport == "MLS"
            sub = f"model={model!r} nbinom_r_disp={nb_r} contracts_parsed={len(raw)}"

            if len(raw) < 4:
                print(f"  Skip scan: fewer than 4 parsable contracts. {sub}")
                continue

            t_resolve = _t_resolve_utc(ms, now_utc)
            res = run_scan_sports(
                raw,
                model=model,
                t_resolve=t_resolve,
                now=now_utc,
                portfolio_cents=total_pf,
                deployed_fraction=dep_frac,
                nbinom_r_disp=nb_r,
                min_informative=4,
                use_weather_liquidity_gate=use_wg,
            )
            title = f"Sports scan  |  {sport}  |  {et}  |  {st}" + (f"  |  {shard}" if shard else "")
            _print_scan_block(title, sub, ms, len(raw), res)

            if ladder_pipeline_debug:
                print()
                for ln in sports_pipeline_debug_lines(raw, use_weather_liquidity_gate=use_wg):
                    print(ln)


def _format_upcoming_scan_summary(res: Any) -> str:
    hint = _hint(str(getattr(res, "reason", "") or ""))
    tail = f"  ({hint})" if hint else ""
    act = str(getattr(res, "action", ""))
    rsn = str(getattr(res, "reason", ""))
    if act == "trade":
        oc = getattr(res, "outlier_ticker", "") or ""
        sd = getattr(res, "side", "") or ""
        ec = getattr(res, "edge_cents", None)
        nb = int(getattr(res, "contracts_to_buy", 0) or 0)
        return f"TRADE_RECOMMENDED  ticker={oc}  side={sd}  edge_cents={ec}  contracts={nb}"
    if act == "skip":
        return f"NO_TRADE(skip)  reason={rsn}{tail}"
    return f"NO_TRADE(abort)  reason={rsn}{tail}"


def run_sports_upcoming_scan(
    *,
    within_minutes: float = 120.0,
    include_upcoming: bool = True,
    include_live: bool = False,
    live_within_minutes_after_start: float = 240.0,
    log_level: int = logging.INFO,
    scan_debug: bool = False,
) -> None:
    """
    Discover sports games (same as full scan report), filter by scheduled start vs now (ET), then
    run ``run_scan_sports`` on each surface ladder.

    * **Upcoming** (``include_upcoming``): ``0 < minutes_to_start <= within_minutes``.
    * **Live** (``include_live``): scheduled start was between ``live_within_minutes_after_start``
      minutes ago and now (inclusive): ``-live_within… <= minutes_to_start <= 0``.
      Negative ``minutes_to_start`` means the listed start time is in the past.

    Prints ``starts_in=…`` (negative = that many minutes after scheduled start) and scan outcome.
    """
    _root = Path(__file__).resolve().parent.parent
    load_dotenv(_root / ".env", override=False)
    setup_logging(level=log_level)
    init_db()
    client = load_client()
    games = iter_sports_game_targets(client, scan_debug=scan_debug)
    now_utc = datetime.now(timezone.utc)
    now_et = now_utc.astimezone(ET)
    total_pf = portfolio_total_cents(client)
    _, dep_frac = deployed_cents_total(total_pf)
    start_overrides = parse_event_overrides_with_start()
    use_mls_weather = (
        os.environ.get("VOL_SPORTS_MLS_WEATHER_GATE", "").strip() in ("1", "true", "yes")
    )

    w = float(within_minutes)
    lw = float(live_within_minutes_after_start)
    banner_parts: list[str] = []
    if include_upcoming:
        banner_parts.append(f"upcoming start in (0, {w:g}] min")
    if include_live:
        banner_parts.append(f"live (scheduled start {lw:g} min ago … now]")
    if not banner_parts:
        print("[error] run_sports_upcoming_scan: include_upcoming and include_live are both false.")
        sys.exit(2)
    print(
        "Sports time-window surface scan — "
        + " + ".join(banner_parts)
        + " (US/Eastern). Read-only, no orders."
    )
    print(f"Now ET: {now_et.isoformat()}  |  portfolio ${total_pf / 100:,.2f}")
    print()
    if not games:
        print(
            "[error] No games discovered. Same fixes as --scan-report "
            "(events, autoscan, catalog).",
            file=sys.stderr,
        )
        sys.exit(2)

    upcoming: list[tuple[float, str, str, str, str, str, int, int, str, Optional[Any]]] = []

    for event_ticker, sport, all_mk in games:
        if not all_mk:
            continue
        gs_et: datetime | None = None
        if event_ticker in start_overrides:
            raw_iso = start_overrides[event_ticker]
            try:
                dt = datetime.fromisoformat(raw_iso.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=ET)
                gs_et = dt.astimezone(ET)
            except ValueError:
                pass
        if gs_et is None:
            gs_et = game_start_et_from_markets(all_mk, sport=sport, fallback_utc=None)
        if gs_et is None:
            continue
        mins_left = (gs_et - now_et).total_seconds() / 60.0
        upcoming_ok = include_upcoming and 0 < mins_left <= w
        live_ok = include_live and (-lw <= mins_left <= 0)
        if not (upcoming_ok or live_ok):
            continue
        start_iso = gs_et.isoformat()

        ladders = group_markets_into_surface_ladders(all_mk)
        for (et, st, shard), ms in sorted(
            ladders.items(), key=lambda x: (x[0][0], x[0][1], x[0][2])
        ):
            if et != event_ticker:
                continue
            raw: list[Any] = []
            for m in ms:
                c = contract_from_sports_market(m)
                if c:
                    raw.append(c)
            shard_s = shard or ""
            n_mk = len(ms)
            n_parsed = len(raw)
            if len(raw) < 4:
                upcoming.append(
                    (
                        mins_left,
                        start_iso,
                        str(et),
                        str(sport),
                        str(st),
                        shard_s,
                        n_mk,
                        n_parsed,
                        "SKIP_SCAN  parsable_contracts<4",
                        None,
                    )
                )
                continue
            t_resolve = _t_resolve_utc(ms, now_utc)
            model, nb_r = infer_sports_distribution(sport, ms)
            use_wg = use_mls_weather and sport == "MLS"
            res = run_scan_sports(
                raw,
                model=model,
                t_resolve=t_resolve,
                now=now_utc,
                portfolio_cents=total_pf,
                deployed_fraction=dep_frac,
                nbinom_r_disp=nb_r,
                min_informative=4,
                use_weather_liquidity_gate=use_wg,
            )
            upcoming.append(
                (
                    mins_left,
                    start_iso,
                    str(et),
                    str(sport),
                    str(st),
                    shard_s,
                    n_mk,
                    n_parsed,
                    _format_upcoming_scan_summary(res),
                    res,
                )
            )

    upcoming.sort(key=lambda x: x[0])
    if not upcoming:
        hints: list[str] = []
        if include_upcoming:
            hints.append(f"upcoming (0, {w:g}] min")
        if include_live:
            hints.append(f"live (≤{lw:g} min after start)")
        print(
            "No ladders matched "
            + " or ".join(hints)
            + " with a parsable scheduled start. "
            "Widen windows, use --events, or confirm discovery / close_time on markets."
        )
        return

    print(f"Matched {len(upcoming)} ladder(s), sorted by minutes_to_start (ET).\n")
    for row in upcoming:
        (
            mins_left,
            start_iso,
            et,
            sp,
            st,
            shard,
            n_mk,
            n_parsed,
            summary,
            res,
        ) = row
        shard_disp = f"  shard={shard!r}" if shard else ""
        tag = "LIVE" if mins_left <= 0 else "SOON"
        print(
            f"{tag}  starts_in={mins_left:7.1f} min  "
            f"(negative = minutes after scheduled start)  start_et={start_iso}  |  {sp}  |  {et}"
        )
        print(
            f"  series={st!r}{shard_disp}  |  n_markets={n_mk}  parsable={n_parsed}  |  {summary}"
        )
        if res is not None and getattr(res, "gate_log", None):
            gl = res.gate_log
            if isinstance(gl, list) and gl:
                print(f"  gates: {' | '.join(str(x) for x in gl[-3:])}")
        print()
