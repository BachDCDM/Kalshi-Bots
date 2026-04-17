"""CLI entry for the sports pre-game vol-surface runner."""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

from vol_surface_strategy.logutil import setup_logging
from vol_surface_strategy.sports_runner import run_sports_forever, tick_sports
from vol_surface_strategy.sports_scan_report import run_sports_scan_report, run_sports_upcoming_scan


def main() -> None:
    p = argparse.ArgumentParser(description="Kalshi sports pre-game vol-surface (LOO + maker limits)")
    p.add_argument("--dry-run", action="store_true", help="Log actions without placing orders")
    p.add_argument(
        "--once",
        action="store_true",
        help="Single poll cycle (default: loop forever with --interval)",
    )
    p.add_argument(
        "--interval",
        type=float,
        default=120.0,
        help="Idle seconds between polls when not --once (overridden by VOL_SPORTS_POLL_IDLE_SEC; 5m when in-window)",
    )
    p.add_argument(
        "--scan-report",
        action="store_true",
        help="One-shot: fetch VOL_SPORTS_EVENTS, print every market + each ladder scan (no orders)",
    )
    p.add_argument(
        "--upcoming-scan",
        action="store_true",
        help=(
            "One-shot: sports ladders whose scheduled start is in the next --within-minutes (default 120); "
            "prints minutes to start + scan outcome per ladder (no orders). Combine with --live-scan."
        ),
    )
    p.add_argument(
        "--live-scan",
        action="store_true",
        help=(
            "One-shot: ladders whose scheduled start was in the past but within "
            "--live-within-minutes (default 240); prints starts_in (negative) + scan outcome. "
            "Combine with --upcoming-scan."
        ),
    )
    p.add_argument(
        "--within-minutes",
        type=float,
        default=120.0,
        metavar="M",
        help="With --upcoming-scan: include games with start in (0, M] minutes from now (ET)",
    )
    p.add_argument(
        "--live-within-minutes",
        type=float,
        default=240.0,
        metavar="M",
        help="With --live-scan: include games whose scheduled start was at most M minutes ago (ET)",
    )
    p.add_argument(
        "--no-per-market-fields",
        action="store_true",
        help="With --scan-report: skip dumping all API fields per market (faster)",
    )
    p.add_argument(
        "--no-ladder-pipeline",
        action="store_true",
        help="With --scan-report: skip monotone/informative/gate2 ladder dump after each scan",
    )
    p.add_argument(
        "--ignore-time-window",
        action="store_true",
        help="With --scan-report: only affects banner note; scans always run",
    )
    p.add_argument(
        "--debug",
        action="store_true",
        help="With --scan-report: set logging to DEBUG on vol_surface logger",
    )
    p.add_argument(
        "--scan-debug",
        action="store_true",
        help=(
            "Verbose catalog / multivariate discovery (VOL_SPORTS_SCAN_DEBUG=1). "
            "With --scan-report / --upcoming-scan / --live-scan. Implied with --debug on those modes."
        ),
    )
    p.add_argument(
        "--events",
        type=str,
        default="",
        metavar="LIST",
        help=(
            "Comma-separated EVENT_TICKER:LEAGUE for this run (sets VOL_SPORTS_EVENTS). "
            "Example: --events 'KXMVENBAGAME-26APR18LALDEN:NBA'. Optional |ISO8601 per row."
        ),
    )
    p.add_argument(
        "--no-autoscan",
        action="store_true",
        help="When VOL_SPORTS_EVENTS is unset, do not query Kalshi multivariate series (no implicit game discovery)",
    )
    args = p.parse_args()

    _root = Path(__file__).resolve().parent.parent
    load_dotenv(_root / ".env", override=False)
    if args.no_autoscan:
        os.environ["VOL_SPORTS_NO_AUTOSCAN"] = "1"
    if args.events.strip():
        os.environ["VOL_SPORTS_EVENTS"] = args.events.strip()

    if args.scan_report and (args.upcoming_scan or args.live_scan):
        p.error("do not combine --scan-report with --upcoming-scan or --live-scan")
    if args.upcoming_scan or args.live_scan:
        scan_debug = bool(args.scan_debug or args.debug)
        if scan_debug:
            os.environ["VOL_SPORTS_SCAN_DEBUG"] = "1"
        run_sports_upcoming_scan(
            within_minutes=args.within_minutes,
            include_upcoming=bool(args.upcoming_scan),
            include_live=bool(args.live_scan),
            live_within_minutes_after_start=args.live_within_minutes,
            log_level=logging.DEBUG if args.debug else logging.INFO,
            scan_debug=scan_debug,
        )
        return
    if args.scan_report:
        scan_debug = bool(args.scan_debug or args.debug)
        if scan_debug:
            os.environ["VOL_SPORTS_SCAN_DEBUG"] = "1"
        run_sports_scan_report(
            per_market_debug=not args.no_per_market_fields,
            ladder_pipeline_debug=not args.no_ladder_pipeline,
            log_level=logging.DEBUG if args.debug else logging.INFO,
            ignore_time_window=args.ignore_time_window,
            scan_debug=scan_debug,
        )
        return
    setup_logging()
    if args.once:
        _ = tick_sports(dry_run=args.dry_run)
    else:
        run_sports_forever(dry_run=args.dry_run, interval_sec=args.interval)


if __name__ == "__main__":
    main()
