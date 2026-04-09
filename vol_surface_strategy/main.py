#!/usr/bin/env python3
"""Vol surface strategy — entry point."""

from __future__ import annotations

import argparse

from vol_surface_strategy.runner import run_forever, tick
from vol_surface_strategy.scan_report import run_scan_report


def main() -> None:
    p = argparse.ArgumentParser(description="Kalshi vol-surface strategy (BTC hourly + weather)")
    p.add_argument("--dry-run", action="store_true", help="Analyze and log only; do not place orders")
    p.add_argument("--once", action="store_true", help="Single tick then exit (for testing)")
    p.add_argument(
        "--scan-report",
        action="store_true",
        help="One-shot: scan all configured markets, print gate failures / trade line (no orders)",
    )
    p.add_argument(
        "--cities",
        type=str,
        default="",
        help="With --scan-report: comma-separated city ids (default: all). Example: NYC,CHI",
    )
    p.add_argument("--no-btc", action="store_true", help="With --scan-report: skip BTC hourly")
    p.add_argument(
        "--btc-only",
        action="store_true",
        help="With --scan-report: BTC hourly only (no weather cities)",
    )
    p.add_argument(
        "--btc-debug",
        action="store_true",
        help="With --scan-report: DEBUG logs + BTC ladder / monotone / gate2 dump",
    )
    p.add_argument(
        "--weather-pages",
        type=int,
        default=None,
        metavar="N",
        help="With --scan-report: max weather API pages; 0=full catalog. Default: env or full catalog.",
    )
    p.add_argument("--interval", type=float, default=12.0, help="Loop sleep seconds (default 12)")
    args = p.parse_args()
    if args.scan_report:
        city_ids = [x.strip() for x in args.cities.split(",") if x.strip()] or None
        run_scan_report(
            city_ids=city_ids,
            include_btc=not args.no_btc,
            weather_discovery_pages=args.weather_pages,
            btc_only=args.btc_only,
            btc_debug=args.btc_debug,
        )
        return
    if args.once:
        tick(dry_run=args.dry_run)
    else:
        run_forever(dry_run=args.dry_run, interval_sec=args.interval)


if __name__ == "__main__":
    main()
