"""Weather arbitrage bot — NWS vs Kalshi same-day temperature buckets."""

from __future__ import annotations

import sys
import time
from datetime import datetime, timezone

import pytz
import schedule
from dotenv import load_dotenv

from config import (
    CITIES,
    DEFAULT_CONTRACTS,
    MAX_CONCURRENT_OPEN_TRADES,
    MAX_DAILY_LOSS_CENTS,
    RISK_DAILY_TIMEZONE,
    TRADE_WINDOW_END_HOUR,
    TRADE_WINDOW_START_HOUR,
)
from db import (
    count_open_trades,
    fetch_open_trades_for_city,
    init_db,
    log_error,
    log_forecast,
    log_signal,
    log_trade_close,
    log_trade_open,
    realized_pnl_today_cents,
)
from kalshi import (
    close_position,
    fetch_open_temp_markets_for_city,
    get_orderbook,
    get_positions,
    load_client,
    market_range_label,
    parse_bucket_range,
    place_order,
)
from nws import get_grid_point, get_projected_high, has_severe_weather_blackout
from signal import check_early_exits, evaluate_trades

load_dotenv()

GRID_CACHE: dict[str, dict] = {}


def _pos_nonzero(p) -> bool:
    try:
        return abs(float(p.position_fp or "0")) >= 1e-6
    except (TypeError, ValueError):
        return False


def initialize() -> None:
    init_db()
    for city_key, cfg in CITIES.items():
        try:
            grid = get_grid_point(cfg["lat"], cfg["lon"])
            GRID_CACHE[city_key] = grid
            print(
                f"Grid cached for {city_key}: "
                f"{grid['gridId']}/{grid['gridX']},{grid['gridY']}"
            )
        except Exception as e:
            log_error("nws", "get_grid_point", 0, str(e))
            print(f"Failed to cache grid for {city_key}: {e}")


def run_signal_loop() -> None:
    try:
        client = load_client()
    except Exception as e:
        log_error("kalshi", "load_client", 0, str(e))
        print(f"Kalshi client: {e}", file=sys.stderr)
        return

    pnl_today = realized_pnl_today_cents(RISK_DAILY_TIMEZONE)
    allow_new = pnl_today > -MAX_DAILY_LOSS_CENTS
    if not allow_new:
        print(
            f"Daily loss limit reached (realized today: {pnl_today / 100:.2f} USD); "
            "skipping new entries."
        )

    open_trade_count = count_open_trades()
    allow_size = open_trade_count < MAX_CONCURRENT_OPEN_TRADES
    if not allow_size:
        print(
            f"Max concurrent open trades ({MAX_CONCURRENT_OPEN_TRADES}) reached; "
            "skipping new entries."
        )

    can_enter = allow_new and allow_size

    for city_key, cfg in CITIES.items():
        try:
            process_city(client, city_key, cfg, allow_new_entries=can_enter)
        except Exception as e:
            log_error("main", city_key, 0, str(e))
            print(f"Error processing {city_key}: {e}")


def process_city(client, city_key: str, cfg: dict, *, allow_new_entries: bool) -> None:
    grid_info = GRID_CACHE.get(city_key)
    if not grid_info:
        return

    tz = pytz.timezone(grid_info["timeZone"])
    now_local = datetime.now(tz)

    if not (TRADE_WINDOW_START_HOUR <= now_local.hour < TRADE_WINDOW_END_HOUR):
        return

    if has_severe_weather_blackout(cfg["alert_area"]):
        print(f"{city_key}: severe-weather blackout active, skipping")
        return

    projected = get_projected_high(city_key, grid_info, cfg["metar_station"])
    if not projected:
        return
    log_forecast(projected)

    if projected["is_stale"]:
        print(
            f"{city_key}: stale forecast "
            f"({projected['forecast_age_minutes']:.0f} min old), skipping"
        )
        return

    raw_markets = fetch_open_temp_markets_for_city(
        client, cfg["kalshi_name_variants"], tz
    )
    if not raw_markets:
        print(f"{city_key}: no Kalshi markets found for today")
        return

    buckets: list[dict] = []
    for m in raw_markets:
        ticker = getattr(m, "ticker", None)
        if not ticker:
            continue
        label = market_range_label(m)
        low, high = parse_bucket_range(label)
        if low is None and high is None:
            continue
        try:
            ob = get_orderbook(client, ticker)
        except Exception as e:
            log_error("kalshi", f"orderbook/{ticker}", 0, str(e))
            continue
        buckets.append(
            {
                "ticker": ticker,
                "title": getattr(m, "title", None) or "",
                "low": low,
                "high": high,
                "yes_bid": ob["yes_bid"],
                "yes_ask": ob["yes_ask"],
                "spread": ob["spread"],
                "midpoint": ob["midpoint"],
            }
        )

    if not buckets:
        return

    ct = getattr(raw_markets[0], "close_time", None)
    if ct is None:
        return
    if isinstance(ct, datetime):
        close_dt = ct
    else:
        s = str(ct)
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        close_dt = datetime.fromisoformat(s)
    if close_dt.tzinfo is None:
        close_dt = close_dt.replace(tzinfo=timezone.utc)
    close_time_local = close_dt.astimezone(tz)

    positions = get_positions(client)
    city_tickers = {b["ticker"] for b in buckets}
    city_positions = [p for p in positions if getattr(p, "ticker", "") in city_tickers]

    open_map = fetch_open_trades_for_city(city_key)
    tracked = [
        {
            "ticker": t,
            "side": meta["side"],
            "entry_price_cents": meta["entry_price_cents"],
            "contracts": meta["contracts"],
        }
        for t, meta in open_map.items()
    ]

    exits = check_early_exits(tracked, buckets, now_local, close_time_local)
    for exit_signal in exits:
        meta = open_map.get(exit_signal["ticker"])
        if not meta:
            continue
        qty = int(meta["contracts"])
        if qty <= 0:
            continue
        try:
            close_position(
                client,
                exit_signal["ticker"],
                exit_signal["side"],
                exit_signal["limit_price_cents"],
                qty,
            )
            log_trade_close(
                meta["trade_id"],
                exit_signal["limit_price_cents"],
                exit_signal["reason"],
            )
            print(
                f"CLOSED {city_key} {exit_signal['ticker']}: {exit_signal['reason']}"
            )
        except Exception as e:
            log_error("kalshi", "close_position", 0, str(e))
            print(f"Close failed {exit_signal['ticker']}: {e}")

    held = {
        getattr(p, "ticker")
        for p in city_positions
        if getattr(p, "ticker", None) in city_tickers and _pos_nonzero(p)
    }

    if not allow_new_entries:
        return

    trade_signals = evaluate_trades(projected, buckets, now_local, close_time_local)

    for signal in trade_signals:
        if signal["ticker"] in held:
            log_signal(city_key, signal, "skipped", "already_holding")
            continue

        try:
            order = place_order(
                client,
                signal["ticker"],
                signal["side"],
                signal["limit_price_cents"],
                DEFAULT_CONTRACTS,
            )
            trade_id = log_trade_open(city_key, signal, DEFAULT_CONTRACTS)
            log_signal(city_key, signal, "entered")
            print(
                f"TRADE {city_key}: {signal['side'].upper()} {signal['ticker']} "
                f"@ {signal['limit_price_cents']}¢ | edge={signal['edge']} "
                f"model={signal['model_prob']} market={signal['market_prob']} "
                f"| order_id={getattr(order, 'order_id', '')}"
            )
        except Exception as e:
            log_error("kalshi", "place_order", 0, str(e))
            log_signal(city_key, signal, "error", str(e))
            print(f"Order failed: {e}")
            continue

        break


if __name__ == "__main__":
    initialize()
    schedule.every(30).minutes.do(run_signal_loop)
    print("Weather bot running. Signal loop every 30 minutes.")
    run_signal_loop()
    while True:
        schedule.run_pending()
        time.sleep(60)
