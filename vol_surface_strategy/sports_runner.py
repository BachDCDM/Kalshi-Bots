"""Pre-game sports vol-surface monitor: one resting order per (event, series) ladder."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from vol_surface_strategy.discovery import parse_market_resolve_utc
from vol_surface_strategy.exec_orders import cancel_order, place_buy, resting_order_limit_cents
from vol_surface_strategy.kalshi_io import (
    fetch_order,
    load_client,
    net_contracts_for_ticker,
    portfolio_total_cents,
)
from vol_surface_strategy.logutil import setup_logging
from vol_surface_strategy.market_utils import contract_from_sports_market
from vol_surface_strategy.runner import (
    _dedupe_monitor,
    _log_scan_result,
    _panel_order_event,
    _panel_record_scan,
    _panel_upsert_open_trade,
    _sync_resting_order_tracker,
)
from vol_surface_strategy.sports_analysis import run_scan_sports
from vol_surface_strategy.sports_discovery import (
    group_markets_into_surface_ladders,
    iter_sports_game_targets,
    parse_event_overrides_with_start,
)
from vol_surface_strategy.sports_model import SportCode, infer_sports_distribution
from vol_surface_strategy.sports_windows import (
    ET,
    any_sports_trading_window_open,
    game_start_et_from_markets,
    in_pre_game_order_window,
    order_expiration_ts,
)
from vol_surface_strategy.tracker import (
    deployed_cents_total,
    get_row,
    init_db,
    sports_key,
    update_status,
    upsert_pending,
)
from vol_surface_strategy.trading_windows import normalize_tracker_status, resting_order_expiration_ts

LOG = setup_logging()


def _game_start_et_for_ladder(
    markets: list[Any],
    event_ticker: str,
    start_overrides: dict[str, str],
    sport: SportCode,
) -> Optional[datetime]:
    if event_ticker in start_overrides:
        raw = start_overrides[event_ticker]
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=ET)
            return dt.astimezone(ET)
        except ValueError:
            LOG.warning("Bad VOL_SPORTS_EVENTS start ISO for %s: %r", event_ticker, raw)
    return game_start_et_from_markets(markets, sport=sport, fallback_utc=None)


def _t_resolve_utc(markets: list[Any], now_utc: datetime) -> datetime:
    if markets:
        u = parse_market_resolve_utc(markets[0])
        if u is not None:
            return u
    return now_utc + timedelta(hours=6)


def _surface_monitor_once(
    client: Any,
    dry_run: bool,
    now_utc: datetime,
    event_ticker: str,
    sport: SportCode,
    series_ticker: str,
    markets: list[Any],
    *,
    ladder_shard: str = "",
) -> None:
    raw: list[Any] = []
    for m in markets:
        c = contract_from_sports_market(m)
        if c:
            raw.append(c)
    if len(raw) < 4:
        return

    start_overrides = parse_event_overrides_with_start()
    gs_et = _game_start_et_for_ladder(markets, event_ticker, start_overrides, sport)
    if gs_et is None:
        LOG.debug("sports: no game start for %s %s", event_ticker, series_ticker)
        return

    now_et = now_utc.astimezone(ET)
    allowed, mins_left = in_pre_game_order_window(sport, now_et, gs_et)
    if not allowed:
        return

    key = sports_key(event_ticker, series_ticker, ladder_shard)
    mid = f"{sport} {event_ticker} {series_ticker}"
    if ladder_shard:
        mid = f"{mid} [{ladder_shard}]"
    if not _dedupe_monitor(f"sports:{key}", now_utc):
        return

    model, nb_r = infer_sports_distribution(sport, markets)
    use_weather_gate = (os.environ.get("VOL_SPORTS_MLS_WEATHER_GATE", "").strip() in ("1", "true", "yes")) and (
        sport == "MLS"
    )

    row = get_row(key)
    st = normalize_tracker_status(row.status) if row else "outside_window"
    if row is None:
        upsert_pending(
            key,
            market_type="sports_vol_surface",
            city_id=None,
            resolution_date=None,
            hour_start_utc=None,
            status="window_open",
        )
        row = get_row(key)
        st = "window_open"

    if st == "order_resting" and row and row.order_id:
        row, st = _sync_resting_order_tracker(client, key, row, st)

    if st == "position_active":
        LOG.info("[SPORTS] %s position_active (%.1f min to start)", mid, mins_left)
        return
    if st in ("resolved", "window_closed"):
        return

    if row and row.ticker and net_contracts_for_ticker(client, row.ticker) >= 1.0:
        update_status(key, "position_active")
        row2 = get_row(key)
        if row2:
            _panel_upsert_open_trade(key, row2)
        return

    t_resolve = _t_resolve_utc(markets, now_utc)
    total = portfolio_total_cents(client)
    _, dep_frac = deployed_cents_total(total)
    res = run_scan_sports(
        raw,
        model=model,
        t_resolve=t_resolve,
        now=now_utc,
        portfolio_cents=total,
        deployed_fraction=dep_frac,
        nbinom_r_disp=nb_r,
        min_informative=4,
        use_weather_liquidity_gate=use_weather_gate,
    )
    _log_scan_result(f"SPORTS {mid}", res)
    _panel_record_scan(key, res)

    exp_cap = order_expiration_ts(gs_et)
    exp_ts = resting_order_expiration_ts(now_utc, latest_allowed_ts=exp_cap)

    fill_check = "unfilled"

    if st == "order_resting" and row and row.order_id and row.ticker:
        oid = str(row.order_id)
        o: Optional[Any] = None
        try:
            o = fetch_order(client, oid)
            ost = str(getattr(o, "status", "") or "")
            if ost == "executed":
                update_status(key, "position_active")
                row2 = get_row(key)
                if row2:
                    _panel_upsert_open_trade(key, row2)
                return
        except Exception as e:
            LOG.warning("Sports get_order %s: %s", oid, e)

        if res.action != "trade":
            if not dry_run:
                try:
                    cancel_order(client, oid)
                except Exception as e:
                    LOG.warning("Sports cancel %s: %s", oid, e)
            update_status(key, "window_open", order_id=None)
            _monitor_sports(mid, mins_left, fill_check, res, "order_cancelled")
            return

        if o is None:
            LOG.warning("Sports order_resting: no API snapshot for %s", oid)
            return

        lim = resting_order_limit_cents(o, str(row.side or ""))
        price_changed = res.entry_cents is not None and lim is not None and res.entry_cents != lim
        ticker_changed = res.outlier_ticker != row.ticker
        if ticker_changed or price_changed:
            if not dry_run:
                try:
                    cancel_order(client, oid)
                except Exception as e:
                    LOG.warning("Sports cancel repricing %s: %s", oid, e)
            if not dry_run:
                try:
                    r = place_buy(
                        client,
                        ticker=res.outlier_ticker or "",
                        side=res.side or "yes",
                        count=res.contracts_to_buy,
                        price_cents=int(res.entry_cents or 0),
                        expiration_ts=exp_ts,
                    )
                    oid_n = getattr(getattr(r, "order", r), "order_id", None) or getattr(r, "order_id", None)
                    dep = (res.contracts_to_buy or 0) * (res.entry_cents or 0)
                    update_status(
                        key,
                        "order_resting",
                        order_id=str(oid_n),
                        ticker=res.outlier_ticker,
                        side=res.side,
                        entry_cents=res.entry_cents,
                        contracts=res.contracts_to_buy,
                        deployed_cents=dep,
                    )
                    _panel_order_event(
                        key,
                        "order_updated",
                        ticker=res.outlier_ticker,
                        side=res.side,
                        contracts=res.contracts_to_buy,
                        price_cents=res.entry_cents,
                        order_id=str(oid_n),
                        detail=f"old_ticker={row.ticker} old_price={lim}",
                    )
                except Exception as e:
                    LOG.exception("Sports replace order failed: %s", e)
                    return
            _monitor_sports(mid, mins_left, fill_check, res, "order_updated")
            return

        LOG.info("Sports order_unchanged ticker=%s price=%s¢", row.ticker, lim)
        _monitor_sports(mid, mins_left, fill_check, res, "order_unchanged")
        return

    if res.action == "trade" and res.side and res.entry_cents and res.contracts_to_buy:
        if dry_run:
            LOG.info(
                "DRY_RUN sports would place %s %s x%d @ %d",
                mid,
                res.side,
                res.contracts_to_buy,
                res.entry_cents,
            )
        else:
            try:
                r = place_buy(
                    client,
                    ticker=res.outlier_ticker or "",
                    side=res.side,
                    count=res.contracts_to_buy,
                    price_cents=res.entry_cents,
                    expiration_ts=exp_ts,
                )
                oid = getattr(getattr(r, "order", r), "order_id", None) or getattr(r, "order_id", None)
                dep = res.contracts_to_buy * res.entry_cents
                update_status(
                    key,
                    "order_resting",
                    order_id=str(oid),
                    ticker=res.outlier_ticker,
                    side=res.side,
                    entry_cents=res.entry_cents,
                    contracts=res.contracts_to_buy,
                    deployed_cents=dep,
                )
                _panel_order_event(
                    key,
                    "order_posted",
                    ticker=res.outlier_ticker,
                    side=res.side,
                    contracts=res.contracts_to_buy,
                    price_cents=res.entry_cents,
                    order_id=str(oid),
                )
            except Exception as e:
                LOG.exception("Sports order failed %s: %s", mid, e)
                return
        _monitor_sports(mid, mins_left, fill_check, res, "order_posted")
        return

    _monitor_sports(mid, mins_left, fill_check, res, "no_change")


def _monitor_sports(mid: str, mins_left: float, fill_check: str, res: Any, action: str) -> None:
    payload = {
        "ok": res.ok,
        "action": res.action,
        "reason": res.reason,
        "sigma_star": res.sigma_star,
        "underlying": res.underlying,
        "edge_cents": res.edge_cents,
        "side": res.side,
        "entry_cents": res.entry_cents,
        "contracts": res.contracts_to_buy,
        "gates": res.gate_log,
    }
    LOG.info(
        "[SPORTS] %s (%.1f min to start) fill=%s action=%s %s",
        mid,
        mins_left,
        fill_check,
        action,
        json.dumps(payload, default=str),
    )


def tick_sports(dry_run: bool = False) -> float:
    """
    Run one poll cycle. Returns recommended seconds until the next wake (longer idle,
    5-minute cadence while any event is in the pre-game trading window).
    """
    idle_sec = float((os.environ.get("VOL_SPORTS_POLL_IDLE_SEC") or "120").strip() or "120")
    window_sec = float((os.environ.get("VOL_SPORTS_POLL_IN_WINDOW_SEC") or "300").strip() or "300")
    idle_sec = max(15.0, idle_sec)
    window_sec = max(60.0, window_sec)

    client = load_client()
    now_utc = datetime.now(timezone.utc)
    games = iter_sports_game_targets(client)
    start_overrides = parse_event_overrides_with_start()
    if not games:
        return idle_sec

    for event_ticker, sport, all_mk in games:
        if not all_mk:
            continue
        ladders = group_markets_into_surface_ladders(all_mk)
        for (et, st, shard), ms in ladders.items():
            if et != event_ticker:
                continue
            try:
                _surface_monitor_once(
                    client,
                    dry_run,
                    now_utc,
                    str(et),
                    sport,
                    str(st),
                    ms,
                    ladder_shard=str(shard or ""),
                )
            except Exception:
                LOG.exception("sports surface failed %s %s", et, st)

    if any_sports_trading_window_open(now_utc, games, start_overrides=start_overrides):
        return window_sec
    return idle_sec


def run_sports_forever(*, dry_run: bool = False, interval_sec: float = 120.0) -> None:
    init_db()
    try:
        from vol_surface_strategy.panel_state import init_panel_db

        init_panel_db()
    except Exception:
        LOG.debug("panel db init failed", exc_info=True)
    LOG.info(
        "Sports vol-surface runner started dry_run=%s idle_poll=%ss in_window_poll=%s (env override)",
        dry_run,
        (os.environ.get("VOL_SPORTS_POLL_IDLE_SEC") or str(interval_sec)).strip() or interval_sec,
        (os.environ.get("VOL_SPORTS_POLL_IN_WINDOW_SEC") or "300").strip() or "300",
    )
    while True:
        try:
            sleep_sec = tick_sports(dry_run)
        except Exception:
            LOG.exception("tick_sports failed")
            sleep_sec = float((os.environ.get("VOL_SPORTS_POLL_IDLE_SEC") or "120").strip() or "120")
        time.sleep(max(15.0, sleep_sec))
