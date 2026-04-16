"""5-minute monitoring loop: BTC hourly + weather HIGH/LOW within trading windows."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Set, Tuple, cast

from zoneinfo import ZoneInfo

from vol_surface_strategy.analysis import run_scan
from vol_surface_strategy.config import CITIES
from vol_surface_strategy.discovery import (
    WeatherKind,
    discover_btc_hourly_markets,
    discover_weather_markets,
    resolution_date_for_high_scan,
    resolution_date_for_low_scan,
)
from vol_surface_strategy.exec_orders import cancel_order, place_buy, resting_order_limit_cents
from vol_surface_strategy.kalshi_io import (
    fetch_order,
    load_client,
    net_contracts_for_ticker,
    portfolio_total_cents,
)
from vol_surface_strategy.logutil import prune_old_logs, setup_logging
from vol_surface_strategy.market_utils import contract_from_market
from vol_surface_strategy.trading_windows import (
    btc_close_tick,
    btc_in_trading_window,
    btc_order_expiration_ts,
    btc_should_monitor,
    minutes_until_btc_hour_end,
    minutes_until_weather_high_end,
    minutes_until_weather_low_end,
    normalize_tracker_status,
    resting_order_expiration_ts,
    weather_high_close_tick,
    weather_high_in_window,
    weather_high_should_monitor,
    weather_low_close_tick,
    weather_low_in_window,
    weather_low_should_monitor,
    weather_high_order_expiration_ts,
    weather_low_order_expiration_ts,
)
from vol_surface_strategy.tracker import (
    btc_key,
    deployed_cents_total,
    get_row,
    init_db,
    update_status,
    upsert_pending,
    weather_key,
)

LOG = setup_logging()


def _btc_hourly_enabled() -> bool:
    """Set ``VOL_SURFACE_SKIP_BTC_HOURLY=1`` (or ``true``/``yes``/``on``) to run weather only."""
    v = (os.environ.get("VOL_SURFACE_SKIP_BTC_HOURLY") or "").strip().lower()
    return v not in ("1", "true", "yes", "on")


def _market_type_for_key(key: str) -> str:
    if key.startswith("btc:"):
        return "btc_hourly"
    if key.startswith("w:"):
        parts = key.split(":")
        if len(parts) >= 3:
            return "weather_high" if parts[2].upper() == "HIGH" else "weather_low"
    return "unknown"


def _panel_record_scan(key: str, res: Any) -> None:
    try:
        from vol_surface_strategy.panel_state import record_last_scan

        record_last_scan(key, market_type=_market_type_for_key(key), res=res)
    except Exception:
        LOG.debug("panel record_last_scan failed", exc_info=True)


def _panel_order_event(
    key: str,
    event: str,
    *,
    ticker: Optional[str] = None,
    side: Optional[str] = None,
    contracts: Optional[int] = None,
    price_cents: Optional[int] = None,
    order_id: Optional[str] = None,
    detail: Optional[str] = None,
) -> None:
    try:
        from vol_surface_strategy.panel_state import record_order_event

        record_order_event(
            key,
            event,
            ticker=ticker,
            side=side,
            contracts=contracts,
            price_cents=price_cents,
            order_id=order_id,
            detail=detail,
        )
    except Exception:
        LOG.debug("panel record_order_event failed", exc_info=True)


def _panel_upsert_open_trade(key: str, row: Any) -> None:
    try:
        from vol_surface_strategy.panel_state import upsert_open_trade

        if row.ticker and row.side and row.contracts and row.entry_cents:
            upsert_open_trade(
                key,
                ticker=str(row.ticker),
                market_type=_market_type_for_key(key),
                side=str(row.side),
                contracts=int(row.contracts),
                entry_cents=int(row.entry_cents),
            )
    except Exception:
        LOG.debug("panel upsert_open_trade failed", exc_info=True)

# (key, year, month, day, hour, minute) -> once per 5-min slot
_FIRED: Set[Tuple[str, int, int, int, int, int]] = set()


def _dedupe_monitor(key: str, when: datetime) -> bool:
    u = when.astimezone(timezone.utc)
    t = (key, u.year, u.month, u.day, u.hour, u.minute)
    if t in _FIRED:
        return False
    _FIRED.add(t)
    if len(_FIRED) > 5000:
        _FIRED.clear()
    return True


def _log_scan_result(prefix: str, res: Any) -> None:
    payload = {
        "ok": res.ok,
        "action": res.action,
        "reason": res.reason,
        "outlier": res.outlier_ticker,
        "sigma_star": res.sigma_star,
        "underlying": res.underlying,
        "edge_cents": res.edge_cents,
        "side": res.side,
        "entry_cents": res.entry_cents,
        "contracts": res.contracts_to_buy,
        "gates": res.gate_log,
    }
    LOG.info("%s %s", prefix, json.dumps(payload, default=str))


def _contracts_from_markets(markets: list[Any], kind: str) -> list[Any]:
    out = []
    for m in markets:
        c = contract_from_market(m, kind=kind)
        if c:
            out.append(c)
    return out


def _order_status_lower(o: Any) -> str:
    raw = getattr(getattr(o, "status", None), "value", None) or getattr(o, "status", "") or ""
    return str(raw).lower()


def _sync_resting_order_tracker(
    client: Any,
    key: str,
    row: Any,
    st: str,
) -> tuple[Any, str]:
    """
    If SQLite says ``order_resting`` but the exchange order is gone or terminal (canceled/expired),
    clear resting state so the bot can post again. Scans can look fine while the tracker was stuck
    forever in ``order_resting`` / ``order_unchanged`` without placing.
    """
    stn = normalize_tracker_status(st)
    if stn != "order_resting" or not row or not row.order_id:
        return row, stn

    oid = str(row.order_id)
    try:
        o = fetch_order(client, oid)
        ost = _order_status_lower(o)
    except Exception as e:
        LOG.warning(
            "sync resting: get_order %s failed (%s); clearing stale order_resting state",
            oid,
            e,
        )
        update_status(
            key,
            "window_open",
            order_id=None,
            ticker=None,
            side=None,
            entry_cents=None,
            contracts=None,
            deployed_cents=0,
        )
        r2 = get_row(key)
        return r2, "window_open" if r2 else "outside_window"

    if ost == "executed":
        update_status(key, "position_active")
        r2 = get_row(key)
        if r2:
            _panel_upsert_open_trade(key, r2)
        return r2, "position_active"

    if ost in ("resting", "pending"):
        return row, "order_resting"

    LOG.info(
        "sync resting: order %s terminal status=%s — resetting to window_open (can re-place)",
        oid,
        ost or "?",
    )
    update_status(
        key,
        "window_open",
        order_id=None,
        ticker=None,
        side=None,
        entry_cents=None,
        contracts=None,
        deployed_cents=0,
    )
    r2 = get_row(key)
    return r2, "window_open" if r2 else "outside_window"


def _monitor_log(
    market_id: str,
    state: str,
    time_remaining_min: float,
    fill_check: str,
    res: Any,
    action: str,
) -> None:
    sig = res.sigma_star if res.sigma_star is not None else None
    und = res.underlying if res.underlying is not None else None
    edge = res.edge_cents if res.edge_cents is not None else None
    tk = res.outlier_ticker or ""
    ent = res.entry_cents if res.entry_cents is not None else None
    LOG.info(
        "[MONITOR] %s state=%s time_remaining=%.1fmin\n"
        "  → fill_check: %s\n"
        "  → recalc: sigma*=%s mu*=%s edge=%s¢ optimal_ticker=%s optimal_entry=%s¢\n"
        "  → action: %s",
        market_id,
        state,
        time_remaining_min,
        fill_check,
        sig,
        und,
        edge,
        tk,
        ent,
        action,
    )


def _btc_market_id(hour_start: datetime) -> str:
    return f"BTC {hour_start.astimezone(timezone.utc).strftime('%Y-%m-%d %H:00')} UTC"


def _resolve_prev_btc_hour(now_utc: datetime) -> None:
    if now_utc.minute != 0 or now_utc.second >= 28:
        return
    prev = (now_utc - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    key = btc_key(prev)
    row = get_row(key)
    if not row:
        return
    st = normalize_tracker_status(row.status)
    if st not in ("resolved",):
        update_status(key, "resolved")


def _force_close_key(client: Any, dry_run: bool, key: str, market_id: str, reason: str) -> None:
    row = get_row(key)
    if not row:
        return
    st = normalize_tracker_status(row.status)
    if st == "order_resting" and row.order_id:
        if not dry_run:
            try:
                cancel_order(client, str(row.order_id))
                LOG.info("[MONITOR] %s → action: order_cancelled (%s)", market_id, reason)
            except Exception as e:
                LOG.warning("cancel failed %s: %s", key, e)
        update_status(key, "window_closed", order_id=None)
    elif st == "window_open":
        update_status(key, "window_closed")
    elif st not in ("position_active", "resolved", "outside_window", "window_closed"):
        update_status(key, "window_closed")


def _btc_snap_close(client: Any, dry_run: bool, now_utc: datetime) -> None:
    if not btc_close_tick(now_utc):
        return
    hour_start = now_utc.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    key = btc_key(hour_start)
    _force_close_key(client, dry_run, key, _btc_market_id(hour_start), "BTC window :45 close")


def _weather_snap_high_closes(client: Any, dry_run: bool, now_utc: datetime) -> None:
    for cid in CITIES:
        tz = ZoneInfo(CITIES[cid].tz_name)
        loc = now_utc.astimezone(tz)
        if not weather_high_close_tick(cid, loc):
            continue
        res_date = resolution_date_for_high_scan(loc)
        key = weather_key(cid, "HIGH", res_date)
        mid = f"{cid} HIGH {res_date}"
        _force_close_key(client, dry_run, key, mid, "weather HIGH window close")


def _weather_snap_low_closes(client: Any, dry_run: bool, now_utc: datetime) -> None:
    for cid in CITIES:
        tz = ZoneInfo(CITIES[cid].tz_name)
        loc = now_utc.astimezone(tz)
        if not weather_low_close_tick(loc):
            continue
        res_date = resolution_date_for_low_scan(loc)
        key = weather_key(cid, "LOW", res_date)
        mid = f"{cid} LOW {res_date}"
        _force_close_key(client, dry_run, key, mid, "weather LOW window close")


def _btc_monitor_cycle(client: Any, dry_run: bool, now_utc: datetime) -> None:
    if not btc_should_monitor(now_utc) or not btc_in_trading_window(now_utc):
        return
    hour_start = now_utc.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    hour_end = hour_start + timedelta(hours=1)
    key = btc_key(hour_start)
    mid = _btc_market_id(hour_start)
    if not _dedupe_monitor(f"btc:{key}", now_utc):
        return

    t_rem = minutes_until_btc_hour_end(now_utc)
    row = get_row(key)
    st = normalize_tracker_status(row.status) if row else "outside_window"
    if row is None:
        upsert_pending(key, market_type="btc_hourly", hour_start_utc=hour_start.isoformat(), status="window_open")
        row = get_row(key)
        st = "window_open"

    if st == "order_resting" and row and row.order_id:
        row, st = _sync_resting_order_tracker(client, key, row, st)

    if st == "position_active":
        _monitor_log(mid, st, t_rem, "filled", ScanResultStub(), "no_change")
        return

    if st == "resolved" or st == "window_closed":
        return

    if row and row.ticker and net_contracts_for_ticker(client, row.ticker) >= 1.0:
        update_status(key, "position_active")
        row2 = get_row(key)
        if row2:
            _panel_upsert_open_trade(key, row2)
        _monitor_log(mid, "position_active", t_rem, "filled", ScanResultStub(), "position_confirmed")
        return

    markets = discover_btc_hourly_markets(client, hour_end_utc=hour_end)
    raw = _contracts_from_markets(markets, "btc")
    if not raw:
        LOG.warning("BTC hourly: no markets for hour_end=%s", hour_end.isoformat())
        return

    total = portfolio_total_cents(client)
    _, dep_frac = deployed_cents_total(total)
    res = run_scan(
        raw,
        model="lognormal",
        t_resolve=hour_end,
        now=now_utc,
        portfolio_cents=total,
        deployed_fraction=dep_frac,
    )
    _log_scan_result("BTC monitor", res)
    _panel_record_scan(key, res)

    fill_check = "unfilled"
    action = "no_change"

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
                _monitor_log(mid, "position_active", t_rem, "filled", res, "position_confirmed")
                return
        except Exception as e:
            LOG.warning("BTC get_order %s: %s", oid, e)

        if res.action != "trade":
            if not dry_run:
                try:
                    cancel_order(client, oid)
                except Exception as e:
                    LOG.warning("BTC cancel %s: %s", oid, e)
            update_status(key, "window_open", order_id=None)
            LOG.info("no_edge_found %s", mid)
            _monitor_log(mid, st, t_rem, fill_check, res, "order_cancelled")
            return

        if o is None:
            LOG.warning("BTC order_resting: no API snapshot for %s; skip cycle", oid)
            return

        lim = resting_order_limit_cents(o, str(row.side or ""))
        price_changed = res.entry_cents is not None and lim is not None and res.entry_cents != lim
        ticker_changed = res.outlier_ticker != row.ticker
        if ticker_changed or price_changed:
            if not dry_run:
                try:
                    cancel_order(client, oid)
                except Exception as e:
                    LOG.warning("BTC cancel repricing %s: %s", oid, e)
            exp = resting_order_expiration_ts(
                now_utc, latest_allowed_ts=btc_order_expiration_ts(now_utc)
            )
            action = "order_updated"
            if dry_run:
                LOG.info("DRY_RUN would replace BTC order %s x%d @ %d", res.side, res.contracts_to_buy, res.entry_cents)
            else:
                try:
                    r = place_buy(
                        client,
                        ticker=res.outlier_ticker or "",
                        side=res.side or "yes",
                        count=res.contracts_to_buy,
                        price_cents=int(res.entry_cents or 0),
                        expiration_ts=exp,
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
                    LOG.info(
                        "order_updated old=%s new=%s old_price=%s¢ new_price=%s¢",
                        row.ticker,
                        res.outlier_ticker,
                        lim,
                        res.entry_cents,
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
                    LOG.exception("BTC replace order failed: %s", e)
                    return
            _monitor_log(mid, "order_resting", t_rem, fill_check, res, action)
            return

        action = "order_unchanged"
        LOG.info("order_unchanged ticker=%s price=%s¢", row.ticker, lim)
        _monitor_log(mid, st, t_rem, fill_check, res, action)
        return

    # window_open: post if trade
    if res.action == "trade" and res.side and res.entry_cents and res.contracts_to_buy:
        exp = resting_order_expiration_ts(
            now_utc, latest_allowed_ts=btc_order_expiration_ts(now_utc)
        )
        if dry_run:
            LOG.info("DRY_RUN would place BTC %s x%d @ %d", res.side, res.contracts_to_buy, res.entry_cents)
            action = "order_posted"
        else:
            try:
                r = place_buy(
                    client,
                    ticker=res.outlier_ticker or "",
                    side=res.side,
                    count=res.contracts_to_buy,
                    price_cents=res.entry_cents,
                    expiration_ts=exp,
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
                action = "order_posted"
                LOG.info("BTC order placed %s", oid)
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
                LOG.exception("BTC order failed: %s", e)
                return
        _monitor_log(mid, "order_resting", t_rem, fill_check, res, action)
        return

    LOG.info("no_edge_found %s", mid)
    _monitor_log(mid, st, t_rem, fill_check, res, "no_change")


class ScanResultStub:
    """Placeholder for monitor logs when skipping recalc details."""

    sigma_star = None
    underlying = None
    edge_cents = None
    outlier_ticker = ""
    entry_cents = None


def _weather_monitor_cycle(
    client: Any,
    dry_run: bool,
    now_utc: datetime,
    city_id: str,
    kind: str,
) -> None:
    city = CITIES[city_id]
    tz = ZoneInfo(city.tz_name)
    local = now_utc.astimezone(tz)

    if kind == "HIGH":
        if not weather_high_in_window(city_id, local) or not weather_high_should_monitor(city_id, local):
            return
        res_date = resolution_date_for_high_scan(local)
        t_rem = minutes_until_weather_high_end(city_id, local)
    else:
        if not weather_low_in_window(local) or not weather_low_should_monitor(local):
            return
        res_date = resolution_date_for_low_scan(local)
        t_rem = minutes_until_weather_low_end(local)

    key = weather_key(city_id, kind, res_date)
    mid = f"{city_id} {kind} {res_date}"
    if not _dedupe_monitor(f"w:{key}", now_utc):
        return

    row = get_row(key)
    st = normalize_tracker_status(row.status) if row else "outside_window"
    if row is None:
        upsert_pending(
            key,
            market_type=f"weather_{kind.lower()}",
            city_id=city_id,
            resolution_date=res_date.isoformat(),
            status="window_open",
        )
        row = get_row(key)
        st = "window_open"

    if st == "order_resting" and row and row.order_id:
        row, st = _sync_resting_order_tracker(client, key, row, st)

    if st == "position_active":
        _monitor_log(mid, st, t_rem, "filled", ScanResultStub(), "no_change")
        return
    if st in ("resolved", "window_closed"):
        return

    if row and row.ticker and net_contracts_for_ticker(client, row.ticker) >= 1.0:
        update_status(key, "position_active")
        row2 = get_row(key)
        if row2:
            _panel_upsert_open_trade(key, row2)
        _monitor_log(mid, "position_active", t_rem, "filled", ScanResultStub(), "position_confirmed")
        return

    markets = discover_weather_markets(client, city, cast(WeatherKind, kind), res_date, tz)
    raw = _contracts_from_markets(markets, "weather")
    if not raw:
        LOG.warning("Weather %s %s %s: no markets", city_id, kind, res_date)
        return

    t_resolve = now_utc + timedelta(hours=12)
    if markets:
        ct = getattr(markets[0], "close_time", None)
        if isinstance(ct, str):
            if ct.endswith("Z"):
                ct = ct[:-1] + "+00:00"
            t_resolve = datetime.fromisoformat(ct)
            if t_resolve.tzinfo is None:
                t_resolve = t_resolve.replace(tzinfo=timezone.utc)

    total = portfolio_total_cents(client)
    _, dep_frac = deployed_cents_total(total)
    res = run_scan(
        raw,
        model="normal",
        t_resolve=t_resolve,
        now=now_utc,
        city_id=city_id,
        weather_temp_kind=kind if kind in ("HIGH", "LOW") else None,
        portfolio_cents=total,
        deployed_fraction=dep_frac,
    )
    _log_scan_result(f"{city_id} {kind} monitor", res)
    _panel_record_scan(key, res)

    fill_check = "unfilled"
    exp_ts = resting_order_expiration_ts(
        now_utc,
        latest_allowed_ts=(
            weather_high_order_expiration_ts(res_date, city_id)
            if kind == "HIGH"
            else weather_low_order_expiration_ts(res_date, city_id)
        ),
    )

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
                _monitor_log(mid, "position_active", t_rem, "filled", res, "position_confirmed")
                return
        except Exception as e:
            LOG.warning("Weather get_order %s: %s", oid, e)

        if res.action != "trade":
            if not dry_run:
                try:
                    cancel_order(client, oid)
                except Exception as e:
                    LOG.warning("Weather cancel %s: %s", oid, e)
            update_status(key, "window_open", order_id=None)
            LOG.info("no_edge_found %s", mid)
            _monitor_log(mid, st, t_rem, fill_check, res, "order_cancelled")
            return

        if o is None:
            LOG.warning("Weather order_resting: no API snapshot for %s; skip cycle", oid)
            return

        lim = resting_order_limit_cents(o, str(row.side or ""))
        price_changed = res.entry_cents is not None and lim is not None and res.entry_cents != lim
        ticker_changed = res.outlier_ticker != row.ticker
        if ticker_changed or price_changed:
            if not dry_run:
                try:
                    cancel_order(client, oid)
                except Exception as e:
                    LOG.warning("Weather cancel repricing %s: %s", oid, e)
            if dry_run:
                LOG.info(
                    "DRY_RUN would replace %s order %s x%d @ %d",
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
                    LOG.info(
                        "order_updated old=%s new=%s old_price=%s¢ new_price=%s¢",
                        row.ticker,
                        res.outlier_ticker,
                        lim,
                        res.entry_cents,
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
                    LOG.exception("Weather replace order failed: %s", e)
                    return
            _monitor_log(mid, "order_resting", t_rem, fill_check, res, "order_updated")
            return

        LOG.info("order_unchanged ticker=%s price=%s¢", row.ticker, lim)
        _monitor_log(mid, st, t_rem, fill_check, res, "order_unchanged")
        return

    if res.action == "trade" and res.side and res.entry_cents and res.contracts_to_buy:
        if dry_run:
            LOG.info(
                "DRY_RUN would place %s %s x%d @ %d",
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
                LOG.info("Weather order placed %s %s", mid, oid)
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
                LOG.exception("Weather order failed %s: %s", mid, e)
                return
        _monitor_log(mid, "order_resting", t_rem, fill_check, res, "order_posted")
        return

    LOG.info("no_edge_found %s", mid)
    _monitor_log(mid, st, t_rem, fill_check, res, "no_change")


def tick(dry_run: bool) -> None:
    client = load_client()
    now = datetime.now(timezone.utc)

    if now.minute == 0 and now.second < 28:
        prune_old_logs(90)

    if _btc_hourly_enabled():
        _resolve_prev_btc_hour(now)
    _weather_snap_high_closes(client, dry_run, now)
    _weather_snap_low_closes(client, dry_run, now)
    if _btc_hourly_enabled():
        _btc_snap_close(client, dry_run, now)
        _btc_monitor_cycle(client, dry_run, now)

    for cid in CITIES:
        _weather_monitor_cycle(client, dry_run, now, cid, "HIGH")
        _weather_monitor_cycle(client, dry_run, now, cid, "LOW")


def run_forever(*, dry_run: bool = False, interval_sec: float = 12.0) -> None:
    init_db()
    try:
        from vol_surface_strategy.panel_state import init_panel_db

        init_panel_db()
    except Exception:
        LOG.debug("panel db init failed", exc_info=True)
    LOG.info(
        "Vol surface runner started dry_run=%s (5-min monitor loop)%s",
        dry_run,
        " [BTC hourly disabled: VOL_SURFACE_SKIP_BTC_HOURLY]" if not _btc_hourly_enabled() else "",
    )
    while True:
        try:
            tick(dry_run)
        except Exception:
            LOG.exception("tick failed")
        time.sleep(interval_sec)
