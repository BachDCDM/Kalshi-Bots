"""Scheduling loop: BTC hourly + weather HIGH/LOW scans."""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Set, Tuple
from zoneinfo import ZoneInfo

from vol_surface_strategy.analysis import run_scan
from vol_surface_strategy.config import CITIES, LOW_CUTOFF_LOCAL, LOW_FIRST_LOCAL, LOW_SECOND_LOCAL
from vol_surface_strategy.discovery import (
    discover_btc_hourly_markets,
    discover_weather_markets,
    resolution_date_for_high_scan,
    resolution_date_for_low_scan,
)
from vol_surface_strategy.exec_orders import place_buy
from vol_surface_strategy.kalshi_io import load_client, portfolio_total_cents
from vol_surface_strategy.logutil import prune_old_logs, setup_logging
from vol_surface_strategy.market_utils import contract_from_market
from vol_surface_strategy.tracker import (
    btc_key,
    deployed_cents_total,
    get_row,
    init_db,
    list_btc_resting,
    update_status,
    upsert_pending,
    weather_key,
)

LOG = setup_logging()

# (year, month, day, hour, minute) -> dedupe
_FIRED: Set[Tuple[str, int, int, int, int, int]] = set()


def _dedupe(key: str, when: datetime) -> bool:
    u = when.astimezone(timezone.utc)
    t = (key, u.year, u.month, u.day, u.hour, u.minute)
    if t in _FIRED:
        return False
    _FIRED.add(t)
    if len(_FIRED) > 5000:
        _FIRED.clear()
    return True


def _in_minute_window(local_dt: datetime, hour: int, minute: int) -> bool:
    return local_dt.hour == hour and local_dt.minute == minute and local_dt.second < 28


def _past_local_time(local_dt: datetime, hour: int, minute: int) -> bool:
    t = local_dt.replace(hour=hour, minute=minute, second=0, microsecond=0)
    return local_dt >= t


def _low_order_expiry_ts(local_now: datetime) -> int:
    cand = local_now.replace(hour=LOW_CUTOFF_LOCAL[0], minute=LOW_CUTOFF_LOCAL[1], second=0, microsecond=0)
    if local_now >= cand:
        cand = cand + timedelta(days=1)
    return int(cand.astimezone(timezone.utc).timestamp())


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


def _run_btc_scan(client: Any, dry_run: bool, scan: str) -> None:
    now = datetime.now(timezone.utc)
    if now.minute >= 55:
        LOG.info("BTC skip: minute >= 55")
        return
    hour_start = now.replace(minute=0, second=0, microsecond=0)
    hour_end = hour_start + timedelta(hours=1)
    key = btc_key(hour_start)
    row = get_row(key)
    if row and row.status not in ("pending_first_scan", "pending_second_scan"):
        return
    if scan == "first" and row and row.status != "pending_first_scan":
        return
    if scan == "second" and row and row.status != "pending_second_scan":
        return

    if not _dedupe(f"btc:{scan}", now):
        return

    if row is None:
        upsert_pending(
            key,
            market_type="btc_hourly",
            hour_start_utc=hour_start.isoformat(),
            status="pending_first_scan",
        )

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
        now=now,
        portfolio_cents=total,
        deployed_fraction=dep_frac,
    )
    _log_scan_result(f"BTC {scan}", res)

    if res.action == "trade" and res.side and res.entry_cents and res.contracts_to_buy:
        exp = int(now.timestamp()) + 5 * 60
        if dry_run:
            LOG.info("DRY_RUN would place BTC %s x%d @ %d", res.side, res.contracts_to_buy, res.entry_cents)
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
                LOG.info("BTC order placed %s", oid)
            except Exception as e:
                LOG.exception("BTC order failed: %s", e)
        return

    if scan == "first":
        update_status(key, "pending_second_scan")
    elif scan == "second":
        update_status(key, "expired")


def _weather_scan(
    client: Any,
    city_id: str,
    kind: str,
    scan: str,
    dry_run: bool,
) -> None:
    city = CITIES[city_id]
    tz = ZoneInfo(city.tz_name)
    now_utc = datetime.now(timezone.utc)
    local = now_utc.astimezone(tz)

    if kind == "HIGH":
        res_date = resolution_date_for_high_scan(local)
        if city.high_second is None:
            if scan == "second":
                return
        if scan == "first":
            h, m = city.high_first
            if not _in_minute_window(local, h, m):
                return
        else:
            if city.high_second is None:
                return
            h, m = city.high_second
            if not _in_minute_window(local, h, m):
                return
        ch, cm = city.high_cutoff
        if _past_local_time(local, ch, cm):
            return
    else:
        res_date = resolution_date_for_low_scan(local)
        if scan == "first":
            h, m = LOW_FIRST_LOCAL
            if not _in_minute_window(local, h, m):
                return
        else:
            h, m = LOW_SECOND_LOCAL
            if not _in_minute_window(local, h, m):
                return
        lh, lm = LOW_CUTOFF_LOCAL
        if _past_local_time(local, lh, lm):
            return

    key = weather_key(city_id, kind, res_date)
    if not _dedupe(f"{key}:{scan}", now_utc):
        return

    row = get_row(key)
    if row and row.status not in ("pending_first_scan", "pending_second_scan"):
        return
    if scan == "first" and row and row.status != "pending_first_scan":
        return
    if scan == "second" and row and row.status != "pending_second_scan":
        return

    if row is None:
        upsert_pending(
            key,
            market_type=f"weather_{kind.lower()}",
            city_id=city_id,
            resolution_date=res_date.isoformat(),
            status="pending_first_scan",
        )

    markets = discover_weather_markets(client, city, kind, res_date, tz)  # type: ignore[arg-type]
    raw = _contracts_from_markets(markets, "weather")
    if not raw:
        LOG.warning("Weather %s %s %s: no markets", city_id, kind, res_date)
        return

    # next morning ~ close — use last market close as resolve proxy
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
        portfolio_cents=total,
        deployed_fraction=dep_frac,
    )
    _log_scan_result(f"{city_id} {kind} {scan}", res)

    if res.action == "trade" and res.side and res.entry_cents and res.contracts_to_buy:
        if kind == "HIGH":
            exp = int(now_utc.timestamp()) + 10 * 60
        else:
            exp = _low_order_expiry_ts(local)
        if dry_run:
            LOG.info(
                "DRY_RUN would place %s %s %s x%d @ %d",
                city_id,
                kind,
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
            except Exception as e:
                LOG.exception("Weather order failed %s %s: %s", city_id, kind, e)
        return

    if scan == "first":
        update_status(key, "pending_second_scan")
    elif scan == "second":
        update_status(key, "expired")


def _btc_expiration_sweep(client: Any) -> None:
    """At minute 50, confirm resting BTC hourly orders cleared after Kalshi expiration."""
    for r in list_btc_resting():
        oid = r["order_id"]
        if not oid:
            continue
        try:
            o = client.get_order(order_id=oid).order
            st = getattr(o, "status", None)
            LOG.info("BTC sweep order %s status=%s", oid, st)
            if st in ("canceled", "expired"):
                update_status(r["key"], "expired")
            elif st == "executed":
                update_status(r["key"], "filled")
        except Exception as e:
            LOG.warning("BTC sweep get_order %s: %s", oid, e)


def tick(dry_run: bool) -> None:
    client = load_client()
    now = datetime.now(timezone.utc)

    if now.minute == 0 and now.second < 20:
        prune_old_logs(90)

    # BTC hourly
    if 10 <= now.minute <= 54:
        if now.minute == 10:
            _run_btc_scan(client, dry_run, "first")
        elif now.minute == 30:
            _run_btc_scan(client, dry_run, "second")
    if now.minute == 50 and now.second < 25:
        _btc_expiration_sweep(client)

    for cid in CITIES:
        _weather_scan(client, cid, "HIGH", "first", dry_run)
        _weather_scan(client, cid, "HIGH", "second", dry_run)
        _weather_scan(client, cid, "LOW", "first", dry_run)
        _weather_scan(client, cid, "LOW", "second", dry_run)


def run_forever(*, dry_run: bool = False, interval_sec: float = 12.0) -> None:
    init_db()
    LOG.info("Vol surface runner started dry_run=%s", dry_run)
    while True:
        try:
            tick(dry_run)
        except Exception:
            LOG.exception("tick failed")
        time.sleep(interval_sec)
