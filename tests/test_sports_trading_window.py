"""Pre-game trading window: T−3h through T−10m (minutes_to_start in (10, 180])."""

from __future__ import annotations

from datetime import datetime, timedelta

from vol_surface_strategy.sports_windows import ET, in_pre_game_order_window


def test_in_window_mid_range() -> None:
    gs = datetime(2026, 6, 1, 19, 0, tzinfo=ET)
    now = gs - timedelta(minutes=60)
    ok, mins = in_pre_game_order_window("MLB", now, gs)
    assert ok
    assert abs(mins - 60.0) < 0.01


def test_outside_window_too_close() -> None:
    gs = datetime(2026, 6, 1, 19, 0, tzinfo=ET)
    now = gs - timedelta(minutes=5)
    ok, mins = in_pre_game_order_window("NBA", now, gs)
    assert not ok
    assert mins <= 10


def test_outside_window_too_early() -> None:
    gs = datetime(2026, 6, 1, 19, 0, tzinfo=ET)
    now = gs - timedelta(minutes=200)
    ok, mins = in_pre_game_order_window("NHL", now, gs)
    assert not ok
    assert mins > 180


def test_boundary_eleven_minutes_ok() -> None:
    gs = datetime(2026, 6, 1, 19, 0, tzinfo=ET)
    now = gs - timedelta(minutes=11)
    ok, _ = in_pre_game_order_window("MLS", now, gs)
    assert ok


def test_boundary_ten_minutes_closed() -> None:
    gs = datetime(2026, 6, 1, 19, 0, tzinfo=ET)
    now = gs - timedelta(minutes=10)
    ok, _ = in_pre_game_order_window("MLS", now, gs)
    assert not ok
