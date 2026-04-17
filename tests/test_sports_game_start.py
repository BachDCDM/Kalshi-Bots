"""Sports game start: rules_primary vs occurrence_datetime − offset (not expiration_time)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from vol_surface_strategy.sports_windows import earliest_sports_game_start_utc, parse_sports_game_start_utc


def test_parse_rules_primary_scheduled_edt() -> None:
    rules = (
        "If ... originally scheduled for Apr 17, 2026 at 9:40 PM EDT "
        "and is not completed by Apr 18, 2026 at 12:40 AM EDT."
    )
    m = SimpleNamespace(rules_primary=rules, occurrence_datetime=None)
    u = parse_sports_game_start_utc(m, "MLB")
    assert u is not None
    # 9:40 PM EDT Apr 17, 2026 → 2026-04-18 01:40:00+00:00
    assert u == datetime(2026, 4, 18, 1, 40, tzinfo=timezone.utc)


def test_occurrence_minus_mlb_offset_when_no_rules() -> None:
    m = SimpleNamespace(
        rules_primary=None,
        rules_secondary=None,
        occurrence_datetime="2026-04-18T04:40:00+00:00",
    )
    u = parse_sports_game_start_utc(m, "MLB")
    assert u is not None
    assert u == datetime(2026, 4, 18, 1, 10, tzinfo=timezone.utc)


def test_rules_primary_beats_occurrence() -> None:
    rules = "scheduled for Apr 17, 2026 at 9:40 PM EDT"
    m = SimpleNamespace(
        rules_primary=rules,
        occurrence_datetime="2099-01-01T00:00:00Z",
    )
    u = parse_sports_game_start_utc(m, "NBA")
    assert u is not None
    assert u.year == 2026


def test_earliest_across_markets() -> None:
    m1 = SimpleNamespace(rules_primary=None, occurrence_datetime="2026-04-18T10:00:00Z")
    m2 = SimpleNamespace(rules_primary=None, occurrence_datetime="2026-04-18T04:40:00Z")
    u = earliest_sports_game_start_utc([m1, m2], "MLB")
    # m2 earlier occurrence → earlier start after offset
    u1 = parse_sports_game_start_utc(m1, "MLB")
    u2 = parse_sports_game_start_utc(m2, "MLB")
    assert u1 is not None and u2 is not None
    assert u == min(u1, u2)


def test_no_expiration_fields_used() -> None:
    """Wide administrative expiration must not affect start when rules parse."""
    rules = "scheduled for Jun 10, 2026 at 7:05 PM EDT"
    m = SimpleNamespace(
        rules_primary=rules,
        occurrence_datetime="2026-04-21T01:40:00Z",
        expiration_time="2026-04-24T01:40:00Z",
        close_time="2026-04-24T01:40:00Z",
    )
    u = parse_sports_game_start_utc(m, "MLB")
    assert u is not None
    assert u.month == 6
