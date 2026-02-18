from datetime import datetime

import pytest

from app.lib.common.bookmark_sync.main import CronMatcher


def test_cron_match_every_minute():
    matcher = CronMatcher("* * * * *")
    assert matcher.matches(datetime(2026, 2, 18, 10, 20, 0))


def test_cron_match_fixed_time():
    matcher = CronMatcher("30 3 * * *")
    assert matcher.matches(datetime(2026, 2, 18, 3, 30, 0))
    assert not matcher.matches(datetime(2026, 2, 18, 3, 31, 0))


def test_cron_match_step_and_range():
    matcher = CronMatcher("*/15 9-18 * * 1-5")
    assert matcher.matches(datetime(2026, 2, 18, 9, 0, 0))   # Wed
    assert matcher.matches(datetime(2026, 2, 18, 9, 15, 0))  # Wed
    assert not matcher.matches(datetime(2026, 2, 22, 9, 15, 0))  # Sun


def test_cron_match_weekday_7_as_sunday():
    matcher = CronMatcher("0 8 * * 7")
    assert matcher.matches(datetime(2026, 2, 22, 8, 0, 0))  # Sunday
    assert not matcher.matches(datetime(2026, 2, 18, 8, 0, 0))  # Wednesday


def test_cron_invalid():
    with pytest.raises(ValueError):
        CronMatcher("* * * *")
    with pytest.raises(ValueError):
        CronMatcher("61 * * * *")
