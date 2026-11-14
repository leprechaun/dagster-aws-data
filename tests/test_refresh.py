from datetime import datetime, timedelta, timezone

from aws_data.lib.cloudfront_s3 import CLOUDFRONT_PARTITIONS_DEF
from aws_data.lib.cloudtrail_s3 import CLOUDTRAIL_PARTITIONS_DEF
from aws_data.lib.cur_s3 import CUR_PARTITIONS_DEF
from aws_data.lib.refresh import billing_periods_to_refresh, partitions_to_refresh


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def test_the_latest_two_days_are_yesterday_and_today():
    assert partitions_to_refresh(utc(2026, 11, 13, 10, 20), CLOUDTRAIL_PARTITIONS_DEF) == ["2026-11-12", "2026-11-13"]


def test_days_are_utc_days_whatever_time_zone_the_tick_arrives_in():
    sydney = timezone(timedelta(hours=10))
    just_after_midnight_in_sydney = datetime(2026, 11, 14, 8, 20, tzinfo=sydney)  # 22:20 on the 13th UTC

    assert partitions_to_refresh(just_after_midnight_in_sydney, CLOUDTRAIL_PARTITIONS_DEF) == ["2026-11-12", "2026-11-13"]


def test_days_before_the_sources_first_partition_are_dropped():
    assert partitions_to_refresh(utc(2026, 11, 13, 10, 40), CLOUDFRONT_PARTITIONS_DEF) == ["2026-11-13"]
    assert partitions_to_refresh(utc(2026, 11, 14, 0, 40), CLOUDFRONT_PARTITIONS_DEF) == ["2026-11-13", "2026-11-14"]


def test_the_latest_two_billing_periods_are_last_month_and_this_month():
    assert billing_periods_to_refresh(utc(2026, 11, 14, 10, 0), CUR_PARTITIONS_DEF) == ["2026-10-01", "2026-11-01"]


def test_the_billing_period_before_january_is_december_of_the_year_before():
    assert billing_periods_to_refresh(utc(2026, 1, 1, 0, 0), CUR_PARTITIONS_DEF) == ["2025-12-01", "2026-01-01"]


def test_billing_periods_are_utc_months_whatever_time_zone_the_tick_arrives_in():
    sydney = timezone(timedelta(hours=10))
    just_after_month_end_in_sydney = datetime(2026, 12, 1, 8, 0, tzinfo=sydney)  # 22:00 on 30 November UTC

    assert billing_periods_to_refresh(just_after_month_end_in_sydney, CUR_PARTITIONS_DEF) == ["2026-10-01", "2026-11-01"]


def test_billing_periods_before_the_exports_first_one_are_dropped():
    assert billing_periods_to_refresh(utc(2022, 1, 20, 0, 0), CUR_PARTITIONS_DEF) == ["2022-01-01"]
