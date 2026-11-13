from datetime import datetime, timedelta, timezone

from aws_data.lib.cloudfront_s3 import CLOUDFRONT_PARTITIONS_DEF
from aws_data.lib.cloudtrail_s3 import CLOUDTRAIL_PARTITIONS_DEF
from aws_data.lib.refresh import partitions_to_refresh


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
