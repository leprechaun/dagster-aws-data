from datetime import datetime, timezone

from aws_data.lib.cur_s3 import (
    CUR_PARTITIONS_DEF,
    billing_period,
    billing_period_prefix,
)
from aws_data.lib.s3 import ObjectInfo, keys_to_copy, stale_keys

FIRST = ObjectInfo(size=10, etag='"aaa"')
RESTATED = ObjectInfo(size=10, etag='"bbb"')
RESIZED = ObjectInfo(size=12, etag='"aaa"')


def test_a_monthly_partition_key_is_its_billing_period():
    assert billing_period("2026-11-01") == "2026-11"


def test_a_billing_periods_prefix_is_its_data_folder_in_the_export():
    assert billing_period_prefix("exports/cur", "2022-01") == "exports/cur/data/BILLING_PERIOD=2022-01/"


def _at(day: str) -> datetime:
    return datetime.fromisoformat(day).replace(tzinfo=timezone.utc)


def test_billing_periods_run_from_the_first_export_month_and_include_the_one_in_progress():
    keys = CUR_PARTITIONS_DEF.get_partition_keys(current_time=_at("2022-03-15"))

    assert keys == ["2022-01-01", "2022-02-01", "2022-03-01"]


def test_new_and_changed_files_are_copied_and_identical_ones_are_not():
    source = {"new": FIRST, "same": FIRST, "restated": RESTATED, "resized": RESIZED}
    dest = {"same": FIRST, "restated": FIRST, "resized": FIRST}

    assert keys_to_copy(source, dest) == ["new", "resized", "restated"]


def test_only_files_missing_from_the_source_are_stale():
    source = {"kept": FIRST}
    dest = {"kept": FIRST, "gone": FIRST, "also-gone": FIRST}

    assert stale_keys(source, dest) == ["also-gone", "gone"]
