from datetime import datetime, timezone

import polars as pl

from aws_data.lib.cloudtrail_activity import (
    ALL,
    GRAINS,
    NONE,
    Grain,
    activity,
    activity_at_grain,
    prepare_records,
)

FIFTEEN_MINUTES = Grain("15m", "15m", ("caller", "event_name"))
DAILY_BY_CALLER = Grain("1d", "1d", ("caller",))


def at(day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 11, day, hour, minute, tzinfo=timezone.utc)


def bronze(*rows: dict) -> pl.LazyFrame:
    """Bronze-shaped Records, with defaults for everything a test doesn't care about."""
    defaults = {
        "event_id": None,
        "event_time": at(12, 10),
        "user_identity_arn": "arn:aws:sts::1:assumed-role/admin/lmac",
        "event_source": "s3.amazonaws.com",
        "event_name": "GetObject",
        "recipient_account_id": "111111111111",
        "aws_region": "us-east-1",
        "error_code": None,
    }
    full = [{**defaults, "event_id": f"id-{i}", **row} for i, row in enumerate(rows)]
    return pl.LazyFrame(full, schema_overrides={"event_time": pl.Datetime("us", "UTC")})


def prepared(*rows: dict) -> pl.LazyFrame:
    return prepare_records(bronze(*rows))


def counts(frame: pl.LazyFrame, *keys: str) -> dict:
    """{(bucket_start, *keys): record_count}, to compare without caring about row order."""
    return {
        (row["bucket_start"], *(row[key] for key in keys)): row["record_count"]
        for row in frame.collect().iter_rows(named=True)
    }


def test_every_step_stays_lazy():
    assert isinstance(prepared({}), pl.LazyFrame)
    assert isinstance(activity_at_grain(prepared({}), FIFTEEN_MINUTES), pl.LazyFrame)
    assert isinstance(activity(prepared({})), pl.LazyFrame)


def test_records_are_counted_per_15_minute_bucket_and_per_key():
    rows = activity_at_grain(
        prepared(
            {"event_time": at(12, 10, 1), "event_name": "GetObject"},
            {"event_time": at(12, 10, 14), "event_name": "GetObject"},
            {"event_time": at(12, 10, 14), "event_name": "PutObject"},
            {"event_time": at(12, 10, 16), "event_name": "GetObject"},
        ),
        FIFTEEN_MINUTES,
    )

    caller = "arn:aws:sts::1:assumed-role/admin/lmac"
    assert counts(rows, "caller", "event_name") == {
        (at(12, 10, 0), caller, "GetObject"): 2,
        (at(12, 10, 0), caller, "PutObject"): 1,
        (at(12, 10, 15), caller, "GetObject"): 1,
    }


def test_a_bucket_includes_its_start_and_excludes_its_end():
    rows = activity_at_grain(
        prepared({"event_time": at(12, 10, 15)}, {"event_time": at(12, 10, 30)}),
        FIFTEEN_MINUTES,
    )

    assert sorted(counts(rows)) == [(at(12, 10, 15),), (at(12, 10, 30),)]


def test_a_quiet_bucket_has_no_row():
    rows = activity_at_grain(
        prepared({"event_time": at(12, 10, 0)}, {"event_time": at(12, 11, 0)}),
        FIFTEEN_MINUTES,
    )

    assert rows.collect().height == 2


def test_errors_are_counted_separately_from_records():
    rows = activity_at_grain(
        prepared(
            {"error_code": "AccessDenied"},
            {"error_code": "AccessDenied"},
            {"error_code": None},
        ),
        DAILY_BY_CALLER,
    ).collect()

    assert rows["record_count"].to_list() == [3]
    assert rows["error_count"].to_list() == [2]


def test_a_dimension_a_grain_leaves_out_reads_all_and_its_rows_are_summed():
    rows = activity_at_grain(
        prepared(
            {"event_name": "GetObject"},
            {"event_name": "PutObject"},
            {"event_name": "PutObject"},
        ),
        DAILY_BY_CALLER,
    ).collect()

    assert rows["record_count"].to_list() == [3]
    assert rows["event_name"].to_list() == [ALL]
    assert rows["account_id"].to_list() == [ALL]


def test_a_missing_caller_is_none_not_null():
    rows = activity_at_grain(
        prepared({"user_identity_arn": None}), DAILY_BY_CALLER
    ).collect()

    assert rows["caller"].to_list() == [NONE]


def test_a_record_delivered_twice_is_counted_once():
    duplicated = prepare_records(
        bronze({"event_id": "same"}, {"event_id": "same"}, {"event_id": "other"})
    )

    rows = activity_at_grain(duplicated, DAILY_BY_CALLER).collect()

    assert rows["record_count"].to_list() == [2]


def test_every_grain_adds_up_to_the_same_totals():
    records = prepared(
        {"event_time": at(12, 9, 59), "error_code": "AccessDenied"},
        {"event_time": at(12, 10, 0), "event_name": "PutObject"},
        {"event_time": at(12, 23, 59), "recipient_account_id": "222222222222"},
        {"event_time": at(13, 0, 0), "event_source": "iam.amazonaws.com"},
        {"event_time": at(13, 0, 1), "user_identity_arn": None},
    )

    rows = activity(records).collect()
    totals = rows.group_by("grain").agg(
        pl.col("record_count").sum(), pl.col("error_count").sum()
    )

    assert totals.height == len(GRAINS)
    assert set(totals["record_count"].to_list()) == {5}
    assert set(totals["error_count"].to_list()) == {1}


def test_all_grains_share_one_schema_and_name_themselves():
    rows = activity(prepared({})).collect()

    assert set(rows["grain"].to_list()) == {grain.name for grain in GRAINS}
    assert rows.columns[:2] == ["grain", "bucket_start"]
