from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import polars as pl

ALL = "(all)"
NONE = "(none)"
READ_ONLY = "read-only"
CHANGE = "change"

# The bronze columns the activity table is built from (raw_json is the big one it skips).
RECORD_COLUMNS = (
    "event_id",
    "event_time",
    "user_identity_arn",
    "event_source",
    "event_name",
    "recipient_account_id",
    "aws_region",
    "error_code",
    "read_only",
)

KEYS = ("caller", "event_source", "event_name", "access", "account_id", "region", "error_code")


@dataclass(frozen=True)
class Grain:
    """One time grain of the activity table: the width of its buckets, and the
    keys it keeps. A key it leaves out reads `ALL`, so coarser grains can drop
    detail without a NULL meaning two things."""

    name: str
    every: str
    keys: tuple[str, ...]


GRAINS = (
    Grain("15m", "15m", KEYS),
    Grain("1h", "1h", ("caller", "event_source", "access", "account_id", "error_code")),
    Grain("1d", "1d", ("caller", "event_source", "access", "account_id")),
)


def prepare_records(records: pl.LazyFrame) -> pl.LazyFrame:
    """Bronze Records as the activity keys: each Record once (a Log File can be
    delivered twice), a missing key as `NONE`, and whether it was an error."""
    return (
        records.unique("event_id", keep="first", maintain_order=False)
        .select(
            "event_time",
            pl.col("user_identity_arn").alias("caller"),
            "event_source",
            "event_name",
            pl.col("recipient_account_id").alias("account_id"),
            pl.when(pl.col("read_only"))
            .then(pl.lit(READ_ONLY))
            .when(~pl.col("read_only"))
            .then(pl.lit(CHANGE))
            .alias("access"),  # a missing read_only is neither, and becomes NONE below
            pl.col("aws_region").alias("region"),
            "error_code",
            pl.col("error_code").is_not_null().alias("is_error"),
        )
        .with_columns(pl.col(*KEYS).fill_null(NONE))
    )


def activity_at_grain(records: pl.LazyFrame, grain: Grain) -> pl.LazyFrame:
    """Counts of prepared `records` per `grain.every` bucket of `event_time` (UTC)
    and per `grain.keys`: (grain, bucket_start, *KEYS, record_count, error_count).

    A bucket includes its start and excludes its end, and a bucket with no
    Record has no row. A bucket cut off by `records` holds a partial count, so
    give it whole buckets."""
    return (
        records.sort(
            *grain.keys, "event_time"
        )  # a dynamic group-by needs the time sorted within each group
        .group_by_dynamic("event_time", every=grain.every, group_by=list(grain.keys))
        .agg(
            pl.len().cast(pl.Int64).alias("record_count"),
            pl.col("is_error").sum().cast(pl.Int64).alias("error_count"),
        )
        .with_columns(
            pl.lit(grain.name).alias("grain"),
            *(pl.lit(ALL).alias(key) for key in KEYS if key not in grain.keys),
        )
        .select(
            "grain",
            pl.col("event_time").alias("bucket_start"),
            *KEYS,
            "record_count",
            "error_count",
        )
        .sort("bucket_start", *KEYS)
    )


def activity(records: pl.LazyFrame, grains: tuple[Grain, ...] = GRAINS) -> pl.LazyFrame:
    """All `grains` of prepared `records` in one table, told apart by `grain`.
    Its counts add up within a grain, never across grains."""
    return pl.concat([activity_at_grain(records, grain) for grain in grains])


def records_for_day(bronze: pl.LazyFrame, day: date) -> pl.LazyFrame:
    """The bronze Records that can hold `day`'s activity: those delivered on `day`
    and on the day after, and only the columns the activity table is built from
    (so the scan prunes the partitions and never reads `raw_json`)."""
    delivered_by_next_day = pl.col("event_date").is_between(
        day, day + timedelta(days=1)
    )
    return bronze.filter(delivered_by_next_day).select(RECORD_COLUMNS)


def activity_for_day(records: pl.LazyFrame, day: date) -> pl.LazyFrame:
    """The activity table for the Records that happened on `day` (UTC), from
    bronze `records` that include that whole day, and `bucket_date` (the day
    each bucket starts on) as the partition column.

    The day is cut on `event_time`, not Delivery Day: its last minutes can sit
    in the next Delivery Day, so `records` should hold both. Every grain's
    buckets nest inside a UTC day, so none is ever cut in two."""
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    happened_on_day = pl.col("event_time").is_between(
        start, start + timedelta(days=1), closed="left"
    )
    return activity(prepare_records(records.filter(happened_on_day))).with_columns(
        bucket_date=pl.col("bucket_start").dt.date()
    )
