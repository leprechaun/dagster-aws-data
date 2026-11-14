from dataclasses import dataclass

import polars as pl

ALL = "(all)"
NONE = "(none)"

KEYS = ("caller", "event_source", "event_name", "account_id", "region", "error_code")


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
    Grain("1h", "1h", ("caller", "event_source", "account_id", "error_code")),
    Grain("1d", "1d", ("caller", "event_source", "account_id")),
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
