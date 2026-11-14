import dagster as dg
import polars as pl

from aws_data.defs.cloudtrail.cloudtrail_events import cloudtrail_events
from aws_data.defs.resources import column
from aws_data.lib.cloudtrail_activity import RECORD_COLUMNS, activity_from_bronze
from aws_data.lib.cloudtrail_s3 import PROJECT_PREFIX

ACTIVITY_COLUMNS = dg.TableSchema(
    columns=[
        column(
            "grain",
            "Bucket width: `15m`, `1h` or `1d`. Rows of different grains describe the same Records, so never sum across grains.",
        ),
        column(
            "bucket_start",
            "Start of the UTC time bucket, inclusive (end exclusive). A bucket with no Records has no row.",
        ),
        column(
            "caller",
            "The calling identity's ARN (assumed roles by session). `(all)` where the grain leaves it out, `(none)` where the Record had none.",
        ),
        column(
            "event_source",
            "The AWS service called. `(all)` where the grain leaves it out.",
        ),
        column(
            "event_name",
            "The API action called. Only kept at the 15m grain; `(all)` at 1h and 1d.",
        ),
        column(
            "access", "`read-only` or `change`; `(none)` when CloudTrail didn't say."
        ),
        column(
            "account_id", "The Account the activity happened in (recipient account)."
        ),
        column(
            "region",
            "The region called. Only kept at the 15m grain; `(all)` at 1h and 1d.",
        ),
        column(
            "error_code",
            "The AWS error code. `(none)` means no error; `(all)` at the 1d grain, which drops it.",
        ),
        column(
            "record_count",
            "Records in the bucket for this key. Each Record counted once, even if its Log File was delivered twice.",
        ),
        column(
            "error_count",
            "How many of record_count failed, i.e. carry an error code. At 1d, where error_code is dropped, this is the only error signal.",
        ),
    ]
)


@dg.asset(
    key_prefix=[PROJECT_PREFIX, "gold"],
    group_name="cloudtrail",
    kinds={"deltalake"},
    # Bronze is partitioned by Delivery Day and this is not: it is small (about
    # a quarter of a million rows), so it is rebuilt in full each run from every
    # partition of Bronze, and is one table with no partitions of its own.
    # Only the columns it aggregates are read, never `raw_json`.
    ins={
        "bronze": dg.AssetIn(
            key=cloudtrail_events.key, metadata={"columns": list(RECORD_COLUMNS)}
        )
    },
    metadata={"dagster/column_schema": ACTIVITY_COLUMNS},
    description=(
        "Gold: CloudTrail Records counted per time bucket, caller, API and account at 15-minute, "
        "hourly and daily grains, told apart by `grain`. Rebuilt in full from Bronze on each run."
    ),
    # Same pool as bronze: writers to MinIO must not run concurrently (see
    # cloudtrail_events).
    pool=PROJECT_PREFIX,
)
def cloudtrail_activity(bronze: pl.LazyFrame) -> pl.LazyFrame:
    """CloudTrail Records counted per time bucket at 15-minute, hourly and daily
    grains, rebuilt from all of Bronze. Lazy from the Bronze scan to the Delta write."""
    return activity_from_bronze(bronze)
