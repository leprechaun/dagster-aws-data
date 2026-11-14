import dagster as dg
import polars as pl

from aws_data.defs.cloudtrail.cloudtrail_events import cloudtrail_events
from aws_data.lib.cloudtrail_activity import RECORD_COLUMNS, activity_from_bronze
from aws_data.lib.cloudtrail_s3 import PROJECT_PREFIX


@dg.asset(
    key_prefix=[PROJECT_PREFIX, "gold"],
    group_name="cloudtrail",
    kinds={"deltalake"},
    # Bronze is partitioned by Delivery Day and this is not: it is small (about
    # a quarter of a million rows), so it is rebuilt in full each run from every
    # partition of Bronze, and is one table with no partitions of its own.
    # Only the columns it aggregates are read, never `raw_json`.
    ins={"bronze": dg.AssetIn(key=cloudtrail_events.key, metadata={"columns": list(RECORD_COLUMNS)})},
    # Same pool as bronze: writers to MinIO must not run concurrently (see
    # cloudtrail_events).
    pool=PROJECT_PREFIX,
)
def cloudtrail_activity(bronze: pl.LazyFrame) -> pl.LazyFrame:
    """CloudTrail Records counted per time bucket at 15-minute, hourly and daily
    grains, rebuilt from all of Bronze. Lazy from the Bronze scan to the Delta write."""
    return activity_from_bronze(bronze)
