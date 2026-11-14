import dagster as dg
import polars as pl

from aws_data.defs.cloudtrail.cloudtrail_events import cloudtrail_events
from aws_data.defs.resources import DeltaTables
from aws_data.lib.cloudtrail_activity import activity_from_bronze
from aws_data.lib.cloudtrail_s3 import PROJECT_PREFIX


@dg.asset(
    key_prefix=[PROJECT_PREFIX, "gold"],
    group_name="cloudtrail",
    kinds={"deltalake"},
    # Bronze is partitioned by Delivery Day and this is not: it is small (about
    # a quarter of a million rows), so it is rebuilt in full each run and is one
    # table with no partitions to keep in step with Bronze's.
    deps=[cloudtrail_events],
    # Read through the resource, not as an input: the IO manager would load
    # only the first partition of Bronze.
    # Same pool as bronze: writers to MinIO must not run concurrently (see
    # cloudtrail_events).
    pool=PROJECT_PREFIX,
)
def cloudtrail_activity(delta_tables: DeltaTables) -> pl.LazyFrame:
    """CloudTrail Records counted per time bucket at 15-minute, hourly and daily
    grains, rebuilt from all of Bronze. Lazy from the Bronze scan to the Delta write."""
    return activity_from_bronze(delta_tables.scan("bronze", "cloudtrail_events"))
