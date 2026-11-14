from datetime import date

import dagster as dg
import polars as pl

from aws_data.defs.cloudtrail.cloudtrail_events import cloudtrail_events
from aws_data.defs.resources import DeltaTables
from aws_data.lib.cloudtrail_activity import activity_for_day, records_for_day
from aws_data.lib.cloudtrail_s3 import CLOUDTRAIL_PARTITIONS_DEF, PROJECT_PREFIX

# A day's last minutes of Records are delivered in the next day's Log Files, so
# a day of activity reads its own Delivery Day and the one after it. This is
# lineage only: the data is read by the asset itself, because the IO manager
# would load just the first day of a two-day input.
WITH_THE_NEXT_DELIVERY_DAY = dg.TimeWindowPartitionMapping(start_offset=0, end_offset=1)


@dg.asset(
    key_prefix=[PROJECT_PREFIX, "gold"],
    partitions_def=CLOUDTRAIL_PARTITIONS_DEF,
    group_name="cloudtrail",
    kinds={"deltalake"},
    # Partitioned by the day the buckets start on (an event day), not by
    # Delivery Day like bronze, so a rebuild replaces whole buckets.
    metadata={"partition_expr": "bucket_date"},
    deps=[dg.AssetDep(cloudtrail_events, partition_mapping=WITH_THE_NEXT_DELIVERY_DAY)],
    # Same pool as bronze: writers to MinIO must not run concurrently (see
    # cloudtrail_events).
    pool=PROJECT_PREFIX,
)
def cloudtrail_activity(
    context: dg.AssetExecutionContext, delta_tables: DeltaTables
) -> pl.LazyFrame:
    """CloudTrail Records counted per time bucket at 15-minute, hourly and daily
    grains, for one event day. Lazy from the bronze scan to the Delta write."""
    day = date.fromisoformat(context.partition_key)
    bronze = delta_tables.scan("bronze", "cloudtrail_events")
    return activity_for_day(records_for_day(bronze, day), day)
