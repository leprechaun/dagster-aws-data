import dagster as dg

from aws_data.defs.cloudfront.cloudfront_raw_files import cloudfront_files
from aws_data.defs.cloudfront.cloudfront_requests import cloudfront_requests
from aws_data.defs.cloudtrail.cloudtrail_events import cloudtrail_events
from aws_data.defs.cloudtrail.cloudtrail_raw_files import cloudtrail_files
from aws_data.lib.cloudfront_s3 import CLOUDFRONT_PARTITIONS_DEF
from aws_data.lib.cloudtrail_s3 import CLOUDTRAIL_PARTITIONS_DEF
from aws_data.lib.refresh import partitions_to_refresh

# Each hour, mirror whatever has arrived for the latest two days and rebuild
# Bronze from it. Safe to repeat: Raw skips what it already has and Bronze
# replaces the day's partition. The run_key is unique per tick: a key that
# repeated across ticks would make Dagster skip every rerun after the first,
# while one per tick still stops the same tick launching a day twice. The
# sources are staggered so their runs don't start together, and they share
# the project pool in any case.
cloudtrail_refresh = dg.define_asset_job(
    "cloudtrail_refresh",
    selection=[cloudtrail_files, cloudtrail_events],
)
cloudfront_refresh = dg.define_asset_job(
    "cloudfront_refresh",
    selection=[cloudfront_files, cloudfront_requests],
)


@dg.schedule(job=cloudtrail_refresh, cron_schedule="20 * * * *", default_status=dg.DefaultScheduleStatus.RUNNING)
def cloudtrail_hourly(context: dg.ScheduleEvaluationContext):
    for partition_key in partitions_to_refresh(context.scheduled_execution_time, CLOUDTRAIL_PARTITIONS_DEF):
        yield dg.RunRequest(partition_key=partition_key, run_key=f"{partition_key}@{context.scheduled_execution_time.isoformat()}")


@dg.schedule(job=cloudfront_refresh, cron_schedule="40 * * * *", default_status=dg.DefaultScheduleStatus.RUNNING)
def cloudfront_hourly(context: dg.ScheduleEvaluationContext):
    for partition_key in partitions_to_refresh(context.scheduled_execution_time, CLOUDFRONT_PARTITIONS_DEF):
        yield dg.RunRequest(partition_key=partition_key, run_key=f"{partition_key}@{context.scheduled_execution_time.isoformat()}")
