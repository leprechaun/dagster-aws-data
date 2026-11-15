import dagster as dg
import polars as pl
from dagster_aws.s3 import S3Resource

from aws_data.defs.claude.claude_raw_files import SESSION_STATUS_FILES
from aws_data.lib.claude_bronze import bronze_schema, parse_raw_file, raw_prefix
from aws_data.lib.cloudtrail_s3 import PROJECT_PREFIX, STORAGE_BUCKET
from aws_data.lib.s3 import list_keys

SESSION_STATUS_BRONZE_DESCRIPTION = (
    "Bronze: one row per Claude Session Status, rebuilt in full from the Raw mirror "
    "under claude/raw/session-status/. Envelope and detail fields as columns; the "
    "statusline payload as JSON in `payload`. Each figure is a Claude Session's "
    "running totals, so the rows must not be summed."
)


@dg.asset(
    name="session_status",
    key_prefix=["claude", "bronze"],
    group_name="claude",
    kinds={"s3", "deltalake"},
    description=SESSION_STATUS_BRONZE_DESCRIPTION,
    deps=[SESSION_STATUS_FILES.key],
    io_manager_key="claude_bronze_io_manager",
    pool=PROJECT_PREFIX,
)
def session_status_bronze(minio_s3: S3Resource) -> pl.DataFrame:
    client = minio_s3.get_client()
    frames = [
        parse_raw_file(
            client.get_object(Bucket=STORAGE_BUCKET, Key=key)["Body"].read(),
            key,
        )
        for key in list_keys(client, STORAGE_BUCKET, raw_prefix())
    ]
    if not frames:
        return pl.DataFrame(schema=bronze_schema())
    return pl.concat(frames)


SESSION_STATUS_BRONZE = session_status_bronze
