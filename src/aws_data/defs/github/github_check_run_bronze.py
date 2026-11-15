import dagster as dg
import polars as pl
from dagster_aws.s3 import S3Resource

from aws_data.lib.cloudtrail_s3 import PROJECT_PREFIX, STORAGE_BUCKET
from aws_data.lib.github_bronze import (
    CHECK_RUN_RAW_PREFIX,
    CHECK_RUN_SCHEMA,
    parse_check_run_file,
)
from aws_data.lib.s3 import list_keys


@dg.asset(
    name="check_run",
    key_prefix=["github", "bronze"],
    group_name="github",
    kinds={"s3", "deltalake"},
    # Not partitioned: all of Raw is under 200 MB, so the table is rebuilt in
    # full from every Raw file each run.
    deps=[dg.AssetKey(["github", "raw", "check_run_files"])],
    io_manager_key="github_bronze_io_manager",
    pool=PROJECT_PREFIX,
)
def check_run(minio_s3: S3Resource) -> pl.DataFrame:
    """Every check_run delivery in Raw, one row per envelope, parsed from the
    Raw mirror on MinIO."""
    client = minio_s3.get_client()

    frames = [
        parse_check_run_file(
            client.get_object(Bucket=STORAGE_BUCKET, Key=key)["Body"].read(), key
        )
        for key in list_keys(client, STORAGE_BUCKET, CHECK_RUN_RAW_PREFIX)
    ]
    if not frames:
        return pl.DataFrame(schema=CHECK_RUN_SCHEMA)
    return pl.concat(frames)
