from datetime import date

import dagster as dg
import polars as pl
from dagster_aws.s3 import S3Resource

from aws_data.lib.cloudtrail_s3 import PROJECT_PREFIX, STORAGE_BUCKET
from aws_data.lib.github_bronze import (
    CHECK_RUN_SCHEMA,
    parse_check_run_file,
    raw_check_run_prefix,
    rows_for_utc_day,
)
from aws_data.lib.github_s3 import GITHUB_PARTITIONS_DEF
from aws_data.lib.s3 import list_keys


@dg.asset(
    name="check_run",
    key_prefix=["github", "bronze"],
    partitions_def=GITHUB_PARTITIONS_DEF,
    group_name="github",
    kinds={"s3", "deltalake"},
    metadata={"partition_expr": "day"},
    deps=[dg.AssetKey(["github", "raw", "check_run_files"])],
    pool=PROJECT_PREFIX,
)
def check_run(context: dg.AssetExecutionContext, minio_s3: S3Resource) -> pl.DataFrame:
    """One UTC day of check_run deliveries, one row per envelope, parsed from the
    Raw mirror on MinIO."""
    day = date.fromisoformat(context.partition_key)
    client = minio_s3.get_client()

    frames = [
        parse_check_run_file(
            client.get_object(Bucket=STORAGE_BUCKET, Key=key)["Body"].read(), key
        )
        for key in list_keys(client, STORAGE_BUCKET, raw_check_run_prefix(day))
    ]
    rows = pl.concat(frames) if frames else pl.DataFrame(schema=CHECK_RUN_SCHEMA)
    return rows_for_utc_day(rows, day).with_columns(
        pl.col("time").dt.date().alias("day")
    )
