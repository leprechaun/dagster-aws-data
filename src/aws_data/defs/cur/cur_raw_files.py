import dagster as dg
from dagster_aws.s3 import S3Resource

from aws_data.lib.cloudtrail_s3 import PROJECT_PREFIX, STORAGE_BUCKET
from aws_data.lib.cur_s3 import (
    CUR_PARTITIONS_DEF,
    CUR_RAW_PREFIX,
    CUR_SOURCE_BUCKET,
    CUR_SOURCE_EXPORT_PREFIX,
    billing_period,
    billing_period_prefix,
)
from aws_data.lib.s3 import sync_prefix


@dg.asset(
    name="cur_files",
    key_prefix=[PROJECT_PREFIX, "raw"],
    partitions_def=CUR_PARTITIONS_DEF,
    group_name="cur",
    kinds={"s3"},
    io_manager_key="raw_files_io_manager",
    # Shared project pool (see PROJECT_PREFIX): keeps concurrent backfills
    # from hammering AWS/MinIO.
    pool=PROJECT_PREFIX,
)
def cur_files(context: dg.AssetExecutionContext, aws_s3: S3Resource, minio_s3: S3Resource) -> None:
    """Mirrors one Billing Period of the Cost and Usage Report's Parquet files
    from AWS S3 to MinIO, byte-for-byte, preserving the key layout."""
    prefix = billing_period_prefix(CUR_SOURCE_EXPORT_PREFIX, billing_period(context.partition_key))

    mirrored = sync_prefix(
        aws_s3.get_client(), CUR_SOURCE_BUCKET, prefix, ".parquet", minio_s3.get_client(), STORAGE_BUCKET, CUR_RAW_PREFIX
    )

    context.add_output_metadata(
        {
            "files_copied": mirrored.files_copied,
            "files_already_cached": mirrored.files_already_cached,
            "files_deleted": mirrored.files_deleted,
            "bytes_copied": mirrored.bytes_copied,
        }
    )
