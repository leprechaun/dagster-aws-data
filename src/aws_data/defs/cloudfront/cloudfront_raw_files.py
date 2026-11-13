from datetime import date

import dagster as dg
from dagster_aws.s3 import S3Resource

from aws_data.lib.cloudfront_s3 import (
    CLOUDFRONT_PARTITIONS_DEF,
    CLOUDFRONT_RAW_PREFIX,
    CLOUDFRONT_SOURCE_BUCKET,
    CLOUDFRONT_SOURCE_PREFIX,
    list_accounts,
    list_day_object_keys,
    list_distributions,
)
from aws_data.lib.cloudtrail_s3 import PROJECT_PREFIX, STORAGE_BUCKET
from aws_data.lib.s3 import object_exists


@dg.asset(
    name="cloudfront_files",
    key_prefix=[PROJECT_PREFIX, "raw"],
    partitions_def=CLOUDFRONT_PARTITIONS_DEF,
    group_name="cloudfront",
    kinds={"s3"},
    io_manager_key="raw_files_io_manager",
    # Shared project pool (see PROJECT_PREFIX): keeps concurrent backfills
    # from hammering AWS/MinIO with redundant listing calls for overlapping days.
    pool=PROJECT_PREFIX,
)
def cloudfront_files(context: dg.AssetExecutionContext, aws_s3: S3Resource, minio_s3: S3Resource) -> None:
    """Mirrors one day of CloudFront log delivery .gz files from AWS S3 to
    MinIO, byte-for-byte, preserving the AWSLogs/... key layout."""
    partition_date = date.fromisoformat(context.partition_key)
    source_client = aws_s3.get_client()
    dest_client = minio_s3.get_client()

    files_copied = 0
    files_already_cached = 0
    bytes_copied = 0
    distributions_seen: set[str] = set()

    accounts = list_accounts(source_client, CLOUDFRONT_SOURCE_BUCKET, CLOUDFRONT_SOURCE_PREFIX)

    for account_id in accounts:
        distribution_ids = list_distributions(
            source_client, CLOUDFRONT_SOURCE_BUCKET, CLOUDFRONT_SOURCE_PREFIX, account_id
        )
        distributions_seen.update(distribution_ids)

        for distribution_id in distribution_ids:
            for key in list_day_object_keys(
                source_client,
                CLOUDFRONT_SOURCE_BUCKET,
                CLOUDFRONT_SOURCE_PREFIX,
                account_id,
                distribution_id,
                partition_date,
            ):
                dest_key = f"{CLOUDFRONT_RAW_PREFIX}/{key}"
                if object_exists(dest_client, STORAGE_BUCKET, dest_key):
                    files_already_cached += 1
                    continue

                body = source_client.get_object(Bucket=CLOUDFRONT_SOURCE_BUCKET, Key=key)["Body"].read()
                dest_client.put_object(Bucket=STORAGE_BUCKET, Key=dest_key, Body=body)
                files_copied += 1
                bytes_copied += len(body)

    context.add_output_metadata(
        {
            "files_copied": files_copied,
            "files_already_cached": files_already_cached,
            "bytes_copied": bytes_copied,
            "accounts": accounts,
            "distributions": sorted(distributions_seen),
        }
    )
