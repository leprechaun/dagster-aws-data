from datetime import date

import dagster as dg
from dagster_aws.s3 import S3Resource

from aws_data.lib.cloudfront_s3 import (
    CLOUDFRONT_PARTITIONS_DEF,
    CLOUDFRONT_RAW_PREFIX,
    CLOUDFRONT_SOURCE_BUCKET,
    CLOUDFRONT_SOURCE_PREFIX,
    day_prefix,
    list_accounts,
    list_distributions,
)
from aws_data.lib.cloudtrail_s3 import PROJECT_PREFIX, STORAGE_BUCKET
from aws_data.lib.s3 import mirror_prefixes


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

    distributions_seen: set[str] = set()
    prefixes: list[str] = []

    accounts = list_accounts(source_client, CLOUDFRONT_SOURCE_BUCKET, CLOUDFRONT_SOURCE_PREFIX)

    for account_id in accounts:
        distribution_ids = list_distributions(
            source_client, CLOUDFRONT_SOURCE_BUCKET, CLOUDFRONT_SOURCE_PREFIX, account_id
        )
        distributions_seen.update(distribution_ids)
        prefixes += [
            day_prefix(CLOUDFRONT_SOURCE_PREFIX, account_id, distribution_id, partition_date)
            for distribution_id in distribution_ids
        ]

    mirrored = mirror_prefixes(
        source_client, CLOUDFRONT_SOURCE_BUCKET, prefixes, ".gz", dest_client, STORAGE_BUCKET, CLOUDFRONT_RAW_PREFIX
    )

    context.add_output_metadata(
        {
            "files_copied": mirrored.files_copied,
            "files_already_cached": mirrored.files_already_cached,
            "bytes_copied": mirrored.bytes_copied,
            "accounts": accounts,
            "distributions": sorted(distributions_seen),
        }
    )
