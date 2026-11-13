from datetime import date

import dagster as dg
from dagster_aws.s3 import S3Resource

from aws_data.lib.cloudtrail_s3 import (
    CLOUDTRAIL_PARTITIONS_DEF,
    PROJECT_PREFIX,
    RAW_PREFIX,
    SOURCE_BUCKET,
    SOURCE_PREFIX,
    STORAGE_BUCKET,
    day_prefix,
    list_accounts,
    list_organizations,
    list_regions,
)
from aws_data.lib.s3 import mirror_prefixes


@dg.asset(
    name="cloudtrail_files",
    key_prefix=[PROJECT_PREFIX, "raw"],
    partitions_def=CLOUDTRAIL_PARTITIONS_DEF,
    group_name="cloudtrail",
    kinds={"s3"},
    io_manager_key="raw_files_io_manager",
    # Shared project pool (see PROJECT_PREFIX): keeps concurrent backfills
    # from hammering AWS/MinIO with redundant listing calls for overlapping days.
    pool=PROJECT_PREFIX,
)
def cloudtrail_files(context: dg.AssetExecutionContext, aws_s3: S3Resource, minio_s3: S3Resource) -> None:
    """Mirrors one day of raw CloudTrail .json.gz files from AWS S3 to MinIO,
    byte-for-byte, preserving the AWSLogs/... key layout. Files already
    present on MinIO are skipped, so reruns only copy what's missing - this
    is what lets bronze be reprocessed without re-downloading from AWS."""
    partition_date = date.fromisoformat(context.partition_key)
    source_client = aws_s3.get_client()
    dest_client = minio_s3.get_client()

    accounts_seen: set[str] = set()
    regions_seen: set[str] = set()
    prefixes: list[str] = []

    organizations = list_organizations(source_client, SOURCE_BUCKET, SOURCE_PREFIX)

    for organization_id in organizations:
        account_ids = list_accounts(source_client, SOURCE_BUCKET, SOURCE_PREFIX, organization_id)
        accounts_seen.update(account_ids)

        for account_id in account_ids:
            regions = list_regions(source_client, SOURCE_BUCKET, SOURCE_PREFIX, organization_id, account_id)
            regions_seen.update(regions)
            prefixes += [
                day_prefix(SOURCE_PREFIX, organization_id, account_id, region, partition_date) for region in regions
            ]

    mirrored = mirror_prefixes(
        source_client, SOURCE_BUCKET, prefixes, ".json.gz", dest_client, STORAGE_BUCKET, RAW_PREFIX
    )

    context.add_output_metadata(
        {
            "files_copied": mirrored.files_copied,
            "files_already_cached": mirrored.files_already_cached,
            "bytes_copied": mirrored.bytes_copied,
            "organizations": organizations,
            "accounts": sorted(accounts_seen),
            "regions": sorted(regions_seen),
        }
    )
