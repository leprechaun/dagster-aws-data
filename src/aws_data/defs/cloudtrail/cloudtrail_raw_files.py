from datetime import date

import dagster as dg
from botocore.exceptions import ClientError
from dagster_aws.s3 import S3Resource

from aws_data.lib.cloudtrail_s3 import (
    CLOUDTRAIL_PARTITIONS_DEF,
    RAW_BUCKET,
    RAW_PREFIX,
    SOURCE_BUCKET,
    SOURCE_PREFIX,
    list_accounts,
    list_day_object_keys,
    list_organizations,
    list_regions,
)


def _object_exists(client, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):
            return False
        raise


@dg.asset(
    name="cloudtrail_files",
    key_prefix="raw",
    partitions_def=CLOUDTRAIL_PARTITIONS_DEF,
    group_name="cloudtrail",
    kinds={"s3"},
    io_manager_key="raw_files_io_manager",
    # Serializes runs so concurrent backfills don't hammer AWS/MinIO with
    # redundant listing calls for overlapping days.
    pool="cloudtrail_raw_copy",
)
def cloudtrail_files(context: dg.AssetExecutionContext, aws_s3: S3Resource, minio_s3: S3Resource) -> None:
    """Mirrors one day of raw CloudTrail .json.gz files from AWS S3 to MinIO,
    byte-for-byte, preserving the AWSLogs/... key layout. Files already
    present on MinIO are skipped, so reruns only copy what's missing - this
    is what lets bronze be reprocessed without re-downloading from AWS."""
    partition_date = date.fromisoformat(context.partition_key)
    source_client = aws_s3.get_client()
    dest_client = minio_s3.get_client()

    files_copied = 0
    files_already_cached = 0
    bytes_copied = 0
    accounts_seen: set[str] = set()
    regions_seen: set[str] = set()

    organizations = list_organizations(source_client, SOURCE_BUCKET, SOURCE_PREFIX)

    for organization_id in organizations:
        account_ids = list_accounts(source_client, SOURCE_BUCKET, SOURCE_PREFIX, organization_id)
        accounts_seen.update(account_ids)

        for account_id in account_ids:
            regions = list_regions(source_client, SOURCE_BUCKET, SOURCE_PREFIX, organization_id, account_id)
            regions_seen.update(regions)

            for region in regions:
                for key in list_day_object_keys(
                    source_client,
                    SOURCE_BUCKET,
                    SOURCE_PREFIX,
                    organization_id,
                    account_id,
                    region,
                    partition_date,
                ):
                    dest_key = f"{RAW_PREFIX}/{key}"
                    if _object_exists(dest_client, RAW_BUCKET, dest_key):
                        files_already_cached += 1
                        continue

                    body = source_client.get_object(Bucket=SOURCE_BUCKET, Key=key)["Body"].read()
                    dest_client.put_object(Bucket=RAW_BUCKET, Key=dest_key, Body=body)
                    files_copied += 1
                    bytes_copied += len(body)

    context.add_output_metadata(
        {
            "files_copied": files_copied,
            "files_already_cached": files_already_cached,
            "bytes_copied": bytes_copied,
            "organizations": organizations,
            "accounts": sorted(accounts_seen),
            "regions": sorted(regions_seen),
        }
    )
