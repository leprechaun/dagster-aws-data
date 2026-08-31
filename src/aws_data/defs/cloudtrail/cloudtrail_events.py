import gzip
import json
from datetime import date, datetime, timezone

import dagster as dg
import polars as pl
from dagster_aws.s3 import S3Resource

from aws_data.defs.cloudtrail.cloudtrail_raw_files import cloudtrail_files
from aws_data.lib.cloudtrail_s3 import (
    CLOUDTRAIL_PARTITIONS_DEF,
    RAW_BUCKET,
    RAW_PREFIX,
    SOURCE_PREFIX,
    list_accounts,
    list_day_object_keys,
    list_organizations,
    list_regions,
)

# Reads from the raw layer's mirror on MinIO (see cloudtrail_raw_files.py),
# not AWS directly - so changing bronze logic never re-downloads from AWS.
RAW_ROOT_PREFIX = f"{RAW_PREFIX}/{SOURCE_PREFIX}"

CLOUDTRAIL_SCHEMA = {
    "event_date": pl.Date,
    "event_id": pl.Utf8,
    "event_time": pl.Datetime("us", "UTC"),
    "event_version": pl.Utf8,
    "event_source": pl.Utf8,
    "event_name": pl.Utf8,
    "event_type": pl.Utf8,
    "event_category": pl.Utf8,
    "aws_region": pl.Utf8,
    "source_ip_address": pl.Utf8,
    "user_agent": pl.Utf8,
    "request_id": pl.Utf8,
    "recipient_account_id": pl.Utf8,
    "management_event": pl.Boolean,
    "read_only": pl.Boolean,
    "error_code": pl.Utf8,
    "error_message": pl.Utf8,
    "user_identity_type": pl.Utf8,
    "user_identity_principal_id": pl.Utf8,
    "user_identity_arn": pl.Utf8,
    "user_identity_account_id": pl.Utf8,
    "user_identity_access_key_id": pl.Utf8,
    "user_identity_user_name": pl.Utf8,
    "user_identity_invoked_by": pl.Utf8,
    "organization_id": pl.Utf8,
    "source_key": pl.Utf8,
    "ingested_at": pl.Datetime("us", "UTC"),
    # Full original record, for fields not promoted to columns above
    # (requestParameters, responseElements, resources, etc). Kept as JSON
    # text rather than parsed further, since these shapes vary per AWS
    # service/API-version and drift over 10 years of history.
    "raw_json": pl.Utf8,
}


def _parse_event_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _row_from_record(
    record: dict, event_date: date, organization_id: str, source_key: str, ingested_at: datetime
) -> dict:
    user_identity = record.get("userIdentity") or {}
    return {
        "event_date": event_date,
        "event_id": record.get("eventID"),
        "event_time": _parse_event_time(record.get("eventTime")),
        "event_version": record.get("eventVersion"),
        "event_source": record.get("eventSource"),
        "event_name": record.get("eventName"),
        "event_type": record.get("eventType"),
        "event_category": record.get("eventCategory"),
        "aws_region": record.get("awsRegion"),
        "source_ip_address": record.get("sourceIPAddress"),
        "user_agent": record.get("userAgent"),
        "request_id": record.get("requestID"),
        "recipient_account_id": record.get("recipientAccountId"),
        "management_event": record.get("managementEvent"),
        "read_only": record.get("readOnly"),
        "error_code": record.get("errorCode"),
        "error_message": record.get("errorMessage"),
        "user_identity_type": user_identity.get("type"),
        "user_identity_principal_id": user_identity.get("principalId"),
        "user_identity_arn": user_identity.get("arn"),
        "user_identity_account_id": user_identity.get("accountId"),
        "user_identity_access_key_id": user_identity.get("accessKeyId"),
        "user_identity_user_name": user_identity.get("userName"),
        "user_identity_invoked_by": user_identity.get("invokedBy"),
        "organization_id": organization_id,
        "source_key": source_key,
        "ingested_at": ingested_at,
        "raw_json": json.dumps(record),
    }


@dg.asset(
    key_prefix="bronze",
    partitions_def=CLOUDTRAIL_PARTITIONS_DEF,
    group_name="cloudtrail",
    kinds={"s3", "deltalake"},
    metadata={"partition_expr": "event_date"},
    deps=[cloudtrail_files],
    # Serializes runs of this asset so concurrent backfills can't race each
    # other committing to the same Delta table on MinIO.
    pool="cloudtrail_delta_write",
)
def cloudtrail_events(context: dg.AssetExecutionContext, minio_s3: S3Resource) -> pl.DataFrame:
    """One day of raw CloudTrail management-event records, parsed from the
    raw layer's MinIO mirror into the bronze Delta table on MinIO."""
    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    client = minio_s3.get_client()

    organizations = list_organizations(client, RAW_BUCKET, RAW_ROOT_PREFIX)

    rows = []
    files_read = 0
    files_skipped = 0
    accounts_seen: set[str] = set()
    regions_seen: set[str] = set()

    for organization_id in organizations:
        account_ids = list_accounts(client, RAW_BUCKET, RAW_ROOT_PREFIX, organization_id)
        accounts_seen.update(account_ids)

        for account_id in account_ids:
            regions = list_regions(client, RAW_BUCKET, RAW_ROOT_PREFIX, organization_id, account_id)
            regions_seen.update(regions)

            for region in regions:
                for key in list_day_object_keys(
                    client, RAW_BUCKET, RAW_ROOT_PREFIX, organization_id, account_id, region, partition_date
                ):
                    try:
                        body = client.get_object(Bucket=RAW_BUCKET, Key=key)["Body"].read()
                        records = json.loads(gzip.decompress(body)).get("Records", [])
                    except (OSError, gzip.BadGzipFile, json.JSONDecodeError) as e:
                        context.log.warning(f"Skipping unreadable CloudTrail file {key}: {e}")
                        files_skipped += 1
                        continue

                    ingested_at = datetime.now(timezone.utc)
                    rows.extend(
                        _row_from_record(r, partition_date, organization_id, key, ingested_at) for r in records
                    )
                    files_read += 1

    context.add_output_metadata(
        {
            "files_read": files_read,
            "files_skipped": files_skipped,
            "records_read": len(rows),
            "organizations": organizations,
            "accounts": sorted(accounts_seen),
            "regions": sorted(regions_seen),
        }
    )

    return pl.DataFrame(rows, schema=CLOUDTRAIL_SCHEMA)
