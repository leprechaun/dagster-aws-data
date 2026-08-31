"""Shared constants and S3-key-walking helpers for an AWS Organizations
CloudTrail trail, whose objects are laid out as:

    AWSLogs/{organization_id}/{account_id}/CloudTrail/{region}/{yyyy}/{mm}/{dd}/*.json.gz

Used by both the `raw` asset (mirrors these files from AWS S3 to MinIO
unchanged) and the `bronze` asset (reads the MinIO mirror and parses them).
"""

from datetime import date

import dagster as dg

# Source: the CloudTrail organization trail's S3 destination bucket.
SOURCE_BUCKET = "lmacguire-aws-logs"
SOURCE_PREFIX = "AWSLogs"

# Destination: raw files are mirrored 1:1 (same key layout) under this
# prefix on MinIO, so re-processing bronze never has to re-download from AWS.
RAW_BUCKET = "lakehouse"
RAW_PREFIX = "raw/cloudtrail"

# Adjust to the actual date this trail's logs begin if known; days with no
# files simply materialize zero rows/copies, so an early guess is harmless.
HISTORY_START_DATE = "2016-01-01"

CLOUDTRAIL_PARTITIONS_DEF = dg.DailyPartitionsDefinition(start_date=HISTORY_START_DATE)


def list_common_prefixes(client, bucket: str, prefix: str) -> list[str]:
    paginator = client.get_paginator("list_objects_v2")
    prefixes = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for common_prefix in page.get("CommonPrefixes", []):
            prefixes.append(common_prefix["Prefix"])
    return prefixes


def list_organizations(client, bucket: str, root_prefix: str) -> list[str]:
    return [p.split("/")[-2] for p in list_common_prefixes(client, bucket, f"{root_prefix}/")]


def list_accounts(client, bucket: str, root_prefix: str, organization_id: str) -> list[str]:
    return [
        p.split("/")[-2] for p in list_common_prefixes(client, bucket, f"{root_prefix}/{organization_id}/")
    ]


def list_regions(client, bucket: str, root_prefix: str, organization_id: str, account_id: str) -> list[str]:
    return [
        p.split("/")[-2]
        for p in list_common_prefixes(
            client, bucket, f"{root_prefix}/{organization_id}/{account_id}/CloudTrail/"
        )
    ]


def day_prefix(root_prefix: str, organization_id: str, account_id: str, region: str, day: date) -> str:
    return (
        f"{root_prefix}/{organization_id}/{account_id}/CloudTrail/{region}/"
        f"{day.year:04d}/{day.month:02d}/{day.day:02d}/"
    )


def list_day_object_keys(
    client,
    bucket: str,
    root_prefix: str,
    organization_id: str,
    account_id: str,
    region: str,
    day: date,
) -> list[str]:
    prefix = day_prefix(root_prefix, organization_id, account_id, region, day)
    paginator = client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json.gz"):
                keys.append(obj["Key"])
    return keys
