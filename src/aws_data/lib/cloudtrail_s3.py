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
SOURCE_REGION = "us-east-1"
SOURCE_PREFIX = "AWSLogs"

# Everything this project owns on shared infrastructure lives under this name:
# the leading asset-key component, the Dagster concurrency pool, and the
# prefix inside the storage bucket (Raw files and Delta tables alike).
PROJECT_PREFIX = "aws-data"

# Destination: one MinIO bucket shared with other projects. Our part of it is
# s3://<bucket>/<PROJECT_PREFIX>, which is also the root the Delta IO manager
# writes tables under (see resources.py).
STORAGE_BUCKET = "deltalake"

# Raw files are mirrored 1:1 (same key layout) beside the tables, so
# re-processing bronze never has to re-download from AWS.
RAW_PREFIX = f"{PROJECT_PREFIX}/raw/cloudtrail"

# The first day this trail delivered logs. Earlier days have no Log Files, so
# they would only add empty partitions.
HISTORY_START_DATE = "2026-08-23"

CLOUDTRAIL_PARTITIONS_DEF = dg.DailyPartitionsDefinition(start_date=HISTORY_START_DATE)


def list_common_prefixes(client, bucket: str, prefix: str) -> list[str]:
    paginator = client.get_paginator("list_objects_v2")
    prefixes = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for common_prefix in page.get("CommonPrefixes", []):
            prefixes.append(common_prefix["Prefix"])
    return prefixes


def is_organization_id(name: str) -> bool:
    """AWS Organization ids start with "o-". The bucket also holds unrelated
    folders under AWSLogs/ (e.g. a non-organization trail's account-id
    folders), which must not be treated as organizations."""
    return name.startswith("o-")


def list_organizations(client, bucket: str, root_prefix: str) -> list[str]:
    names = [p.split("/")[-2] for p in list_common_prefixes(client, bucket, f"{root_prefix}/")]
    return [name for name in names if is_organization_id(name)]


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
