"""Shared constants and S3-key-walking helpers for CloudFront log delivery,
whose objects are laid out as:

    AWSLogs/{account_id}/CloudFront/{distribution_id}/{yyyy}/{mm}/{dd}/{hh}/{distribution_id}.{yyyy-mm-dd-hh}.{hash}.gz
"""

from datetime import date

import dagster as dg

from aws_data.lib.cloudtrail_s3 import PROJECT_PREFIX
from aws_data.lib.s3 import list_common_prefixes

# Source: the CloudFront log delivery destination bucket.
CLOUDFRONT_SOURCE_BUCKET = "lmacguire-access-logs"
CLOUDFRONT_SOURCE_PREFIX = "AWSLogs"

# Destination: mirrored 1:1 (same key layout) beside the other Raw files.
CLOUDFRONT_RAW_PREFIX = f"{PROJECT_PREFIX}/raw/cloudfront"

# The first day logs were delivered to the bucket.
CLOUDFRONT_HISTORY_START_DATE = "2026-11-13"

CLOUDFRONT_PARTITIONS_DEF = dg.DailyPartitionsDefinition(start_date=CLOUDFRONT_HISTORY_START_DATE)


def is_account_id(name: str) -> bool:
    """AWS account ids are 12 digits. Anything else under AWSLogs/ is not a
    delivery folder and must not be treated as an account."""
    return len(name) == 12 and name.isdigit()


def list_accounts(client, bucket: str, root_prefix: str) -> list[str]:
    names = [p.split("/")[-2] for p in list_common_prefixes(client, bucket, f"{root_prefix}/")]
    return [name for name in names if is_account_id(name)]


def list_distributions(client, bucket: str, root_prefix: str, account_id: str) -> list[str]:
    return [
        p.split("/")[-2]
        for p in list_common_prefixes(client, bucket, f"{root_prefix}/{account_id}/CloudFront/")
    ]


def day_prefix(root_prefix: str, account_id: str, distribution_id: str, day: date) -> str:
    return (
        f"{root_prefix}/{account_id}/CloudFront/{distribution_id}/"
        f"{day.year:04d}/{day.month:02d}/{day.day:02d}/"
    )


def list_day_object_keys(
    client, bucket: str, root_prefix: str, account_id: str, distribution_id: str, day: date
) -> list[str]:
    prefix = day_prefix(root_prefix, account_id, distribution_id, day)
    paginator = client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".gz"):
                keys.append(obj["Key"])
    return keys
