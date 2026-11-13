"""S3 helpers shared by every log source."""

from botocore.exceptions import ClientError


def list_common_prefixes(client, bucket: str, prefix: str) -> list[str]:
    paginator = client.get_paginator("list_objects_v2")
    prefixes = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for common_prefix in page.get("CommonPrefixes", []):
            prefixes.append(common_prefix["Prefix"])
    return prefixes


def object_exists(client, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):
            return False
        raise
