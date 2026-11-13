"""S3 helpers shared by every log source."""

from concurrent.futures import ThreadPoolExecutor
from typing import NamedTuple

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


class MirrorResult(NamedTuple):
    files_copied: int
    files_already_cached: int
    bytes_copied: int


def list_keys(client, bucket: str, prefix: str, suffix: str = "") -> list[str]:
    paginator = client.get_paginator("list_objects_v2")
    return [
        obj["Key"]
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(suffix)
    ]


def mirror_prefixes(
    source_client,
    source_bucket: str,
    source_prefixes: list[str],
    suffix: str,
    dest_client,
    dest_bucket: str,
    dest_prefix: str,
    max_workers: int = 16,
) -> MirrorResult:
    """Copies every `suffix` file under the source prefixes to the destination,
    byte-for-byte, under `dest_prefix` and keeping the source key. What's
    already there is found with one listing per prefix and skipped; the rest is
    copied by one thread pool across all the prefixes."""
    files_already_cached = 0
    to_copy: list[str] = []
    for source_prefix in source_prefixes:
        already_there = set(list_keys(dest_client, dest_bucket, f"{dest_prefix}/{source_prefix}", suffix))
        for key in list_keys(source_client, source_bucket, source_prefix, suffix):
            if f"{dest_prefix}/{key}" in already_there:
                files_already_cached += 1
            else:
                to_copy.append(key)

    def copy(key: str) -> int:
        body = source_client.get_object(Bucket=source_bucket, Key=key)["Body"].read()
        dest_client.put_object(Bucket=dest_bucket, Key=f"{dest_prefix}/{key}", Body=body)
        return len(body)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        bytes_copied = sum(pool.map(copy, to_copy))
    return MirrorResult(len(to_copy), files_already_cached, bytes_copied)
