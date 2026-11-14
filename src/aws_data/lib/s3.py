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


class ObjectInfo(NamedTuple):
    size: int
    etag: str


def list_object_infos(client, bucket: str, prefix: str, suffix: str = "") -> dict[str, ObjectInfo]:
    paginator = client.get_paginator("list_objects_v2")
    return {
        obj["Key"]: ObjectInfo(obj["Size"], obj["ETag"])
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(suffix)
    }


def keys_to_copy(source: dict[str, ObjectInfo], dest: dict[str, ObjectInfo]) -> list[str]:
    """The source keys that are missing from the destination or whose size or
    ETag differ, i.e. were restated in place. Both are keyed the same way."""
    return sorted(key for key, info in source.items() if dest.get(key) != info)


def stale_keys(source: dict[str, ObjectInfo], dest: dict[str, ObjectInfo]) -> list[str]:
    """The destination keys the source no longer has."""
    return sorted(key for key in dest if key not in source)


class SyncResult(NamedTuple):
    files_copied: int
    files_already_cached: int
    files_deleted: int
    bytes_copied: int


def sync_prefix(
    source_client,
    source_bucket: str,
    source_prefix: str,
    suffix: str,
    dest_client,
    dest_bucket: str,
    dest_prefix: str,
    max_workers: int = 16,
) -> SyncResult:
    """Makes the destination hold exactly what the source prefix holds now.
    Unlike `mirror_prefixes`, a key that is already there is only skipped when
    its size and ETag still match the source's, and keys the source no longer
    has are deleted. Only the destination under `source_prefix` is touched."""
    source = list_object_infos(source_client, source_bucket, source_prefix, suffix)
    dest_root = f"{dest_prefix}/"
    dest = {
        key.removeprefix(dest_root): info
        for key, info in list_object_infos(dest_client, dest_bucket, f"{dest_root}{source_prefix}", suffix).items()
    }
    to_copy = keys_to_copy(source, dest)
    to_delete = stale_keys(source, dest)

    def copy(key: str) -> int:
        body = source_client.get_object(Bucket=source_bucket, Key=key)["Body"].read()
        dest_client.put_object(Bucket=dest_bucket, Key=f"{dest_root}{key}", Body=body)
        return len(body)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        bytes_copied = sum(pool.map(copy, to_copy))
    for key in to_delete:
        dest_client.delete_object(Bucket=dest_bucket, Key=f"{dest_root}{key}")
    return SyncResult(len(to_copy), len(source) - len(to_copy), len(to_delete), bytes_copied)
