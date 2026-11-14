import hashlib
import io
from datetime import datetime, timezone

from botocore.exceptions import ClientError


class FakePaginator:
    def __init__(self, keys: list[str], page_size: int, objects: dict[str, bytes] | None = None):
        self._keys = keys
        self._page_size = page_size
        self._objects = objects or {}

    def _listing(self, key: str) -> dict:
        """What S3 lists for an object: its key, plus size and the quoted MD5
        that is a single-put object's ETag (when the body is known)."""
        if key not in self._objects:
            return {"Key": key}
        body = self._objects[key]
        return {"Key": key, "Size": len(body), "ETag": f'"{hashlib.md5(body).hexdigest()}"'}

    def paginate(self, Bucket: str, Prefix: str = "", Delimiter: str | None = None):
        matching = sorted(k for k in self._keys if k.startswith(Prefix))
        if Delimiter:
            common_prefixes: list[str] = []
            for key in matching:
                rest = key[len(Prefix) :]
                if Delimiter in rest:
                    common = Prefix + rest.split(Delimiter)[0] + Delimiter
                    if common not in common_prefixes:
                        common_prefixes.append(common)
            items = [("CommonPrefixes", {"Prefix": p}) for p in common_prefixes]
        else:
            items = [("Contents", self._listing(k)) for k in matching]

        for start in range(0, len(items), self._page_size) or [0]:
            page: dict = {}
            for field, entry in items[start : start + self._page_size]:
                page.setdefault(field, []).append(entry)
            yield page


class FakeS3Client:
    """In-memory stand-in for the slice of the boto3 S3 client the code under
    test uses: listing, get, put and head. One instance stands for one bucket."""

    def __init__(self, keys: list[str] | None = None, page_size: int = 2, objects: dict[str, bytes] | None = None):
        self._keys = list(keys or [])
        self._objects = dict(objects or {})
        self._page_size = page_size

    @property
    def objects(self) -> dict[str, bytes]:
        return dict(self._objects)

    def _all_keys(self) -> list[str]:
        return self._keys + [key for key in self._objects if key not in self._keys]

    def get_paginator(self, operation: str) -> FakePaginator:
        assert operation == "list_objects_v2"
        return FakePaginator(self._all_keys(), self._page_size, self._objects)

    def get_object(self, Bucket: str, Key: str) -> dict:
        return {"Body": io.BytesIO(self._objects[Key])}

    def put_object(self, Bucket: str, Key: str, Body: bytes) -> None:
        self._objects[Key] = Body

    def delete_object(self, Bucket: str, Key: str) -> None:
        self._objects.pop(Key, None)

    def head_object(self, Bucket: str, Key: str) -> dict:
        if Key not in self._objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {}


# A day's partition only exists once that day has ended, so run "after" the
# days under test instead of depending on today's date.
AFTER_THE_DAYS_UNDER_TEST = datetime(2026, 12, 1, tzinfo=timezone.utc)


def log_file(account: str, distribution: str, day: str, hour: str, suffix: str = "aaaa1111") -> str:
    year, month, dom = day.split("-")
    return (
        f"AWSLogs/{account}/CloudFront/{distribution}/{year}/{month}/{dom}/{hour}/"
        f"{distribution}.{day}-{hour}.{suffix}.gz"
    )


class FakeS3Resource:
    def __init__(self, client: FakeS3Client):
        self._client = client

    def get_client(self) -> FakeS3Client:
        return self._client
