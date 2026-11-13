import io

from botocore.exceptions import ClientError


class FakePaginator:
    def __init__(self, keys: list[str], page_size: int):
        self._keys = keys
        self._page_size = page_size

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
            items = [("Contents", {"Key": k}) for k in matching]

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
        return FakePaginator(self._all_keys(), self._page_size)

    def get_object(self, Bucket: str, Key: str) -> dict:
        return {"Body": io.BytesIO(self._objects[Key])}

    def put_object(self, Bucket: str, Key: str, Body: bytes) -> None:
        self._objects[Key] = Body

    def head_object(self, Bucket: str, Key: str) -> dict:
        if Key not in self._objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {}
