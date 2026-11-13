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
    """In-memory stand-in for the slice of the boto3 S3 client the lib helpers use."""

    def __init__(self, keys: list[str], page_size: int = 2):
        self._keys = keys
        self._page_size = page_size

    def get_paginator(self, operation: str) -> FakePaginator:
        assert operation == "list_objects_v2"
        return FakePaginator(self._keys, self._page_size)
