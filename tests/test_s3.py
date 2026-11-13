import threading

import pytest
from botocore.exceptions import ClientError

from aws_data.lib.s3 import (
    MirrorResult,
    list_common_prefixes,
    mirror_prefixes,
    object_exists,
)
from tests.conftest import FakeS3Client


class FakeHeadClient:
    def __init__(self, error_code: str | None = None):
        self._error_code = error_code

    def head_object(self, Bucket: str, Key: str):
        if self._error_code is not None:
            raise ClientError({"Error": {"Code": self._error_code}}, "HeadObject")
        return {}


def test_object_exists_is_true_when_the_object_is_there():
    assert object_exists(FakeHeadClient(), "bucket", "some/key") is True


def test_object_exists_is_false_when_s3_says_404():
    assert object_exists(FakeHeadClient("404"), "bucket", "some/key") is False


def test_object_exists_is_false_when_s3_says_no_such_key():
    assert object_exists(FakeHeadClient("NoSuchKey"), "bucket", "some/key") is False


def test_object_exists_does_not_hide_other_errors():
    with pytest.raises(ClientError):
        object_exists(FakeHeadClient("403"), "bucket", "some/key")


def test_list_common_prefixes_returns_the_folders_one_level_down_across_pages():
    keys = ["logs/a/one.gz", "logs/a/two.gz", "logs/b/three.gz", "logs/loose.txt"]
    client = FakeS3Client(keys, page_size=1)

    assert list_common_prefixes(client, "bucket", "logs/") == ["logs/a/", "logs/b/"]


def test_mirror_prefixes_copies_the_matching_files_under_every_prefix_and_nothing_else():
    source = FakeS3Client(
        objects={
            "logs/a/1.gz": b"one",
            "logs/a/2.gz": b"twotwo",
            "logs/b/3.gz": b"three!",
            "logs/a/readme.txt": b"wrong suffix",
            "logs/c/4.gz": b"outside the prefixes",
        }
    )
    dest = FakeS3Client()

    result = mirror_prefixes(source, "src", ["logs/a/", "logs/b/"], ".gz", dest, "dst", "mirror")

    assert dest.objects == {"mirror/logs/a/1.gz": b"one", "mirror/logs/a/2.gz": b"twotwo", "mirror/logs/b/3.gz": b"three!"}
    assert result == MirrorResult(files_copied=3, files_already_cached=0, bytes_copied=15)


def test_mirror_prefixes_skips_files_already_at_the_destination_and_counts_them():
    source = FakeS3Client(objects={"logs/a/1.gz": b"one", "logs/a/2.gz": b"twotwo"})
    dest = FakeS3Client(objects={"mirror/logs/a/1.gz": b"already here"})

    result = mirror_prefixes(source, "src", ["logs/a/"], ".gz", dest, "dst", "mirror")

    assert dest.objects == {"mirror/logs/a/1.gz": b"already here", "mirror/logs/a/2.gz": b"twotwo"}
    assert result == MirrorResult(files_copied=1, files_already_cached=1, bytes_copied=6)


class NoPerObjectChecksClient(FakeS3Client):
    """Any per-file existence request is a failure: checking a day's ~1,800 files
    one by one is what makes mirroring slow."""

    def head_object(self, Bucket: str, Key: str):
        raise AssertionError("existence must be decided from a listing, not one request per file")


def test_mirror_prefixes_decides_what_exists_from_a_listing_not_a_request_per_file():
    source = FakeS3Client(objects={"logs/a/1.gz": b"one", "logs/a/2.gz": b"twotwo", "logs/a/3.gz": b"3"})
    dest = NoPerObjectChecksClient(objects={"mirror/logs/a/1.gz": b"already here"}, page_size=1)

    result = mirror_prefixes(source, "src", ["logs/a/"], ".gz", dest, "dst", "mirror")

    assert result == MirrorResult(files_copied=2, files_already_cached=1, bytes_copied=7)


class RendezvousClient(FakeS3Client):
    """Each get blocks until `parties` gets are in flight together, so it only
    completes if the copies really run concurrently."""

    def __init__(self, parties: int, **kwargs):
        super().__init__(**kwargs)
        self._barrier = threading.Barrier(parties, timeout=3)

    def get_object(self, Bucket: str, Key: str) -> dict:
        self._barrier.wait()
        return super().get_object(Bucket, Key)


def test_mirror_prefixes_copies_files_concurrently_across_prefixes():
    source = RendezvousClient(
        parties=4,
        objects={"logs/a/1.gz": b"1", "logs/a/2.gz": b"2", "logs/b/3.gz": b"3", "logs/b/4.gz": b"4"},
    )
    dest = FakeS3Client()

    result = mirror_prefixes(source, "src", ["logs/a/", "logs/b/"], ".gz", dest, "dst", "mirror")

    assert result.files_copied == 4


class FlakySourceClient(FakeS3Client):
    def get_object(self, Bucket: str, Key: str) -> dict:
        if Key == "logs/a/2.gz":
            raise ClientError({"Error": {"Code": "500"}}, "GetObject")
        return super().get_object(Bucket, Key)


def test_mirror_prefixes_raises_when_a_copy_fails_instead_of_reporting_success():
    source = FlakySourceClient(objects={"logs/a/1.gz": b"one", "logs/a/2.gz": b"two", "logs/a/3.gz": b"three"})

    with pytest.raises(ClientError):
        mirror_prefixes(source, "src", ["logs/a/"], ".gz", FakeS3Client(), "dst", "mirror")


def test_mirror_prefixes_with_nothing_to_do_returns_zeros():
    assert mirror_prefixes(FakeS3Client(), "src", ["logs/a/"], ".gz", FakeS3Client(), "dst", "mirror") == MirrorResult(0, 0, 0)
    assert mirror_prefixes(FakeS3Client(), "src", [], ".gz", FakeS3Client(), "dst", "mirror") == MirrorResult(0, 0, 0)
