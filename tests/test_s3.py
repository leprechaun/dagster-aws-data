import pytest
from botocore.exceptions import ClientError

from aws_data.lib.s3 import list_common_prefixes, object_exists
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
