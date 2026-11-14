import os

import pytest

from aws_data.defs.resources import aws_source_s3, resources

MINIO_ENV = {
    "AWS_ACCESS_KEY_ID": "minio-key",
    "AWS_SECRET_ACCESS_KEY": "minio-secret",
    "AWS_ENDPOINT_URL_S3": "http://minio.example:9000",
}


@pytest.fixture
def minio_ambient_env(monkeypatch):
    for name, value in MINIO_ENV.items():
        monkeypatch.setenv(name, value)


def test_source_client_ignores_ambient_minio_endpoint(minio_ambient_env):
    client = aws_source_s3("src-key", "src-secret", region="us-east-1").get_client()
    assert client.meta.endpoint_url == "https://s3.us-east-1.amazonaws.com"


def test_source_client_ignores_ambient_credentials(minio_ambient_env):
    client = aws_source_s3("src-key", "src-secret").get_client()
    assert client._request_signer._credentials.access_key == "src-key"


def test_source_client_uses_bucket_region():
    client = aws_source_s3("k", "s").get_client()
    assert client.meta.region_name == "us-east-1"



UNSAFE_RENAME = "AWS_S3_ALLOW_UNSAFE_RENAME"


def test_delta_commits_to_minio_are_allowed_unsafe_rename_by_default(monkeypatch):
    monkeypatch.setenv(UNSAFE_RENAME, "unset")  # so the test's own changes are undone afterwards
    monkeypatch.delenv(UNSAFE_RENAME)

    resources()

    assert os.environ[UNSAFE_RENAME] == "true"


def test_an_explicit_unsafe_rename_setting_is_not_overridden(monkeypatch):
    monkeypatch.setenv(UNSAFE_RENAME, "false")

    resources()

    assert os.environ[UNSAFE_RENAME] == "false"
