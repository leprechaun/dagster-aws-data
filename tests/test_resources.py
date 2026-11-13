import pytest

from aws_data.defs.resources import aws_source_s3

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
    client = aws_source_s3("src-key", "src-secret", region="ap-southeast-1").get_client()
    assert client.meta.endpoint_url == "https://s3.ap-southeast-1.amazonaws.com"


def test_source_client_ignores_ambient_credentials(minio_ambient_env):
    client = aws_source_s3("src-key", "src-secret").get_client()
    assert client._request_signer._credentials.access_key == "src-key"


def test_source_client_uses_bucket_region():
    client = aws_source_s3("k", "s").get_client()
    assert client.meta.region_name == "ap-southeast-1"
