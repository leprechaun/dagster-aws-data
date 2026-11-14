import polars as pl

from aws_data.defs.resources import DeltaTables


def test_a_table_is_scanned_lazily_from_its_layer_under_the_root(tmp_path):
    pl.DataFrame({"n": [1, 2, 3]}).write_delta(str(tmp_path / "bronze" / "things"))

    scanned = DeltaTables(root_uri=str(tmp_path)).scan("bronze", "things")

    assert isinstance(scanned, pl.LazyFrame)
    assert scanned.collect()["n"].to_list() == [1, 2, 3]


def test_s3_settings_are_passed_only_when_given():
    assert DeltaTables(root_uri="s3://bucket/project").storage_options() == {}
    assert DeltaTables(
        root_uri="s3://bucket/project",
        access_key_id="key",
        secret_access_key="secret",
        endpoint="https://minio",
    ).storage_options() == {
        "aws_access_key_id": "key",
        "aws_secret_access_key": "secret",
        "aws_endpoint_url": "https://minio",
        "aws_region": "us-east-1",
    }
