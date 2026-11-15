import dagster as dg

from aws_data.defs.github.github_raw_files import (
    GITHUB_RAW_ASSETS,
    github_raw_files_asset,
)
from tests.conftest import FakeS3Client, FakeS3Resource

DELIVERY = "data/github/check_run/2026/11/13/webhooks-2-2026-11-13-10-32-19-6738f2f0"

check_run_files = github_raw_files_asset("check_run")


def mirror(source: FakeS3Client, raw: FakeS3Client) -> dg.ExecuteInProcessResult:
    return dg.materialize(
        [check_run_files],
        resources={
            "github_source_s3": FakeS3Resource(source),
            "minio_s3": FakeS3Resource(raw),
            "raw_files_io_manager": dg.mem_io_manager,
        },
    )


def metadata_of(result: dg.ExecuteInProcessResult) -> dict:
    return result.asset_materializations_for_node("github__raw__check_run_files")[
        0
    ].metadata


def test_a_check_run_delivery_is_mirrored_into_github_raw_byte_for_byte():
    source = FakeS3Client(objects={DELIVERY: b'{"detail-type": "check_run"}'})
    raw = FakeS3Client()

    result = mirror(source, raw)

    assert result.success
    assert raw.objects == {
        "github/raw/check_run/2026/11/13/webhooks-2-2026-11-13-10-32-19-6738f2f0": b'{"detail-type": "check_run"}'
    }


def test_every_day_of_one_event_type_is_mirrored_and_other_types_are_not():
    source = FakeS3Client(
        objects={
            "data/github/check_run/2026/11/13/a": b"a",
            "data/github/check_run/2026/11/13/10/b": b"b",
            "data/github/check_run/2026/11/14/c": b"next day",
            "data/github/push/2026/11/13/d": b"other type",
            "data/github/create/2026/11/13/e": b"other type",
        }
    )
    raw = FakeS3Client()

    assert mirror(source, raw).success

    assert raw.objects == {
        "github/raw/check_run/2026/11/13/a": b"a",
        "github/raw/check_run/2026/11/13/10/b": b"b",
        "github/raw/check_run/2026/11/14/c": b"next day",
    }


def test_rerunning_copies_nothing_new_and_reports_the_files_already_cached():
    source = FakeS3Client(objects={DELIVERY: b"x"})
    raw = FakeS3Client()
    mirror(source, raw)

    result = mirror(source, raw)

    assert result.success
    assert len(raw.objects) == 1
    metadata = metadata_of(result)
    assert metadata["files_copied"].value == 0
    assert metadata["files_already_cached"].value == 1
    assert metadata["bytes_copied"].value == 0


def test_the_run_reports_files_copied_already_cached_and_bytes_copied_and_no_days():
    source = FakeS3Client(
        objects={
            "data/github/check_run/2026/11/13/a": b"one",
            "data/github/check_run/2026/11/14/b": b"three",
        }
    )
    raw = FakeS3Client(objects={"github/raw/check_run/2026/11/13/a": b"one"})

    result = mirror(source, raw)

    metadata = metadata_of(result)
    assert metadata["files_copied"].value == 1
    assert metadata["files_already_cached"].value == 1
    assert metadata["bytes_copied"].value == len(b"three")
    assert "days" not in metadata


def test_the_github_raw_assets_are_unpartitioned_and_there_is_one_per_event_type():
    assert check_run_files.partitions_def is None
    assert len(GITHUB_RAW_ASSETS) == 17
    assert all(asset.partitions_def is None for asset in GITHUB_RAW_ASSETS)


def test_the_github_source_resource_talks_to_the_bucket_in_its_own_region():
    from aws_data.defs.resources import aws_source_s3
    from aws_data.lib.github_s3 import GITHUB_SOURCE_REGION

    resource = aws_source_s3("key-id", "secret", region=GITHUB_SOURCE_REGION)

    assert resource.region_name == "ap-southeast-1"
    assert resource.endpoint_url == "https://s3.ap-southeast-1.amazonaws.com"
