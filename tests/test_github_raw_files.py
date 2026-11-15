import dagster as dg
from dagster._core.test_utils import freeze_time

from aws_data.defs.github.github_raw_files import github_raw_files_asset

check_run_files = github_raw_files_asset("check_run")
from tests.conftest import (
    AFTER_THE_DAYS_UNDER_TEST,
    FakeS3Client,
    FakeS3Resource,
)

DELIVERY = "data/github/check_run/2026/11/13/webhooks-2-2026-11-13-10-32-19-6738f2f0"


def mirror(
    source: FakeS3Client, raw: FakeS3Client, partition_key: str = "2026-11-13"
) -> dg.ExecuteInProcessResult:
    with freeze_time(AFTER_THE_DAYS_UNDER_TEST):
        return dg.materialize(
            [check_run_files],
            partition_key=partition_key,
            resources={
                "github_source_s3": FakeS3Resource(source),
                "minio_s3": FakeS3Resource(raw),
                "raw_files_io_manager": dg.mem_io_manager,
            },
        )


def test_a_check_run_delivery_is_mirrored_into_github_raw_byte_for_byte():
    source = FakeS3Client(objects={DELIVERY: b'{"detail-type": "check_run"}'})
    raw = FakeS3Client()

    result = mirror(source, raw)

    assert result.success
    assert raw.objects == {
        "github/raw/check_run/2026/11/13/webhooks-2-2026-11-13-10-32-19-6738f2f0": b'{"detail-type": "check_run"}'
    }


def test_only_check_run_deliveries_for_the_partition_day_are_mirrored():
    day = {
        "data/github/check_run/2026/11/13/a": b"a",
        "data/github/check_run/2026/11/13/10/b": b"b",
    }
    other = {
        "data/github/check_run/2026/11/14/c": b"next day",
        "data/github/push/2026/11/13/d": b"other type",
        "data/github/create/2026/11/13/e": b"other type",
    }
    source = FakeS3Client(objects={**day, **other})
    raw = FakeS3Client()

    assert mirror(source, raw).success

    assert raw.objects == {
        "github/raw/check_run/2026/11/13/a": b"a",
        "github/raw/check_run/2026/11/13/10/b": b"b",
    }


def test_rerunning_the_partition_copies_nothing_new():
    source = FakeS3Client(objects={DELIVERY: b"x"})
    raw = FakeS3Client()
    mirror(source, raw)

    result = mirror(source, raw)

    assert result.success
    assert len(raw.objects) == 1


def test_the_github_source_resource_talks_to_the_bucket_in_its_own_region():
    from aws_data.defs.resources import aws_source_s3
    from aws_data.lib.github_s3 import GITHUB_SOURCE_REGION

    resource = aws_source_s3("key-id", "secret", region=GITHUB_SOURCE_REGION)

    assert resource.region_name == "ap-southeast-1"
    assert resource.endpoint_url == "https://s3.ap-southeast-1.amazonaws.com"
