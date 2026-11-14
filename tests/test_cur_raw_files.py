import dagster as dg
from dagster._core.test_utils import freeze_time

from aws_data.defs.cur.cur_raw_files import cur_files
from tests.conftest import (
    AFTER_THE_DAYS_UNDER_TEST,
    FakeS3Client,
    FakeS3Resource,
)

EXPORT = "cur-reports/with-resource-ids-split-hourly-parquet-overwrite"


def cur_file(billing_period: str, name: str = "part-00001.snappy.parquet") -> str:
    return f"{EXPORT}/data/BILLING_PERIOD={billing_period}/{name}"


def mirror(source: FakeS3Client, raw: FakeS3Client, partition_key: str = "2026-11-01") -> dg.ExecuteInProcessResult:
    with freeze_time(AFTER_THE_DAYS_UNDER_TEST):
        return dg.materialize(
            [cur_files],
            partition_key=partition_key,
            resources={
                "aws_s3": FakeS3Resource(source),
                "minio_s3": FakeS3Resource(raw),
                "raw_files_io_manager": dg.mem_io_manager,
            },
        )


def test_a_billing_periods_files_are_mirrored_into_raw_byte_for_byte():
    source = FakeS3Client(objects={cur_file("2026-11"): b"PAR1pretend-parquet-bytes"})
    raw = FakeS3Client()

    result = mirror(source, raw)

    assert result.success
    assert raw.objects == {f"aws-data/raw/cur/{cur_file('2026-11')}": b"PAR1pretend-parquet-bytes"}


def reported(result: dg.ExecuteInProcessResult) -> dict:
    """What the run reported about itself, as plain values."""
    [materialization] = result.asset_materializations_for_node("aws_data__raw__cur_files")
    return {name: value.value for name, value in materialization.metadata.items()}


def test_a_file_restated_in_place_is_copied_again():
    source = FakeS3Client(objects={cur_file("2026-11"): b"restated, so longer"})
    raw = FakeS3Client(objects={f"aws-data/raw/cur/{cur_file('2026-11')}": b"first delivery"})

    result = mirror(source, raw)

    assert result.success
    assert raw.objects == {f"aws-data/raw/cur/{cur_file('2026-11')}": b"restated, so longer"}
    assert reported(result)["files_copied"] == 1


def test_a_file_aws_no_longer_has_is_deleted_from_raw_and_other_billing_periods_are_left_alone():
    source = FakeS3Client(objects={cur_file("2026-11", "part-00002.snappy.parquet"): b"current"})
    raw = FakeS3Client(
        objects={
            f"aws-data/raw/cur/{cur_file('2026-11', 'part-00001.snappy.parquet')}": b"superseded",
            f"aws-data/raw/cur/{cur_file('2026-10')}": b"last month, not ours to touch",
        }
    )

    result = mirror(source, raw)

    assert result.success
    assert raw.objects == {
        f"aws-data/raw/cur/{cur_file('2026-11', 'part-00002.snappy.parquet')}": b"current",
        f"aws-data/raw/cur/{cur_file('2026-10')}": b"last month, not ours to touch",
    }
    assert reported(result)["files_deleted"] == 1


def test_running_again_with_nothing_changed_copies_and_deletes_nothing():
    source = FakeS3Client(objects={cur_file("2026-11"): b"unchanged"})
    raw = FakeS3Client()
    mirror(source, raw)

    result = mirror(source, raw)

    assert result.success
    assert reported(result) == {"files_copied": 0, "files_already_cached": 1, "files_deleted": 0, "bytes_copied": 0}
