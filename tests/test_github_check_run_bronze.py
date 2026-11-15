import dagster as dg
from dagster._core.test_utils import freeze_time

from aws_data.defs.github.github_check_run_bronze import check_run
from tests.conftest import AFTER_THE_DAYS_UNDER_TEST, FakeS3Client, FakeS3Resource
from tests.github_envelopes import envelope, one_line

RAW_DAY_1 = "github/raw/check_run/2026/09/08/webhooks-2-2026-09-08-17-17-42-aaa"
RAW_DAY_2 = "github/raw/check_run/2026/09/09/webhooks-2-2026-09-09-18-02-10-bbb"


def materialize_bronze(raw: FakeS3Client) -> dg.ExecuteInProcessResult:
    with freeze_time(AFTER_THE_DAYS_UNDER_TEST):
        return dg.materialize(
            [check_run],
            resources={
                "minio_s3": FakeS3Resource(raw),
                "github_bronze_io_manager": dg.mem_io_manager,
            },
        )


def test_every_raw_file_on_every_day_becomes_rows_in_one_table():
    raw = FakeS3Client(
        objects={
            RAW_DAY_1: one_line(envelope(id="first", time="2026-09-08T17:17:42Z"))
            + one_line(envelope(id="second", time="2026-09-08T23:59:59Z")),
            RAW_DAY_2: one_line(envelope(id="third", time="2026-09-09T00:00:00Z")),
        }
    )

    result = materialize_bronze(raw)

    assert result.success
    df = result.output_for_node(check_run.op.name)
    assert sorted(df["id"].to_list()) == ["first", "second", "third"]
    assert set(df["source_file"].to_list()) == {RAW_DAY_1, RAW_DAY_2}


def test_with_no_raw_files_the_table_is_empty_and_does_not_fail():
    result = materialize_bronze(FakeS3Client())

    assert result.success
    assert result.output_for_node(check_run.op.name).height == 0


def test_the_table_is_not_partitioned():
    assert check_run.partitions_def is None


def test_the_table_is_written_under_github_in_storage():
    from aws_data.defs.resources import resources
    from aws_data.lib.cloudtrail_s3 import STORAGE_BUCKET

    key = dg.AssetKey(["github", "bronze", "check_run"])
    io_manager_key = check_run.get_io_manager_key_for_asset_key(key)
    manager = resources().resources[io_manager_key]

    assert io_manager_key == "github_bronze_io_manager"
    assert manager.root_uri == f"s3://{STORAGE_BUCKET}/github"
