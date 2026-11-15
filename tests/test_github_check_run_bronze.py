import dagster as dg
from dagster._core.test_utils import freeze_time

from aws_data.defs.github.github_check_run_bronze import check_run
from tests.conftest import AFTER_THE_DAYS_UNDER_TEST, FakeS3Client, FakeS3Resource
from tests.github_envelopes import envelope, one_line

RAW_DAY_1 = "github/raw/check_run/2026/09/08/webhooks-2-2026-09-08-17-17-42-aaa"
RAW_DAY_2 = "github/raw/check_run/2026/09/08/webhooks-2-2026-09-08-18-02-10-bbb"


def materialize_day(raw: FakeS3Client, partition_key: str = "2026-09-08") -> dg.ExecuteInProcessResult:
    with freeze_time(AFTER_THE_DAYS_UNDER_TEST):
        return dg.materialize(
            [check_run],
            partition_key=partition_key,
            resources={
                "minio_s3": FakeS3Resource(raw),
                "io_manager": dg.mem_io_manager,
            },
        )


def test_a_days_raw_files_become_that_days_bronze_rows():
    raw = FakeS3Client(
        objects={
            RAW_DAY_1: one_line(envelope(id="first", time="2026-09-08T17:17:42Z")),
            RAW_DAY_2: one_line(envelope(id="second", time="2026-09-08T18:02:10Z"))
            + one_line(envelope(id="third", time="2026-09-08T23:59:59Z")),
        }
    )

    result = materialize_day(raw)

    assert result.success
    df = result.output_for_node(check_run.op.name)
    assert sorted(df["id"].to_list()) == ["first", "second", "third"]
    assert set(df["source_file"].to_list()) == {RAW_DAY_1, RAW_DAY_2}


def test_a_day_with_no_raw_files_gives_no_rows_and_does_not_fail():
    result = materialize_day(FakeS3Client())

    assert result.success
    assert result.output_for_node(check_run.op.name).height == 0
