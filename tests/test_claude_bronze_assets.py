import dagster as dg
from dagster._core.test_utils import freeze_time

from aws_data.defs.claude.claude_bronze_assets import SESSION_STATUS_BRONZE
from aws_data.lib.claude_bronze import bronze_schema
from tests.claude_envelopes import envelope, one_line
from tests.conftest import AFTER_THE_DAYS_UNDER_TEST, FakeS3Client, FakeS3Resource

STATUS_DAY_1 = (
    "claude/raw/session-status/2026/11/13/statusline-1-2026-11-13-10-32-19-aaa"
)
STATUS_DAY_2 = (
    "claude/raw/session-status/2026/11/14/statusline-1-2026-11-14-09-00-00-bbb"
)
OTHER_FOLDER = "claude/raw/stats/2026/11/13/stats-1-2026-11-13-10-00-00-ccc"


def materialize(raw: FakeS3Client) -> dg.ExecuteInProcessResult:
    with freeze_time(AFTER_THE_DAYS_UNDER_TEST):
        return dg.materialize(
            [SESSION_STATUS_BRONZE],
            resources={
                "minio_s3": FakeS3Resource(raw),
                "claude_bronze_io_manager": dg.mem_io_manager,
            },
        )


def test_every_raw_file_on_every_day_becomes_rows_in_one_table():
    raw = FakeS3Client(
        objects={
            STATUS_DAY_1: one_line(envelope(id="first"))
            + one_line(envelope(id="second", time="2026-11-13T23:59:59Z")),
            STATUS_DAY_2: one_line(envelope(id="third", time="2026-11-14T00:00:00Z")),
        }
    )

    result = materialize(raw)

    assert result.success
    df = result.output_for_node(SESSION_STATUS_BRONZE.op.name)
    assert sorted(df["id"].to_list()) == ["first", "second", "third"]
    assert set(df["source_file"].to_list()) == {STATUS_DAY_1, STATUS_DAY_2}


def test_only_the_session_status_folder_is_read():
    raw = FakeS3Client(
        objects={
            STATUS_DAY_1: one_line(envelope(id="a-status")),
            OTHER_FOLDER: b"not a session status",
        }
    )

    result = materialize(raw)

    df = result.output_for_node(SESSION_STATUS_BRONZE.op.name)
    assert df["id"].to_list() == ["a-status"]


def test_with_no_raw_files_the_table_is_empty_with_the_declared_schema():
    result = materialize(FakeS3Client())

    assert result.success
    df = result.output_for_node(SESSION_STATUS_BRONZE.op.name)
    assert df.height == 0
    assert df.schema == bronze_schema()


def test_the_table_is_the_unpartitioned_claude_bronze_asset_with_its_own_io_manager():
    key = dg.AssetKey(["claude", "bronze", "session_status"])

    assert SESSION_STATUS_BRONZE.keys == {key}
    assert SESSION_STATUS_BRONZE.partitions_def is None
    assert SESSION_STATUS_BRONZE.get_io_manager_key_for_asset_key(key) == (
        "claude_bronze_io_manager"
    )
    assert SESSION_STATUS_BRONZE.specs_by_key[key].group_name == "claude"
    assert SESSION_STATUS_BRONZE.specs_by_key[key].kinds == {"s3", "deltalake"}
    assert dg.AssetKey(["claude", "raw", "session_status_files"]) in {
        dep.asset_key for dep in SESSION_STATUS_BRONZE.specs_by_key[key].deps
    }


def test_the_description_names_a_claude_session_status_and_warns_against_summing():
    description = SESSION_STATUS_BRONZE.specs_by_key[
        dg.AssetKey(["claude", "bronze", "session_status"])
    ].description

    assert "Claude Session Status" in description
    assert "running totals" in description
    assert "must not be summed" in description
