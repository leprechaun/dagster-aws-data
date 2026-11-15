import dagster as dg
from dagster._core.test_utils import freeze_time

from aws_data.defs.github.github_bronze_assets import (
    GITHUB_BRONZE_ASSETS,
    github_bronze_asset,
)
from aws_data.lib.github_s3 import GITHUB_EVENT_TYPES
from tests.conftest import AFTER_THE_DAYS_UNDER_TEST, FakeS3Client, FakeS3Resource
from tests.github_envelopes import envelope, one_line

CHECK_RUN_DAY_1 = "github/raw/check_run/2026/09/08/webhooks-2-2026-09-08-17-17-42-aaa"
CHECK_RUN_DAY_2 = "github/raw/check_run/2026/09/09/webhooks-2-2026-09-09-18-02-10-bbb"
PUSH_DAY_1 = "github/raw/push/2026/09/08/webhooks-2-2026-09-08-09-00-00-ccc"


def materialize(asset: dg.AssetsDefinition, raw: FakeS3Client) -> dg.ExecuteInProcessResult:
    with freeze_time(AFTER_THE_DAYS_UNDER_TEST):
        return dg.materialize(
            [asset],
            resources={
                "minio_s3": FakeS3Resource(raw),
                "github_bronze_io_manager": dg.mem_io_manager,
            },
        )


def test_every_raw_file_in_the_folder_on_every_day_becomes_rows_in_one_table():
    raw = FakeS3Client(
        objects={
            CHECK_RUN_DAY_1: one_line(envelope(id="first", time="2026-09-08T17:17:42Z"))
            + one_line(envelope(id="second", time="2026-09-08T23:59:59Z")),
            CHECK_RUN_DAY_2: one_line(envelope(id="third", time="2026-09-09T00:00:00Z")),
        }
    )
    asset = github_bronze_asset("check_run")

    result = materialize(asset, raw)

    assert result.success
    df = result.output_for_node(asset.op.name)
    assert sorted(df["id"].to_list()) == ["first", "second", "third"]
    assert set(df["source_file"].to_list()) == {CHECK_RUN_DAY_1, CHECK_RUN_DAY_2}


def test_an_event_type_only_reads_its_own_folder():
    raw = FakeS3Client(
        objects={
            CHECK_RUN_DAY_1: one_line(envelope(id="a-check-run")),
            PUSH_DAY_1: one_line(
                envelope(id="a-push", detail_type="push", action=None)
            ),
        }
    )
    asset = github_bronze_asset("push")

    result = materialize(asset, raw)

    df = result.output_for_node(asset.op.name)
    assert df["id"].to_list() == ["a-push"]


def test_with_no_raw_files_the_table_is_empty_and_does_not_fail():
    asset = github_bronze_asset("check_run")

    result = materialize(asset, FakeS3Client())

    assert result.success
    assert result.output_for_node(asset.op.name).height == 0


def test_there_is_one_unpartitioned_bronze_table_per_event_type_under_github():
    assert len(GITHUB_BRONZE_ASSETS) == len(GITHUB_EVENT_TYPES) == 17
    for event_type, asset in zip(GITHUB_EVENT_TYPES, GITHUB_BRONZE_ASSETS):
        key = dg.AssetKey(["github", "bronze", event_type])
        assert asset.keys == {key}
        assert asset.partitions_def is None
        assert asset.get_io_manager_key_for_asset_key(key) == "github_bronze_io_manager"
        assert asset.op.name == f"github__bronze__{event_type}"


def test_every_bronze_table_has_a_description_so_it_is_written_to_the_delta_table():
    for event_type, asset in zip(GITHUB_EVENT_TYPES, GITHUB_BRONZE_ASSETS):
        spec = asset.specs_by_key[dg.AssetKey(["github", "bronze", event_type])]
        assert spec.description
        assert f"`{event_type}`" in spec.description


def test_the_tables_are_written_under_github_in_storage():
    from aws_data.defs.resources import resources
    from aws_data.lib.cloudtrail_s3 import STORAGE_BUCKET

    manager = resources().resources["github_bronze_io_manager"]

    assert manager.root_uri == f"s3://{STORAGE_BUCKET}/github"
