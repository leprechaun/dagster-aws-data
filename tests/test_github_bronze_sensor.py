import dagster as dg

from aws_data.defs.github.github_bronze_assets import GITHUB_BRONZE_ASSETS
from aws_data.defs.github.github_bronze_sensor import (
    RAW_KEYS,
    bronze_key,
    github_bronze_job,
    github_bronze_on_raw_change,
)
from aws_data.defs.github.github_raw_files import GITHUB_RAW_ASSETS
from aws_data.defs.resources import resources
from aws_data.lib.github_bronze import event_types_with_new_files
from aws_data.lib.github_s3 import GITHUB_EVENT_TYPES


def test_only_types_that_copied_a_file_are_rebuilt_and_sorted():
    copied = {"push": [0, 0], "check_run": [0, 3], "create": [1]}

    assert event_types_with_new_files(copied) == ["check_run", "create"]


def test_a_run_that_copied_nothing_rebuilds_nothing():
    assert event_types_with_new_files({"check_run": [0], "push": [0, 0]}) == []


def test_no_runs_at_all_rebuilds_nothing():
    assert event_types_with_new_files({}) == []


def test_the_sensor_watches_the_raw_asset_of_every_event_type():
    assert set(RAW_KEYS) == set(GITHUB_EVENT_TYPES)
    assert RAW_KEYS["check_run"] == dg.AssetKey(["github", "raw", "check_run_files"])


def report_raw_run(
    instance: dg.DagsterInstance, event_type: str, files_copied: int
) -> None:
    instance.report_runless_asset_event(
        dg.AssetMaterialization(
            asset_key=RAW_KEYS[event_type],
            metadata={"files_copied": files_copied},
        )
    )


def tick(instance: dg.DagsterInstance, cursor: str | None = None) -> dg.SensorResult:
    context = dg.build_multi_asset_sensor_context(
        monitored_assets=list(RAW_KEYS.values()),
        instance=instance,
        cursor=cursor,
        definitions=dg.Definitions.merge(
            resources(),
            dg.Definitions(
                assets=[*GITHUB_RAW_ASSETS, *GITHUB_BRONZE_ASSETS],
                jobs=[github_bronze_job],
                sensors=[github_bronze_on_raw_change],
            ),
        ),
    )
    return github_bronze_on_raw_change.evaluate_tick(context)


def requested_selection(result: dg.SensorResult) -> set[dg.AssetKey]:
    return {key for request in result.run_requests for key in request.asset_selection}


def test_a_raw_run_that_copied_files_triggers_only_that_types_bronze():
    with dg.instance_for_test() as instance:
        report_raw_run(instance, "check_run", 4)
        report_raw_run(instance, "push", 0)

        result = tick(instance)

    assert requested_selection(result) == {bronze_key("check_run")}
    assert len(result.run_requests) == 1


def test_a_raw_run_that_copied_nothing_triggers_nothing():
    with dg.instance_for_test() as instance:
        report_raw_run(instance, "check_run", 0)

        result = tick(instance)

    assert result.run_requests == []


def test_a_tick_only_sees_raw_runs_since_the_last_tick():
    with dg.instance_for_test() as instance:
        report_raw_run(instance, "check_run", 2)
        first = tick(instance)
        cursor = first.cursor

        report_raw_run(instance, "push", 1)
        second = tick(instance, cursor=cursor)

    assert requested_selection(first) == {bronze_key("check_run")}
    assert requested_selection(second) == {bronze_key("push")}


def test_copies_in_several_runs_since_the_last_tick_all_count():
    with dg.instance_for_test() as instance:
        report_raw_run(instance, "release", 5)
        report_raw_run(instance, "release", 0)

        result = tick(instance)

    assert requested_selection(result) == {bronze_key("release")}


def test_the_sensor_triggers_the_bronze_job_on_its_own_schedule_and_starts_running():
    assert github_bronze_on_raw_change.default_status == dg.DefaultSensorStatus.RUNNING
    assert github_bronze_on_raw_change.minimum_interval_seconds == 300
    assert github_bronze_on_raw_change.job_name == "github_bronze_refresh"
