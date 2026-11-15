import dagster as dg

from aws_data.definitions import defs
from aws_data.lib.github_s3 import GITHUB_EVENT_TYPES


def test_the_project_definitions_load_with_unpartitioned_github_raw_assets():
    repository = defs().get_repository_def()

    raw = repository.assets_defs_by_key[
        dg.AssetKey(["github", "raw", "check_run_files"])
    ]
    assert raw.partitions_def is None


def test_the_project_loads_with_no_github_bronze_sensor():
    sensor_names = {sensor.name for sensor in defs().get_repository_def().sensor_defs}

    assert not any("bronze" in name for name in sensor_names)


def test_each_github_bronze_asset_depends_on_its_raw_asset():
    asset_graph = defs().resolve_asset_graph()

    for event_type in GITHUB_EVENT_TYPES:
        bronze = dg.AssetKey(["github", "bronze", event_type])
        raw = dg.AssetKey(["github", "raw", f"{event_type}_files"])
        assert raw in asset_graph.get(bronze).parent_keys
