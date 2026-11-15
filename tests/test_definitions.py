import dagster as dg

from aws_data.definitions import defs


def test_the_project_definitions_load_with_unpartitioned_github_raw_assets():
    repository = defs().get_repository_def()

    raw = repository.assets_defs_by_key[
        dg.AssetKey(["github", "raw", "check_run_files"])
    ]
    assert raw.partitions_def is None
