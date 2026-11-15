import dagster as dg

from aws_data.defs.github.github_bronze_assets import GITHUB_BRONZE_ASSETS
from aws_data.lib.github_bronze import event_types_with_new_files
from aws_data.lib.github_s3 import GITHUB_EVENT_TYPES

# Raw mirror asset keys, one per event type: github/raw/<type>_files.
RAW_KEYS = {t: dg.AssetKey(["github", "raw", f"{t}_files"]) for t in GITHUB_EVENT_TYPES}


def bronze_key(event_type: str) -> dg.AssetKey:
    return dg.AssetKey(["github", "bronze", event_type])


def files_copied(record) -> int:
    """The `files_copied` a Raw mirror run reported. A run from before that
    metadata existed has none: it counts as 1, so Bronze is rebuilt to be safe."""
    metadata = record.event_log_entry.dagster_event.event_specific_data.materialization.metadata
    value = metadata.get("files_copied")
    return value.value if value is not None else 1


github_bronze_job = dg.define_asset_job(
    "github_bronze_refresh",
    selection=GITHUB_BRONZE_ASSETS,
)


@dg.multi_asset_sensor(
    monitored_assets=list(RAW_KEYS.values()),
    job=github_bronze_job,
    name="github_bronze_on_raw_change",
    minimum_interval_seconds=300,
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def github_bronze_on_raw_change(context: dg.MultiAssetSensorEvaluationContext):
    """Rebuilds the Bronze table of each event type that gained a Raw file since
    the last tick. A tick with no new files launches nothing."""
    copied_by_type: dict[str, list[int]] = {}
    latest_storage_id = None
    for event_type, key in RAW_KEYS.items():
        records = list(context.materialization_records_for_key(key))
        if not records:
            continue
        copied_by_type[event_type] = [files_copied(record) for record in records]
        newest = max(record.storage_id for record in records)
        latest_storage_id = max(latest_storage_id or newest, newest)

    changed = event_types_with_new_files(copied_by_type)
    context.advance_all_cursors()
    if not changed:
        return

    yield dg.RunRequest(
        run_key=f"{','.join(changed)}@{latest_storage_id}",
        asset_selection=[bronze_key(t) for t in changed],
    )
