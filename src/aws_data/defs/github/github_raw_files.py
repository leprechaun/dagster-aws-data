from datetime import date

import dagster as dg
from dagster_aws.s3 import S3Resource

from aws_data.lib.cloudtrail_s3 import PROJECT_PREFIX, STORAGE_BUCKET
from aws_data.lib.github_s3 import (
    GITHUB_EVENT_TYPES,
    GITHUB_PARTITIONS_DEF,
    SOURCE_BUCKET,
    SOURCE_PREFIX,
    github_raw_key,
    keys_for_days,
)
from aws_data.lib.s3 import list_keys, mirror_keys


def github_raw_files_asset(event_type: str) -> dg.AssetsDefinition:
    """One asset per event type. It is a single-run backfill: a backfill of many
    days is one run that lists the type's folder once and copies every selected
    day, rather than one run per day."""

    @dg.asset(
        name=f"{event_type}_files",
        key_prefix=["github", "raw"],
        partitions_def=GITHUB_PARTITIONS_DEF,
        backfill_policy=dg.BackfillPolicy.single_run(),
        group_name="github",
        kinds={"s3"},
        io_manager_key="raw_files_io_manager",
        # Shared project pool (see PROJECT_PREFIX), as for the other raw assets.
        pool=PROJECT_PREFIX,
    )
    def _asset(
        context: dg.AssetExecutionContext,
        github_source_s3: S3Resource,
        minio_s3: S3Resource,
    ) -> None:
        days = {date.fromisoformat(key) for key in context.partition_keys}
        source_client = github_source_s3.get_client()
        folder = f"{SOURCE_PREFIX}/{event_type}/"
        keys = list_keys(source_client, SOURCE_BUCKET, folder)
        mirrored = mirror_keys(
            source_client,
            SOURCE_BUCKET,
            keys_for_days(keys, event_type, days),
            minio_s3.get_client(),
            STORAGE_BUCKET,
            github_raw_key,
            folder,
        )

        context.add_output_metadata(
            {
                "days": len(days),
                "files_copied": mirrored.files_copied,
                "files_already_cached": mirrored.files_already_cached,
                "bytes_copied": mirrored.bytes_copied,
            }
        )

    return _asset


GITHUB_RAW_ASSETS: list[dg.AssetsDefinition] = [
    github_raw_files_asset(t) for t in GITHUB_EVENT_TYPES
]
