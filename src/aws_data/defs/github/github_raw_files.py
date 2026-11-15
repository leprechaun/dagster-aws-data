import dagster as dg
from dagster_aws.s3 import S3Resource

from aws_data.lib.cloudtrail_s3 import PROJECT_PREFIX, STORAGE_BUCKET
from aws_data.lib.github_s3 import (
    GITHUB_EVENT_TYPES,
    SOURCE_BUCKET,
    SOURCE_PREFIX,
    github_raw_key,
)
from aws_data.lib.s3 import list_keys, mirror_keys


def github_raw_files_asset(event_type: str) -> dg.AssetsDefinition:
    """One unpartitioned asset per event type. Each run lists the type's whole
    source folder and copies every file Raw doesn't have yet."""

    @dg.asset(
        name=f"{event_type}_files",
        key_prefix=["github", "raw"],
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
        source_client = github_source_s3.get_client()
        folder = f"{SOURCE_PREFIX}/{event_type}/"
        mirrored = mirror_keys(
            source_client,
            SOURCE_BUCKET,
            list_keys(source_client, SOURCE_BUCKET, folder),
            minio_s3.get_client(),
            STORAGE_BUCKET,
            github_raw_key,
            folder,
        )

        context.add_output_metadata(
            {
                "files_copied": mirrored.files_copied,
                "files_already_cached": mirrored.files_already_cached,
                "bytes_copied": mirrored.bytes_copied,
            }
        )

    return _asset


GITHUB_RAW_ASSETS: list[dg.AssetsDefinition] = [
    github_raw_files_asset(t) for t in GITHUB_EVENT_TYPES
]
