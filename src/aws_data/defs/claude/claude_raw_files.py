import dagster as dg
from dagster_aws.s3 import S3Resource

from aws_data.lib.claude_s3 import CLAUDE_SOURCE_PREFIX, SESSION_STATUS, claude_raw_key
from aws_data.lib.cloudtrail_s3 import PROJECT_PREFIX, STORAGE_BUCKET

# Claude and GitHub sources share the one firehose bucket, and github_source_s3
# already carries that bucket's region.
from aws_data.lib.github_s3 import SOURCE_BUCKET
from aws_data.lib.s3 import list_keys, mirror_keys


def claude_raw_files_asset(detail_type: str) -> dg.AssetsDefinition:
    """One unpartitioned asset per detail-type folder. Each run lists the folder's
    whole source and copies every file Raw doesn't have yet."""

    @dg.asset(
        # Asset names can't contain "-", so `session-status` becomes `session_status_files`.
        name=f"{detail_type.replace('-', '_')}_files",
        key_prefix=["claude", "raw"],
        group_name="claude",
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
        folder = f"{CLAUDE_SOURCE_PREFIX}/{detail_type}/"
        mirrored = mirror_keys(
            source_client,
            SOURCE_BUCKET,
            list_keys(source_client, SOURCE_BUCKET, folder),
            minio_s3.get_client(),
            STORAGE_BUCKET,
            claude_raw_key,
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


SESSION_STATUS_FILES = claude_raw_files_asset(SESSION_STATUS)
