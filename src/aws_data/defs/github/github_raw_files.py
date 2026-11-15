from datetime import date

import dagster as dg
from dagster_aws.s3 import S3Resource

from aws_data.lib.cloudtrail_s3 import PROJECT_PREFIX, STORAGE_BUCKET
from aws_data.lib.github_s3 import (
    GITHUB_PARTITIONS_DEF,
    RAW_PREFIX,
    SOURCE_BUCKET,
    github_day_prefix,
    github_raw_key,
)
from aws_data.lib.s3 import mirror_prefixes


@dg.asset(
    name="check_run_files",
    key_prefix=["github", "raw"],
    partitions_def=GITHUB_PARTITIONS_DEF,
    group_name="github",
    kinds={"s3"},
    io_manager_key="raw_files_io_manager",
    # Shared project pool (see PROJECT_PREFIX), as for the other raw assets.
    pool=PROJECT_PREFIX,
)
def check_run_files(
    context: dg.AssetExecutionContext,
    github_source_s3: S3Resource,
    minio_s3: S3Resource,
) -> None:
    """Mirrors one UTC day of check_run deliveries from the firehose bucket to
    Raw, byte-for-byte, with the `data/` segment dropped from each key."""
    day = date.fromisoformat(context.partition_key)
    mirrored = mirror_prefixes(
        github_source_s3.get_client(),
        SOURCE_BUCKET,
        [github_day_prefix("check_run", day)],
        "",
        minio_s3.get_client(),
        STORAGE_BUCKET,
        RAW_PREFIX,
        dest_key=github_raw_key,
    )

    context.add_output_metadata(
        {
            "files_copied": mirrored.files_copied,
            "files_already_cached": mirrored.files_already_cached,
            "bytes_copied": mirrored.bytes_copied,
        }
    )
