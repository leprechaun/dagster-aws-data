import dagster as dg
import polars as pl
from dagster_aws.s3 import S3Resource

from aws_data.lib.cloudtrail_s3 import PROJECT_PREFIX, STORAGE_BUCKET
from aws_data.lib.github_bronze import bronze_schema, parse_raw_file, raw_prefix
from aws_data.lib.github_s3 import GITHUB_EVENT_TYPES
from aws_data.lib.s3 import list_keys


def github_bronze_asset(event_type: str) -> dg.AssetsDefinition:
    """One unpartitioned Bronze table per event type, rebuilt in full from every
    Raw file of that type. All of Raw is under 200 MB, so this is cheap."""

    @dg.asset(
        name=event_type,
        key_prefix=["github", "bronze"],
        group_name="github",
        kinds={"s3", "deltalake"},
        description=(
            f"Bronze: every GitHub `{event_type}` webhook delivery, one row per delivery, "
            f"rebuilt in full from the Raw mirror under github/raw/{event_type}/. Envelope "
            "and common GitHub fields as columns, then any typed columns for the type; "
            "the full payload as JSON in `payload`."
        ),
        deps=[dg.AssetKey(["github", "raw", f"{event_type}_files"])],
        io_manager_key="github_bronze_io_manager",
        pool=PROJECT_PREFIX,
    )
    def _asset(minio_s3: S3Resource) -> pl.DataFrame:
        client = minio_s3.get_client()
        frames = [
            parse_raw_file(
                client.get_object(Bucket=STORAGE_BUCKET, Key=key)["Body"].read(),
                key,
                event_type,
            )
            for key in list_keys(client, STORAGE_BUCKET, raw_prefix(event_type))
        ]
        if not frames:
            return pl.DataFrame(schema=bronze_schema(event_type))
        return pl.concat(frames)

    return _asset


GITHUB_BRONZE_ASSETS: list[dg.AssetsDefinition] = [
    github_bronze_asset(t) for t in GITHUB_EVENT_TYPES
]
