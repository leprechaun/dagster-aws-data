import os

import dagster as dg
from dagster_aws.s3 import S3Resource
from dagster_deltalake import S3Config
from dagster_deltalake.config import ClientConfig
from dagster_deltalake_polars import DeltaLakePolarsIOManager


def _minio_use_ssl() -> bool:
    return os.getenv("MINIO_USE_SSL", "false").strip().lower() in ("1", "true", "yes")


class RawFilesIOManager(dg.ConfigurableIOManager):
    """For assets that persist their own output as a side effect (e.g.
    copying raw files to MinIO directly) rather than through an IO manager.
    Downstream assets that need those files read them back independently."""

    def handle_output(self, context: dg.OutputContext, obj) -> None:
        pass

    def load_input(self, context: dg.InputContext):
        raise NotImplementedError(
            f"Asset '{context.asset_key}' is not loadable through an IO manager; "
            "downstream assets should read its output directly instead of taking it as an input."
        )


@dg.definitions
def resources() -> dg.Definitions:
    # Source data: real AWS S3. Credentials/region come from the standard
    # boto3 chain (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_REGION env
    # vars, ~/.aws/credentials, etc.) since there's no IAM-role auto-injection
    # on bare-metal k3s.
    aws_s3 = S3Resource()

    # Storage: MinIO running on the k3s cluster, spoken to via the S3-compatible API.
    minio_s3 = S3Resource(
        endpoint_url=dg.EnvVar("MINIO_ENDPOINT"),
        aws_access_key_id=dg.EnvVar("MINIO_ACCESS_KEY"),
        aws_secret_access_key=dg.EnvVar("MINIO_SECRET_KEY"),
        region_name="us-east-1",
        use_ssl=_minio_use_ssl(),
    )

    # Default IO manager for table-shaped assets: writes Polars DataFrames to
    # Delta tables on MinIO. Table location is <root_uri>/<schema>/<table>,
    # where <schema> is taken from an asset's key_prefix and <table> from its
    # name - so key_prefix maps 1:1 to the medallion layer (bronze/silver/gold).
    deltalake_io_manager = DeltaLakePolarsIOManager(
        root_uri=dg.EnvVar("MINIO_LAKEHOUSE_ROOT_URI"),
        storage_options=S3Config(
            access_key_id=dg.EnvVar("MINIO_ACCESS_KEY"),
            secret_access_key=dg.EnvVar("MINIO_SECRET_KEY"),
            endpoint=dg.EnvVar("MINIO_ENDPOINT"),
            region="us-east-1",
            # MinIO isn't guaranteed to support the conditional-put semantics
            # delta-rs otherwise relies on for safe concurrent commits, so
            # writers to the same table must not run with concurrency > 1.
            allow_unsafe_rename=True,
        ),
        client_options=ClientConfig(allow_http=not _minio_use_ssl()),
    )

    return dg.Definitions(
        resources={
            "aws_s3": aws_s3,
            "minio_s3": minio_s3,
            "io_manager": deltalake_io_manager,
            "raw_files_io_manager": RawFilesIOManager(),
        }
    )
