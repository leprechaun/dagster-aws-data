import dagster as dg
from dagster_aws.s3 import S3Resource
from dagster_deltalake import S3Config
from dagster_deltalake_polars import DeltaLakePolarsIOManager

from aws_data.lib.cloudtrail_s3 import SOURCE_REGION


def aws_source_s3(
    access_key_id: str | dg.EnvVar, secret_access_key: str | dg.EnvVar, region: str = SOURCE_REGION
) -> S3Resource:
    """S3 client for the real AWS source bucket. Everything is passed
    explicitly, including the endpoint, so the ambient AWS_* variables (which
    other pipelines in this environment point at MinIO, e.g. AWS_ENDPOINT_URL_S3)
    can never redirect or re-credential it. An explicit endpoint also turns off
    botocore's cross-region redirect, so it must be the bucket's own region."""
    return S3Resource(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name=region,
        endpoint_url=f"https://s3.{region}.amazonaws.com",
    )


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
    # Source data: real AWS S3, with its own SOURCE_AWS_* credentials. The
    # plain AWS_* variables are not used here: in this environment they point
    # at MinIO (see below).
    aws_s3 = aws_source_s3(
        dg.EnvVar("SOURCE_AWS_ACCESS_KEY_ID"),
        dg.EnvVar("SOURCE_AWS_SECRET_ACCESS_KEY"),
    )

    # Storage: MinIO running on the k3s cluster, spoken to via the S3-compatible
    # API. It uses the standard AWS_* variables (shared with the other
    # pipelines on this cluster), passed explicitly rather than left to boto3.
    minio_s3 = S3Resource(
        endpoint_url=dg.EnvVar("AWS_ENDPOINT_URL_S3"),
        aws_access_key_id=dg.EnvVar("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=dg.EnvVar("AWS_SECRET_ACCESS_KEY"),
        region_name="us-east-1",
    )

    # Default IO manager for table-shaped assets: writes Polars DataFrames to
    # Delta tables on MinIO. Table location is <root_uri>/<schema>/<table>,
    # where <schema> is taken from an asset's key_prefix and <table> from its
    # name - so key_prefix maps 1:1 to the medallion layer (bronze/silver/gold).
    deltalake_io_manager = DeltaLakePolarsIOManager(
        root_uri=dg.EnvVar("MINIO_LAKEHOUSE_ROOT_URI"),
        storage_options=S3Config(
            access_key_id=dg.EnvVar("AWS_ACCESS_KEY_ID"),
            secret_access_key=dg.EnvVar("AWS_SECRET_ACCESS_KEY"),
            endpoint=dg.EnvVar("AWS_ENDPOINT_URL_S3"),
            region="us-east-1",
            # MinIO isn't guaranteed to support the conditional-put semantics
            # delta-rs otherwise relies on for safe concurrent commits, so
            # writers to the same table must not run with concurrency > 1.
            allow_unsafe_rename=True,
        ),
    )

    return dg.Definitions(
        resources={
            "aws_s3": aws_s3,
            "minio_s3": minio_s3,
            "io_manager": deltalake_io_manager,
            "raw_files_io_manager": RawFilesIOManager(),
        }
    )
