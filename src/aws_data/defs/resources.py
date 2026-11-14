import os

import dagster as dg
from dagster_aws.s3 import S3Resource
from dagster_delta import S3Config
from dagster_delta.io_manager.arrow import _DeltaLakePyArrowTypeHandler
from dagster_delta.io_manager.base import TableConnection
from dagster_delta.io_manager.polars import (
    DeltaLakePolarsIOManager,
    _DeltaLakePolarsTypeHandler,
)
from deltalake import DeltaTable

from aws_data.lib.cloudtrail_s3 import PROJECT_PREFIX, SOURCE_REGION, STORAGE_BUCKET


def aws_source_s3(
    access_key_id: str | dg.EnvVar,
    secret_access_key: str | dg.EnvVar,
    region: str = SOURCE_REGION,
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


def sync_table_description(dt: DeltaTable, description: str | None) -> bool:
    """Set the table's Delta-level description, returning whether it changed.

    Each alter is its own commit in the Delta log, so only write when the
    description actually differs.
    """
    if not description or dt.metadata().description == description:
        return False
    dt.alter.set_table_description(description)
    return True


def column(name: str, description: str | None = None, **tags: str) -> dg.TableColumn:
    """A `dagster/column_schema` entry carrying only a description and tags
    (e.g. `unit="bpm"`). The type is left blank on purpose: Delta Lake already
    records it, and TableColumn's "string" default would be wrong for most columns.
    """
    return dg.TableColumn(name, type="", description=description, tags=tags or None)


def column_metadata_updates(
    schema: dg.TableSchema | None, current: dict[str, dict[str, str]]
) -> dict[str, dict[str, str]]:
    """Map each column whose declared metadata isn't already on its Delta field
    to the metadata to set.

    A column's description is stored under `comment` (the Spark/Unity Catalog
    convention), alongside its tags verbatim. Keys are only ever added or
    overwritten, never removed. Raises if the schema declares a column the
    table doesn't have, since that means the declaration has drifted.
    """
    if schema is None:
        return {}
    updates = {}
    for col in schema.columns:
        if col.name not in current:
            raise ValueError(
                f"column_schema declares {col.name!r}, which the table doesn't have"
            )
        desired = dict(col.tags or {})
        if col.description:
            desired["comment"] = col.description
        existing = current[col.name]
        if any(existing.get(k) != v for k, v in desired.items()):
            updates[col.name] = desired
    return updates


def sync_column_metadata(dt: DeltaTable, schema: dg.TableSchema | None) -> list[str]:
    """Copy declared column descriptions/tags onto the table's fields,
    returning the columns that changed. One commit per changed column.
    """
    current = {f.name: dict(f.metadata) for f in dt.schema().fields}
    updates = column_metadata_updates(schema, current)
    for col, metadata in updates.items():
        dt.alter.set_column_metadata(col, metadata)
    return list(updates)


class DescribedDeltaLakePolarsTypeHandler(_DeltaLakePolarsTypeHandler):
    def handle_output(
        self, context: dg.OutputContext, table_slice, obj, connection: TableConnection
    ):
        super().handle_output(context, table_slice, obj, connection)
        dt = DeltaTable(
            connection.table_uri, storage_options=connection.storage_options
        )
        if sync_table_description(dt, context.asset_spec.description):
            context.log.info(
                f"Updated Delta table description on {connection.table_uri}"
            )
        schema = (context.definition_metadata or {}).get("dagster/column_schema")
        if changed := sync_column_metadata(dt, schema):
            context.log.info(
                f"Updated Delta column metadata on {connection.table_uri}: {changed}"
            )


class DescribedDeltaLakePolarsIOManager(DeltaLakePolarsIOManager):
    """DeltaLakePolarsIOManager that also copies each asset's description, and
    its `dagster/column_schema` column descriptions/tags, onto its Delta table,
    so readers outside Dagster (e.g. the marimo notebooks) see them.
    """

    @staticmethod
    def type_handlers():
        return [DescribedDeltaLakePolarsTypeHandler(), _DeltaLakePyArrowTypeHandler()]


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

    # MinIO isn't guaranteed to support the conditional-put semantics delta-rs
    # otherwise relies on for safe concurrent commits, so delta-rs is told to
    # commit by an unsafe rename, and writers to the same table must not run
    # with concurrency > 1. dagster-delta's S3Config has no field for it, so it
    # is set where delta-rs also reads it, in the environment (unless already set).
    os.environ.setdefault("AWS_S3_ALLOW_UNSAFE_RENAME", "true")

    # Default IO manager for table-shaped assets: writes Polars DataFrames to
    # Delta tables on MinIO. Table location is <root_uri>/<schema>/<table>,
    # where <schema> is taken from an asset's key_prefix and <table> from its
    # name - so key_prefix maps 1:1 to the medallion layer (bronze/silver/gold).
    # Only those last two asset-key parts place a table, so the project
    # namespace lives in the root: s3://<bucket>/<project>, beside the Raw files.
    deltalake_io_manager = DescribedDeltaLakePolarsIOManager(
        root_uri=f"s3://{STORAGE_BUCKET}/{PROJECT_PREFIX}",
        storage_options=S3Config(
            access_key_id=dg.EnvVar("AWS_ACCESS_KEY_ID"),
            secret_access_key=dg.EnvVar("AWS_SECRET_ACCESS_KEY"),
            endpoint=dg.EnvVar("AWS_ENDPOINT_URL_S3"),
            region="us-east-1",
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
