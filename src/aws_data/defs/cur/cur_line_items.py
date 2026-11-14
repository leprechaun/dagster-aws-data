import io
from collections.abc import Iterator
from datetime import date, datetime, timezone

import dagster as dg
import polars as pl
from dagster_aws.s3 import S3Resource

from aws_data.defs.cur.cur_raw_files import cur_files
from aws_data.defs.resources import column
from aws_data.lib.cloudtrail_s3 import PROJECT_PREFIX, STORAGE_BUCKET
from aws_data.lib.cur_s3 import (
    CUR_PARTITIONS_DEF,
    CUR_RAW_PREFIX,
    CUR_SOURCE_EXPORT_PREFIX,
    billing_period,
    billing_period_prefix,
)
from aws_data.lib.s3 import list_keys

# Only the columns Bronze adds are declared: the rest are whatever AWS delivers,
# and vary by month.
CUR_COLUMNS = dg.TableSchema(
    columns=[
        column(
            "billing_period",
            "Billing Period: the first day (UTC) of the month the Line Item belongs to, and the partition key.",
        ),
        column(
            "ingested_at",
            "When this row was parsed into Bronze (UTC). One value per Billing Period per refresh.",
        ),
    ]
)


def clean_line_items(raw: pl.DataFrame, billing_period_start: date, ingested_at: datetime) -> pl.DataFrame:
    """The Line Items read from Raw as Bronze rows: every column as delivered,
    plus the Billing Period (as its first day, the partition key) and when
    they were ingested (a timezone-aware UTC time)."""
    return raw.with_columns(
        pl.lit(billing_period_start).alias("billing_period"),
        pl.lit(ingested_at).alias("ingested_at"),
    )


@dg.asset(
    key_prefix=[PROJECT_PREFIX, "bronze"],
    partitions_def=CUR_PARTITIONS_DEF,
    group_name="cur",
    kinds={"s3", "deltalake"},
    metadata={
        "partition_expr": "billing_period",
        # A Billing Period can bring columns an earlier one lacked (AWS adds
        # them over time); earlier months read back as null in those columns.
        "schema_mode": "merge",
        "dagster/column_schema": CUR_COLUMNS,
    },
    description=(
        "Bronze: every Line Item of the Cost and Usage Report, one row per Line Item, read from the Raw "
        "mirror and partitioned by Billing Period. Every column as AWS delivers it; map columns such as "
        "resource_tags are lists of key/value pairs."
    ),
    deps=[cur_files],
    # A Billing Period AWS hasn't delivered yet is left alone, not materialized.
    output_required=False,
    # Shared project pool (see PROJECT_PREFIX), which must be limited to 1:
    # see cloudfront_requests.
    pool=PROJECT_PREFIX,
)
def cur_line_items(context: dg.AssetExecutionContext, minio_s3: S3Resource) -> Iterator[dg.Output[pl.DataFrame]]:
    """One Billing Period of Line Items, read from the raw layer's MinIO
    mirror into the bronze Delta table on MinIO."""
    period = billing_period(context.partition_key)
    client = minio_s3.get_client()
    prefix = f"{CUR_RAW_PREFIX}/{billing_period_prefix(CUR_SOURCE_EXPORT_PREFIX, period)}"

    keys = list_keys(client, STORAGE_BUCKET, prefix, ".parquet")
    if not keys:
        context.log.warning(f"No Cost and Usage Report files in Raw yet for Billing Period {period}; leaving it as it was")
        return

    frames = []
    unreadable: dict[str, str] = {}
    for key in keys:
        body = client.get_object(Bucket=STORAGE_BUCKET, Key=key)["Body"].read()
        try:
            frames.append(pl.read_parquet(io.BytesIO(body)))
        except (pl.exceptions.PolarsError, OSError) as e:
            unreadable[key] = str(e)
    # The Billing Period is replaced as a whole, so a month missing any file
    # would silently lose its Line Items: fail before writing anything.
    if unreadable:
        raise dg.Failure(
            f"{len(unreadable)} Cost and Usage Report file(s) in Raw for Billing Period {period} "
            "can't be read; leaving it as it was",
            metadata={"unreadable_files": unreadable},
        )

    # Files of one Billing Period can carry different columns (AWS adds
    # them over time), so they are stacked by name with nulls for the gaps.
    line_items = pl.concat(frames, how="diagonal_relaxed")

    context.add_output_metadata({"files_read": len(frames), "line_items_read": line_items.height})

    yield dg.Output(clean_line_items(line_items, date.fromisoformat(context.partition_key), datetime.now(timezone.utc)))
