import gzip
import json
from datetime import date, datetime, timezone

import dagster as dg
import polars as pl
from dagster_aws.s3 import S3Resource

from aws_data.defs.cloudfront.cloudfront_raw_files import cloudfront_files
from aws_data.lib.cloudfront_s3 import (
    CLOUDFRONT_PARTITIONS_DEF,
    CLOUDFRONT_RAW_PREFIX,
    CLOUDFRONT_SOURCE_PREFIX,
    list_accounts,
    list_day_object_keys,
    list_distributions,
)
from aws_data.lib.cloudtrail_s3 import PROJECT_PREFIX, STORAGE_BUCKET

# Reads from the raw layer's mirror on MinIO (see cloudfront_raw_files.py),
# not AWS directly - so changing bronze logic never re-downloads from AWS.
RAW_ROOT_PREFIX = f"{CLOUDFRONT_RAW_PREFIX}/{CLOUDFRONT_SOURCE_PREFIX}"

# Delivered log lines are JSON objects keyed by CloudFront's field names
# ("cs(User-Agent)", "x-edge-location", ...), with every value a string and
# "-" for a missing one.
STRING_COLUMNS = [
    "x_edge_location",
    "c_ip",
    "cs_method",
    "x_host_header",
    "cs_uri_stem",
    "cs_uri_query",
]
INT_COLUMNS = ["sc_status", "sc_bytes", "cs_bytes", "c_port", "sc_content_len", "sc_range_start", "sc_range_end"]
FLOAT_COLUMNS = ["time_taken", "time_to_first_byte"]


CLOUDFRONT_SCHEMA = {
    # The UTC day in the Log File's folder, i.e. the day the requests were made.
    "request_date": pl.Date,
    "request_time": pl.Datetime("us", "UTC"),
    "account_id": pl.Utf8,
    "distribution_id": pl.Utf8,
    **{column: pl.Utf8 for column in STRING_COLUMNS},
    **{column: pl.Int64 for column in INT_COLUMNS},
    **{column: pl.Float64 for column in FLOAT_COLUMNS},
    "source_key": pl.Utf8,
    "ingested_at": pl.Datetime("us", "UTC"),
    # Full original line, for fields not promoted to columns above.
    "raw_json": pl.Utf8,
}


def _clean(value):
    """CloudFront writes "-" for a missing value."""
    return None if value in (None, "", "-") else value


def _to_int(value) -> int | None:
    return None if value is None else int(value)


def _to_float(value) -> float | None:
    return None if value is None else float(value)


def parse_log_file(body: bytes) -> list[dict]:
    """The JSON objects, one per line, of a gzipped Log File."""
    text = gzip.decompress(body).decode("utf-8")
    return [json.loads(line) for line in text.splitlines()]


def _row_from_record(
    record: dict, request_date: date, account_id: str, distribution_id: str, source_key: str, ingested_at: datetime
) -> dict:
    fields = {name.lower().replace("-", "_"): _clean(value) for name, value in record.items()}
    request_time = datetime.strptime(f"{fields['date']} {fields['time']}", "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=timezone.utc
    )
    return {
        "request_date": request_date,
        "request_time": request_time,
        "account_id": account_id,
        "distribution_id": distribution_id,
        **{column: fields.get(column) for column in STRING_COLUMNS},
        **{column: _to_int(fields.get(column)) for column in INT_COLUMNS},
        **{column: _to_float(fields.get(column)) for column in FLOAT_COLUMNS},
        "source_key": source_key,
        "ingested_at": ingested_at,
        "raw_json": json.dumps(record),
    }


@dg.asset(
    key_prefix=[PROJECT_PREFIX, "bronze"],
    partitions_def=CLOUDFRONT_PARTITIONS_DEF,
    group_name="cloudfront",
    kinds={"s3", "deltalake"},
    metadata={"partition_expr": "request_date"},
    deps=[cloudfront_files],
    # Shared project pool (see PROJECT_PREFIX), which must be limited to 1:
    # MinIO needs allow_unsafe_rename, so concurrent commits to the same Delta
    # table can race. The limit lives in instance config, not in this repo.
    pool=PROJECT_PREFIX,
)
def cloudfront_requests(context: dg.AssetExecutionContext, minio_s3: S3Resource) -> pl.DataFrame:
    """One day of CloudFront requests, parsed from the raw layer's MinIO
    mirror into the bronze Delta table on MinIO."""
    partition_date = date.fromisoformat(context.partition_key)
    client = minio_s3.get_client()

    rows = []
    for account_id in list_accounts(client, STORAGE_BUCKET, RAW_ROOT_PREFIX):
        for distribution_id in list_distributions(client, STORAGE_BUCKET, RAW_ROOT_PREFIX, account_id):
            for key in list_day_object_keys(
                client, STORAGE_BUCKET, RAW_ROOT_PREFIX, account_id, distribution_id, partition_date
            ):
                body = client.get_object(Bucket=STORAGE_BUCKET, Key=key)["Body"].read()
                ingested_at = datetime.now(timezone.utc)
                rows.extend(
                    _row_from_record(record, partition_date, account_id, distribution_id, key, ingested_at)
                    for record in parse_log_file(body)
                )

    return pl.DataFrame(rows, schema=CLOUDFRONT_SCHEMA)
