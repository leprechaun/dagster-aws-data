import gzip
import io
import re
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
    "cs_host",
    "cs_referer",
    "cs_user_agent",
    "cs_cookie",
    "x_edge_result_type",
    "x_edge_request_id",
    "cs_protocol",
    "x_forwarded_for",
    "ssl_protocol",
    "ssl_cipher",
    "x_edge_response_result_type",
    "cs_protocol_version",
    "fle_status",
    "fle_encrypted_fields",
    "x_edge_detailed_result_type",
    "sc_content_type",
    "c_country",
    "asn",
    "cache_behavior_path_pattern",
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


def _column_name(field: str) -> str:
    """CloudFront field name -> column name: "cs(User-Agent)" -> "cs_user_agent"."""
    return re.sub(r"[^a-z0-9]+", "_", field.lower()).strip("_")


def parse_log_file(body: bytes) -> pl.DataFrame:
    """A gzipped Log File as a table: one row per line, a column per delivered
    field (named as delivered), plus the original line as `raw_json`. Raises
    OSError (bad gzip) or ValueError (bad encoding, bad JSON, or a line that
    isn't a JSON object): a file is read whole or not at all."""
    text = gzip.decompress(body).decode("utf-8")
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        return pl.DataFrame({"raw_json": []}, schema={"raw_json": pl.Utf8})
    try:
        fields = pl.read_ndjson(io.BytesIO("\n".join(lines).encode()), infer_schema_length=None)
    except (pl.exceptions.ComputeError, pl.exceptions.SchemaError) as e:
        raise ValueError(str(e)) from e
    return fields.with_columns(pl.Series("raw_json", lines))


def clean_requests(
    raw: pl.DataFrame,
    request_date: date,
    account_id: str,
    distribution_id: str,
    source_key: str,
    ingested_at: datetime,
) -> pl.DataFrame:
    """The table from `parse_log_file`, as typed Bronze rows in CLOUDFRONT_SCHEMA.
    A field a delivery lacks is a null column; "-" is null; numbers that don't
    parse are null; fields with no column survive only in `raw_json`."""
    source_of = {_column_name(name): name for name in raw.columns if name != "raw_json"}

    def field(column: str) -> pl.Expr:
        if column not in source_of:
            return pl.lit(None, dtype=pl.Utf8)
        value = pl.col(source_of[column]).cast(pl.Utf8)
        return pl.when(value.is_in(["", "-"])).then(None).otherwise(value)

    request_time = (
        (field("date") + pl.lit(" ") + field("time"))
        .str.strptime(pl.Datetime("us"), "%Y-%m-%d %H:%M:%S", strict=False)
        .dt.replace_time_zone("UTC")
    )
    return raw.select(
        pl.lit(request_date).alias("request_date"),
        request_time.alias("request_time"),
        pl.lit(account_id).alias("account_id"),
        pl.lit(distribution_id).alias("distribution_id"),
        *[field(column).alias(column) for column in STRING_COLUMNS],
        *[field(column).cast(pl.Int64, strict=False).alias(column) for column in INT_COLUMNS],
        *[field(column).cast(pl.Float64, strict=False).alias(column) for column in FLOAT_COLUMNS],
        pl.lit(source_key).alias("source_key"),
        pl.lit(ingested_at).alias("ingested_at"),
        pl.col("raw_json"),
    ).cast(CLOUDFRONT_SCHEMA)


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

    frames = []
    files_read = 0
    files_skipped = 0
    distributions_seen: set[str] = set()

    accounts = list_accounts(client, STORAGE_BUCKET, RAW_ROOT_PREFIX)

    for account_id in accounts:
        distribution_ids = list_distributions(client, STORAGE_BUCKET, RAW_ROOT_PREFIX, account_id)
        distributions_seen.update(distribution_ids)

        for distribution_id in distribution_ids:
            for key in list_day_object_keys(
                client, STORAGE_BUCKET, RAW_ROOT_PREFIX, account_id, distribution_id, partition_date
            ):
                body = client.get_object(Bucket=STORAGE_BUCKET, Key=key)["Body"].read()
                try:
                    parsed = parse_log_file(body)
                except (OSError, ValueError) as e:
                    context.log.warning(f"Skipping unreadable CloudFront log file {key}: {e}")
                    files_skipped += 1
                    continue

                frames.append(
                    clean_requests(
                        parsed,
                        partition_date,
                        account_id,
                        distribution_id,
                        key,
                        datetime.now(timezone.utc),
                    )
                )
                files_read += 1

    requests = pl.concat(frames) if frames else pl.DataFrame(schema=CLOUDFRONT_SCHEMA)

    context.add_output_metadata(
        {
            "files_read": files_read,
            "files_skipped": files_skipped,
            "requests_read": requests.height,
            "accounts": accounts,
            "distributions": sorted(distributions_seen),
        }
    )

    return requests
