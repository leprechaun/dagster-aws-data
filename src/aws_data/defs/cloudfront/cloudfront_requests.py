import gzip
import io
import re
from datetime import date, datetime, timezone

import dagster as dg
import polars as pl
from dagster_aws.s3 import S3Resource

from aws_data.defs.cloudfront.cloudfront_raw_files import cloudfront_files
from aws_data.defs.resources import column
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
INT_COLUMNS = [
    "sc_status",
    "sc_bytes",
    "cs_bytes",
    "c_port",
    "sc_content_len",
    "sc_range_start",
    "sc_range_end",
]
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


_DASH = ' `"-"` in the log means missing, and is null here.'

CLOUDFRONT_COLUMNS = dg.TableSchema(
    columns=[
        column(
            "request_date",
            "Request Day: the UTC day in the Log File's folder, and the partition key.",
        ),
        column(
            "request_time",
            "When CloudFront finished serving the request (UTC), from the log's date and time. Null if either failed to parse.",
        ),
        column(
            "account_id",
            "The Account owning the Distribution, taken from the Log File's key.",
        ),
        column("distribution_id", "The Distribution, taken from the Log File's key."),
        column(
            "x_edge_location",
            "The edge location that served the request, e.g. `SYD62-P1`.",
        ),
        column("c_ip", "Viewer's IP address (IPv4 or IPv6)."),
        column("cs_method", "HTTP method."),
        column("x_host_header", "The Host header value CloudFront received."),
        column("cs_uri_stem", "Path of the request, without the query string."),
        column("cs_uri_query", "Query string, without the `?`."),
        column(
            "cs_host",
            "The distribution's domain name, e.g. `d111111abcdef8.cloudfront.net`.",
        ),
        column("cs_referer", "Referer header. Viewer-controlled."),
        column(
            "cs_user_agent",
            "User-Agent header. Viewer-controlled, so not to be trusted.",
        ),
        column(
            "cs_cookie",
            "Cookie header. Null unless cookie logging is on, and can hold sensitive values.",
        ),
        column(
            "x_edge_result_type",
            "How the edge classified the response: `Hit`, `Miss`, `RefreshHit`, `Error`...",
        ),
        column("x_edge_request_id", "CloudFront's id for the request."),
        column("cs_protocol", "`http`, `https`, `ws` or `wss`."),
        column(
            "x_forwarded_for",
            "X-Forwarded-For chain when the viewer came through a proxy.",
        ),
        column("ssl_protocol", "TLS version, e.g. `TLSv1.3`. Null for plain HTTP."),
        column("ssl_cipher", "TLS cipher negotiated. Null for plain HTTP."),
        column(
            "x_edge_response_result_type",
            "How the edge classified the response as sent to the viewer. Can differ from x_edge_result_type (e.g. a Miss served as a `Hit` after a stale refresh).",
        ),
        column("cs_protocol_version", "HTTP version, e.g. `HTTP/2.0`."),
        column("fle_status", "Field-level encryption status."),
        column(
            "fle_encrypted_fields", "Number of fields field-level encryption encrypted."
        ),
        column(
            "x_edge_detailed_result_type",
            "Like x_edge_result_type with more detail, e.g. `OriginShieldHit`.",
        ),
        column("sc_content_type", "Content-Type of the response."),
        column("c_country", "Viewer's two-letter country code, from GeoIP."),
        column("asn", "Viewer's autonomous system number, as text."),
        column(
            "cache_behavior_path_pattern",
            "Path pattern of the cache behavior that matched the request.",
        ),
        column(
            "sc_status",
            "HTTP status returned to the viewer. Null if it didn't parse as a number.",
        ),
        column("sc_bytes", "Bytes sent to the viewer, headers included.", unit="bytes"),
        column("cs_bytes", "Bytes the viewer sent, headers included.", unit="bytes"),
        column("c_port", "Viewer's source port."),
        column("sc_content_len", "Content-Length of the response.", unit="bytes"),
        column(
            "sc_range_start",
            "Start of the byte range served, for range responses only.",
        ),
        column(
            "sc_range_end", "End of the byte range served, for range responses only."
        ),
        column(
            "time_taken",
            "Seconds from CloudFront receiving the request to sending the last byte of the response.",
            unit="seconds",
        ),
        column(
            "time_to_first_byte",
            "Seconds from receiving the request to sending the first byte.",
            unit="seconds",
        ),
        column("source_key", "Raw key of the Log File the request came from."),
        column(
            "ingested_at",
            "When this row was parsed into Bronze (UTC). One value per Log File.",
        ),
        column(
            "raw_json",
            "The original log line as JSON, for fields not promoted to columns.",
        ),
    ]
)


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
        fields = pl.read_ndjson(
            io.BytesIO("\n".join(lines).encode()), infer_schema_length=None
        )
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
        *[
            field(column).cast(pl.Int64, strict=False).alias(column)
            for column in INT_COLUMNS
        ],
        *[
            field(column).cast(pl.Float64, strict=False).alias(column)
            for column in FLOAT_COLUMNS
        ],
        pl.lit(source_key).alias("source_key"),
        pl.lit(ingested_at).alias("ingested_at"),
        pl.col("raw_json"),
    ).cast(CLOUDFRONT_SCHEMA)


@dg.asset(
    key_prefix=[PROJECT_PREFIX, "bronze"],
    partitions_def=CLOUDFRONT_PARTITIONS_DEF,
    group_name="cloudfront",
    kinds={"s3", "deltalake"},
    metadata={
        "partition_expr": "request_date",
        "dagster/column_schema": CLOUDFRONT_COLUMNS,
    },
    description=(
        "Bronze: every CloudFront request delivered, one row per Request, parsed from the Raw "
        "mirror and partitioned by Request Day. Fields as CloudFront delivers them, with a "
        '"-" (missing) value as null and numbers typed; anything without a column is in raw_json.'
    ),
    deps=[cloudfront_files],
    # Shared project pool (see PROJECT_PREFIX), which must be limited to 1:
    # MinIO needs allow_unsafe_rename, so concurrent commits to the same Delta
    # table can race. The limit lives in instance config, not in this repo.
    pool=PROJECT_PREFIX,
)
def cloudfront_requests(
    context: dg.AssetExecutionContext, minio_s3: S3Resource
) -> pl.DataFrame:
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
        distribution_ids = list_distributions(
            client, STORAGE_BUCKET, RAW_ROOT_PREFIX, account_id
        )
        distributions_seen.update(distribution_ids)

        for distribution_id in distribution_ids:
            for key in list_day_object_keys(
                client,
                STORAGE_BUCKET,
                RAW_ROOT_PREFIX,
                account_id,
                distribution_id,
                partition_date,
            ):
                body = client.get_object(Bucket=STORAGE_BUCKET, Key=key)["Body"].read()
                try:
                    parsed = parse_log_file(body)
                except (OSError, ValueError) as e:
                    context.log.warning(
                        f"Skipping unreadable CloudFront log file {key}: {e}"
                    )
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
