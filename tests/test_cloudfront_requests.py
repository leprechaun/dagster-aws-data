import gzip
import json
from datetime import date, datetime, timedelta, timezone

import dagster as dg
import polars as pl
from dagster._core.test_utils import freeze_time

from aws_data.defs.cloudfront.cloudfront_requests import (
    _row_from_record,
    cloudfront_requests,
    parse_log_file,
)
from tests.conftest import (
    AFTER_THE_DAYS_UNDER_TEST,
    FakeS3Client,
    FakeS3Resource,
    log_file,
)

RAW_PREFIX = "aws-data/raw/cloudfront/"
ACCOUNT = "111111111111"
DISTRIBUTION = "E1EXAMPLE0001"

REQUEST = {
    "date": "2026-11-13",
    "time": "09:13:40",
    "x-edge-location": "HEL51-P5",
    "c-ip": "203.0.113.4",
    "cs-method": "GET",
    "x-host-header": "www.example.com",
    "cs-uri-stem": "/",
    "cs-uri-query": "-",
    "sc-status": "200",
    "sc-bytes": "944",
    "time-taken": "0.537",
}


class CapturingIOManager(dg.IOManager):
    """Records what the asset would have written to Delta Lake."""

    def __init__(self):
        self.outputs: dict[str, pl.DataFrame] = {}

    def handle_output(self, context: dg.OutputContext, obj) -> None:
        self.outputs[context.asset_key.to_user_string()] = obj

    def load_input(self, context: dg.InputContext):
        raise NotImplementedError


def raw_log_file(account: str, distribution: str, day: str, hour: str, *requests: dict) -> tuple[str, bytes]:
    """One Log File as it sits in the Raw mirror: the source key under the Raw prefix."""
    body = "".join(json.dumps(request) + "\n" for request in requests)
    return RAW_PREFIX + log_file(account, distribution, day, hour), gzip.compress(body.encode())


def bronze_table(raw_objects: dict[str, bytes], partition_key: str = "2026-11-13") -> pl.DataFrame:
    """Run the Bronze asset against a fake Raw mirror. There is deliberately no AWS
    source resource: Bronze must read only Raw."""
    io_manager = CapturingIOManager()
    with freeze_time(AFTER_THE_DAYS_UNDER_TEST):
        result = dg.materialize(
            [cloudfront_requests],
            partition_key=partition_key,
            resources={"minio_s3": FakeS3Resource(FakeS3Client(objects=raw_objects)), "io_manager": io_manager},
        )
    assert result.success
    return io_manager.outputs["aws-data/bronze/cloudfront_requests"]


def test_a_request_becomes_a_row_with_its_request_day_account_and_distribution():
    key, body = raw_log_file(ACCOUNT, DISTRIBUTION, "2026-11-13", "09", REQUEST)

    table = bronze_table({key: body})

    assert table.height == 1
    row = table.row(0, named=True)
    assert row["request_date"] == date(2026, 11, 13)
    assert row["account_id"] == ACCOUNT
    assert row["distribution_id"] == DISTRIBUTION


def test_the_request_time_is_a_utc_timestamp_from_the_lines_date_and_time():
    key, body = raw_log_file(ACCOUNT, DISTRIBUTION, "2026-11-13", "09", REQUEST)

    row = bronze_table({key: body}).row(0, named=True)

    assert row["request_time"] == datetime(2026, 11, 13, 9, 13, 40, tzinfo=timezone.utc)


def test_textual_fields_are_strings_and_a_dash_means_missing():
    key, body = raw_log_file(ACCOUNT, DISTRIBUTION, "2026-11-13", "09", REQUEST)

    row = bronze_table({key: body}).row(0, named=True)

    assert row["x_edge_location"] == "HEL51-P5"
    assert row["c_ip"] == "203.0.113.4"
    assert row["cs_method"] == "GET"
    assert row["x_host_header"] == "www.example.com"
    assert row["cs_uri_stem"] == "/"
    assert row["cs_uri_query"] is None


def test_numeric_fields_are_numbers():
    key, body = raw_log_file(ACCOUNT, DISTRIBUTION, "2026-11-13", "09", REQUEST)

    table = bronze_table({key: body})
    row = table.row(0, named=True)

    assert row["sc_status"] == 200
    assert row["sc_bytes"] == 944
    assert row["time_taken"] == 0.537
    assert table.schema["sc_status"] == pl.Int64
    assert table.schema["time_taken"] == pl.Float64


def test_each_row_records_its_log_file_and_when_it_was_ingested():
    key, body = raw_log_file(ACCOUNT, DISTRIBUTION, "2026-11-13", "09", REQUEST)

    row = bronze_table({key: body}).row(0, named=True)

    assert row["source_key"] == key
    assert timedelta(0) <= datetime.now(timezone.utc) - row["ingested_at"] < timedelta(minutes=1)


def test_the_full_original_line_is_kept_as_json():
    key, body = raw_log_file(ACCOUNT, DISTRIBUTION, "2026-11-13", "09", REQUEST)

    row = bronze_table({key: body}).row(0, named=True)

    assert json.loads(row["raw_json"]) == REQUEST


def test_a_day_with_no_log_files_gives_an_empty_table_with_typed_columns():
    other_day_key, body = raw_log_file(ACCOUNT, DISTRIBUTION, "2026-11-14", "00", REQUEST)

    table = bronze_table({other_day_key: body})

    assert table.height == 0
    assert table.schema["request_date"] == pl.Date
    assert table.schema["request_time"] == pl.Datetime("us", "UTC")
    assert table.schema["account_id"] == pl.Utf8
    assert table.schema["sc_status"] == pl.Int64
    assert table.schema["time_taken"] == pl.Float64
    assert table.schema["ingested_at"] == pl.Datetime("us", "UTC")
    assert table.schema["raw_json"] == pl.Utf8


def test_the_asset_is_registered_after_raw_under_the_project_namespace_in_the_shared_pool():
    key = cloudfront_requests.key
    [spec] = cloudfront_requests.specs

    assert key.path == ["aws-data", "bronze", "cloudfront_requests"]
    assert cloudfront_requests.asset_deps[key] == {dg.AssetKey(["aws-data", "raw", "cloudfront_files"])}
    assert cloudfront_requests.partitions_def.start.date().isoformat() == "2026-11-13"
    assert cloudfront_requests.group_names_by_key[key] == "cloudfront"
    assert spec.kinds == {"s3", "deltalake"}
    assert spec.metadata["partition_expr"] == "request_date"
    assert cloudfront_requests.op.pool == "aws-data"


def test_parse_log_file_returns_one_object_per_line():
    other = {**REQUEST, "time": "09:14:00", "sc-status": "404"}
    body = gzip.compress((json.dumps(REQUEST) + "\n" + json.dumps(other) + "\n").encode())

    assert parse_log_file(body) == [REQUEST, other]


def test_row_from_record_maps_one_line_to_one_row():
    ingested_at = datetime(2026, 11, 14, 0, 5, tzinfo=timezone.utc)

    row = _row_from_record(REQUEST, date(2026, 11, 13), ACCOUNT, DISTRIBUTION, "raw/some/key.gz", ingested_at)

    assert row == {
        "request_date": date(2026, 11, 13),
        "request_time": datetime(2026, 11, 13, 9, 13, 40, tzinfo=timezone.utc),
        "account_id": ACCOUNT,
        "distribution_id": DISTRIBUTION,
        "x_edge_location": "HEL51-P5",
        "c_ip": "203.0.113.4",
        "cs_method": "GET",
        "x_host_header": "www.example.com",
        "cs_uri_stem": "/",
        "cs_uri_query": None,
        "sc_status": 200,
        "sc_bytes": 944,
        "cs_bytes": None,
        "c_port": None,
        "sc_content_len": None,
        "sc_range_start": None,
        "sc_range_end": None,
        "time_taken": 0.537,
        "time_to_first_byte": None,
        "source_key": "raw/some/key.gz",
        "ingested_at": ingested_at,
        "raw_json": json.dumps(REQUEST),
    }
