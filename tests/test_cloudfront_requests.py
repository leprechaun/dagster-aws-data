import gzip
import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import dagster as dg
import polars as pl
import pytest
from dagster._core.test_utils import freeze_time

from aws_data.defs.cloudfront.cloudfront_requests import (
    CLOUDFRONT_SCHEMA,
    _column_name,
    clean_requests,
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


# A "narrow" delivery: fewer fields, but three the full one lacks.
NARROW_REQUEST = {
    **REQUEST,
    "c-country": "US",
    "asn": "64500",
    "cs-bytes": "190",
    "x-edge-result-type": "Miss",
    "cache-behavior-path-pattern": "*",
}

# A "full" delivery: every field, including CloudFront's parenthesised names.
FULL_REQUEST = {
    "date": "2026-11-13",
    "time": "23:59:58",
    "x-edge-location": "SIN52-P1",
    "sc-bytes": "1024",
    "c-ip": "203.0.113.9",
    "cs-method": "GET",
    "cs(Host)": "d111111abcdef8.cloudfront.net",
    "cs-uri-stem": "/index.html",
    "sc-status": "304",
    "cs(Referer)": "https://example.com/",
    "cs(User-Agent)": "Mozilla/5.0",
    "cs-uri-query": "a=1",
    "cs(Cookie)": "-",
    "x-edge-result-type": "Hit",
    "x-edge-request-id": "req-1",
    "x-host-header": "www.example.com",
    "cs-protocol": "https",
    "cs-bytes": "300",
    "time-taken": "0.002",
    "x-forwarded-for": "-",
    "ssl-protocol": "TLSv1.3",
    "ssl-cipher": "TLS_AES_128_GCM_SHA256",
    "x-edge-response-result-type": "Hit",
    "cs-protocol-version": "HTTP/2.0",
    "fle-status": "-",
    "fle-encrypted-fields": "-",
    "c-port": "51234",
    "time-to-first-byte": "0.001",
    "x-edge-detailed-result-type": "Hit",
    "sc-content-type": "text/html",
    "sc-content-len": "1024",
    "sc-range-start": "-",
    "sc-range-end": "-",
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


@dataclass
class BronzeRun:
    table: pl.DataFrame
    report: dict  # what the run reported about itself, as plain values
    warnings: list[str]


def run_bronze(raw_objects: dict[str, bytes], partition_key: str = "2026-11-13") -> BronzeRun:
    """Run the Bronze asset against a fake Raw mirror. There is deliberately no AWS
    source resource: Bronze must read only Raw."""
    io_manager = CapturingIOManager()
    instance = dg.DagsterInstance.ephemeral()
    with freeze_time(AFTER_THE_DAYS_UNDER_TEST):
        result = dg.materialize(
            [cloudfront_requests],
            partition_key=partition_key,
            instance=instance,
            resources={"minio_s3": FakeS3Resource(FakeS3Client(objects=raw_objects)), "io_manager": io_manager},
        )
    assert result.success
    [materialization] = result.asset_materializations_for_node("aws_data__bronze__cloudfront_requests")
    return BronzeRun(
        table=io_manager.outputs["aws-data/bronze/cloudfront_requests"],
        report={name: value.value for name, value in materialization.metadata.items()},
        warnings=[
            entry.user_message for entry in instance.all_logs(result.run_id) if entry.level == logging.WARNING
        ],
    )


def bronze_table(raw_objects: dict[str, bytes], partition_key: str = "2026-11-13") -> pl.DataFrame:
    return run_bronze(raw_objects, partition_key).table


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


def test_a_full_delivery_line_maps_its_parenthesised_and_extra_fields():
    key, body = raw_log_file(ACCOUNT, DISTRIBUTION, "2026-11-13", "23", FULL_REQUEST)

    row = bronze_table({key: body}).row(0, named=True)

    assert row["cs_host"] == "d111111abcdef8.cloudfront.net"
    assert row["cs_user_agent"] == "Mozilla/5.0"
    assert row["cs_referer"] == "https://example.com/"
    assert row["cs_cookie"] is None
    assert row["x_edge_request_id"] == "req-1"
    assert row["ssl_protocol"] == "TLSv1.3"
    assert row["cs_protocol_version"] == "HTTP/2.0"
    assert row["sc_content_type"] == "text/html"
    assert row["c_port"] == 51234
    assert row["sc_content_len"] == 1024
    assert row["time_to_first_byte"] == 0.001


def test_a_narrow_delivery_line_maps_the_fields_only_it_has():
    key, body = raw_log_file(ACCOUNT, DISTRIBUTION, "2026-11-13", "09", NARROW_REQUEST)

    row = bronze_table({key: body}).row(0, named=True)

    assert row["c_country"] == "US"
    assert row["asn"] == "64500"
    assert row["cache_behavior_path_pattern"] == "*"
    assert row["x_edge_result_type"] == "Miss"
    assert row["cs_bytes"] == 190


def test_both_delivery_shapes_land_in_one_table_with_nulls_for_the_fields_a_delivery_lacks():
    narrow_key, narrow_body = raw_log_file("111111111111", "E1EXAMPLE0001", "2026-11-13", "09", NARROW_REQUEST)
    full_key, full_body = raw_log_file("222222222222", "E1EXAMPLE0002", "2026-11-13", "09", FULL_REQUEST)

    table = bronze_table({narrow_key: narrow_body, full_key: full_body})

    narrow, full = table.sort("account_id").rows(named=True)
    assert table.height == 2
    assert (narrow["c_country"], narrow["cs_user_agent"]) == ("US", None)
    assert (full["c_country"], full["cs_user_agent"]) == (None, "Mozilla/5.0")
    assert narrow["x_edge_location"] == "HEL51-P5" and full["x_edge_location"] == "SIN52-P1"


def test_a_field_with_no_column_is_kept_only_in_the_original_line():
    with_new_field = {**REQUEST, "brand-new-field": "surprise"}
    key, body = raw_log_file(ACCOUNT, DISTRIBUTION, "2026-11-13", "09", with_new_field)

    table = bronze_table({key: body})

    assert "brand_new_field" not in table.columns
    assert json.loads(table["raw_json"][0]) == with_new_field


INGESTED_AT = datetime(2026, 11, 14, 0, 5, tzinfo=timezone.utc)


def gz_lines(*records: dict) -> bytes:
    return gzip.compress("".join(json.dumps(record) + "\n" for record in records).encode())


def cleaned(*records: dict) -> pl.DataFrame:
    """Records -> a Log File's bytes -> the typed Bronze rows, through the two pure functions."""
    return clean_requests(
        parse_log_file(gz_lines(*records)), date(2026, 11, 13), ACCOUNT, DISTRIBUTION, "raw/some/key.gz", INGESTED_AT
    )


def test_parse_log_file_gives_a_row_per_line_with_the_delivered_field_names_and_the_original_line():
    other = {**REQUEST, "time": "09:14:00", "sc-status": "404"}

    table = parse_log_file(gz_lines(REQUEST, other))

    assert table.height == 2
    assert table["sc-status"].to_list() == ["200", "404"]
    assert [json.loads(line) for line in table["raw_json"]] == [REQUEST, other]


def test_clean_requests_maps_one_line_to_one_row():
    row = cleaned(REQUEST).row(0, named=True)

    populated = {column: value for column, value in row.items() if value is not None}
    assert populated == {
        "request_date": date(2026, 11, 13),
        "request_time": datetime(2026, 11, 13, 9, 13, 40, tzinfo=timezone.utc),
        "account_id": ACCOUNT,
        "distribution_id": DISTRIBUTION,
        "x_edge_location": "HEL51-P5",
        "c_ip": "203.0.113.4",
        "cs_method": "GET",
        "x_host_header": "www.example.com",
        "cs_uri_stem": "/",
        "sc_status": 200,
        "sc_bytes": 944,
        "time_taken": 0.537,
        "source_key": "raw/some/key.gz",
        "ingested_at": INGESTED_AT,
        "raw_json": json.dumps(REQUEST),
    }


def test_a_number_that_does_not_parse_becomes_null_and_the_rest_of_the_row_survives():
    row = cleaned({**REQUEST, "sc-status": "abc", "time-taken": "fast"}).row(0, named=True)

    assert row["sc_status"] is None
    assert row["time_taken"] is None
    assert row["sc_bytes"] == 944
    assert row["x_edge_location"] == "HEL51-P5"


def test_a_000_status_from_an_aborted_request_is_kept_as_zero_not_missing():
    assert cleaned({**REQUEST, "sc-status": "000"})["sc_status"].to_list() == [0]


def test_a_missing_or_malformed_date_or_time_gives_a_null_request_time():
    def request_time(record: dict):
        return cleaned(record)["request_time"].to_list()[0]

    assert request_time({"x-edge-location": "HEL51-P5"}) is None
    assert request_time({"date": "2026-11-13"}) is None
    assert request_time({"time": "09:13:40"}) is None
    assert request_time({"date": "2026-11-13", "time": "25:00:00"}) is None
    assert request_time({"date": "yesterday", "time": "09:13:40"}) is None
    assert request_time({"date": "-", "time": "-"}) is None


def test_every_row_from_either_shape_has_exactly_the_tables_columns_and_types():
    for record in (NARROW_REQUEST, FULL_REQUEST, {"a-field-with-no-column": "x"}):
        assert cleaned(record).schema == pl.Schema(CLOUDFRONT_SCHEMA)


def test_every_field_either_delivery_sends_has_a_column():
    sent = {_column_name(field) for record in (NARROW_REQUEST, FULL_REQUEST) for field in record}

    # date and time are combined into request_time
    assert sent - {"date", "time"} <= set(CLOUDFRONT_SCHEMA)


def test_parse_log_file_gives_an_empty_table_for_an_empty_file():
    table = parse_log_file(gzip.compress(b""))

    assert table.height == 0
    assert "raw_json" in table.columns


def test_the_run_reports_what_it_read():
    first_key, first_body = raw_log_file("111111111111", "E1EXAMPLE0001", "2026-11-13", "09", REQUEST, REQUEST)
    second_key, second_body = raw_log_file("222222222222", "E1EXAMPLE0002", "2026-11-13", "10", FULL_REQUEST)

    report = run_bronze({first_key: first_body, second_key: second_body}).report

    assert report["files_read"] == 2
    assert report["files_skipped"] == 0
    assert report["requests_read"] == 3
    assert report["accounts"] == ["111111111111", "222222222222"]
    assert report["distributions"] == ["E1EXAMPLE0001", "E1EXAMPLE0002"]


def test_an_empty_log_file_counts_as_read_with_no_requests_not_as_skipped():
    empty_key, empty_body = raw_log_file(ACCOUNT, DISTRIBUTION, "2026-11-13", "08")
    real_key, real_body = raw_log_file(ACCOUNT, DISTRIBUTION, "2026-11-13", "09", REQUEST)

    run = run_bronze({empty_key: empty_body, real_key: real_body})

    assert run.report["files_read"] == 2
    assert run.report["files_skipped"] == 0
    assert run.report["requests_read"] == 1
    assert run.table.height == 1


def test_parse_log_file_raises_oserror_for_a_corrupt_gzip():
    with pytest.raises(OSError):
        parse_log_file(b"definitely not gzip")


def test_parse_log_file_raises_valueerror_for_invalid_utf8():
    with pytest.raises(ValueError):
        parse_log_file(gzip.compress(b'{"c-ip": "\xff\xfe"}\n'))


def test_parse_log_file_raises_valueerror_for_invalid_json():
    body = gzip.compress((json.dumps(REQUEST) + "\n{this is not json\n").encode())

    with pytest.raises(ValueError):
        parse_log_file(body)


@pytest.mark.parametrize(
    "lines",
    [
        ["[1, 2, 3]"],  # nothing but a list
        [json.dumps(REQUEST), "[1, 2, 3]"],  # a list among objects
        [json.dumps(REQUEST), '"just a string"'],
        [json.dumps(REQUEST), "42"],
    ],
)
def test_parse_log_file_raises_valueerror_for_a_line_that_is_not_a_json_object(lines):
    body = gzip.compress(("\n".join(lines) + "\n").encode())

    with pytest.raises(ValueError):
        parse_log_file(body)


def test_an_unreadable_log_file_is_skipped_with_a_warning_and_the_rest_of_the_day_is_still_read():
    good_early, good_early_body = raw_log_file(ACCOUNT, DISTRIBUTION, "2026-11-13", "09", REQUEST)
    corrupt, _ = raw_log_file(ACCOUNT, DISTRIBUTION, "2026-11-13", "10", REQUEST)
    bad_json, _ = raw_log_file(ACCOUNT, DISTRIBUTION, "2026-11-13", "11", REQUEST)
    good_late, good_late_body = raw_log_file(ACCOUNT, DISTRIBUTION, "2026-11-13", "12", REQUEST)

    run = run_bronze(
        {
            good_early: good_early_body,
            corrupt: b"definitely not gzip",
            bad_json: gzip.compress(b"{this is not json\n"),
            good_late: good_late_body,
        }
    )

    assert run.table["source_key"].to_list() == [good_early, good_late]
    assert run.report["files_read"] == 2
    assert run.report["files_skipped"] == 2
    assert run.report["requests_read"] == 2
    assert len(run.warnings) == 2
    assert any(corrupt in warning for warning in run.warnings)
    assert any(bad_json in warning for warning in run.warnings)
