import json
from datetime import date, datetime, timezone

import polars as pl

from aws_data.defs.cloudtrail.cloudtrail_events import (
    CLOUDTRAIL_SCHEMA,
    _parse_event_time,
    _row_from_record,
)

INGESTED_AT = datetime(2026, 11, 11, 0, 5, tzinfo=timezone.utc)
DELIVERY_DAY = date(2026, 11, 10)

FULL_RECORD = {
    "eventVersion": "1.09",
    "userIdentity": {
        "type": "IAMUser",
        "principalId": "AIDAEXAMPLE",
        "arn": "arn:aws:iam::111111111111:user/alice",
        "accountId": "999999999999",
        "accessKeyId": "AKIAEXAMPLE",
        "userName": "alice",
        "invokedBy": "cloudformation.amazonaws.com",
    },
    "eventTime": "2026-11-10T12:34:56Z",
    "eventSource": "s3.amazonaws.com",
    "eventName": "GetObject",
    "awsRegion": "us-east-1",
    "sourceIPAddress": "203.0.113.7",
    "userAgent": "aws-cli/2",
    "requestID": "req-1",
    "eventID": "evt-1",
    "readOnly": True,
    "eventType": "AwsApiCall",
    "managementEvent": True,
    "recipientAccountId": "111111111111",
    "eventCategory": "Management",
    "errorCode": "AccessDenied",
    "errorMessage": "nope",
    "requestParameters": {"bucketName": "x"},
}


def _row(record: dict) -> dict:
    return _row_from_record(record, DELIVERY_DAY, "o-abc123", "AWSLogs/some/key.json.gz", INGESTED_AT)


class TestParseEventTime:
    def test_valid_timestamp_is_utc(self):
        assert _parse_event_time("2026-11-10T12:34:56Z") == datetime(2026, 11, 10, 12, 34, 56, tzinfo=timezone.utc)

    def test_missing_values_are_none(self):
        assert _parse_event_time(None) is None
        assert _parse_event_time("") is None

    def test_malformed_values_are_none(self):
        assert _parse_event_time("not a date") is None
        assert _parse_event_time("2026-11-10T12:34:56.123Z") is None
        assert _parse_event_time("2026-11-10 12:34:56") is None


class TestRowFromRecord:
    def test_maps_record_fields(self):
        row = _row(FULL_RECORD)
        assert row["event_id"] == "evt-1"
        assert row["event_name"] == "GetObject"
        assert row["event_source"] == "s3.amazonaws.com"
        assert row["recipient_account_id"] == "111111111111"
        assert row["read_only"] is True
        assert row["error_code"] == "AccessDenied"

    def test_maps_user_identity_with_caller_account_distinct_from_account(self):
        row = _row(FULL_RECORD)
        assert row["user_identity_account_id"] == "999999999999"
        assert row["recipient_account_id"] == "111111111111"
        assert row["user_identity_user_name"] == "alice"
        assert row["user_identity_invoked_by"] == "cloudformation.amazonaws.com"

    def test_carries_lineage_arguments(self):
        row = _row(FULL_RECORD)
        assert row["event_date"] == DELIVERY_DAY
        assert row["organization_id"] == "o-abc123"
        assert row["source_key"] == "AWSLogs/some/key.json.gz"
        assert row["ingested_at"] == INGESTED_AT

    def test_event_date_is_delivery_day_not_event_time_day(self):
        late_delivered = {**FULL_RECORD, "eventTime": "2026-11-09T23:55:16Z"}
        row = _row(late_delivered)
        assert row["event_date"] == DELIVERY_DAY
        assert row["event_time"].date() == date(2026, 11, 9)

    def test_raw_json_round_trips_the_original_record(self):
        assert json.loads(_row(FULL_RECORD)["raw_json"]) == FULL_RECORD

    def test_sparse_record_yields_nones_without_raising(self):
        row = _row({})
        assert row["event_id"] is None
        assert row["event_time"] is None
        assert row["user_identity_arn"] is None

    def test_null_user_identity_is_tolerated(self):
        assert _row({"userIdentity": None})["user_identity_type"] is None

    def test_keys_match_schema_exactly(self):
        assert list(_row(FULL_RECORD)) == list(CLOUDTRAIL_SCHEMA)


class TestDataFrame:
    def test_rows_build_a_typed_frame(self):
        df = pl.DataFrame([_row(FULL_RECORD), _row({})], schema=CLOUDTRAIL_SCHEMA)
        assert df.schema == pl.Schema(CLOUDTRAIL_SCHEMA)
        assert df.height == 2

    def test_no_rows_still_yields_a_typed_empty_frame(self):
        df = pl.DataFrame([], schema=CLOUDTRAIL_SCHEMA)
        assert df.height == 0
        assert df.schema == pl.Schema(CLOUDTRAIL_SCHEMA)
