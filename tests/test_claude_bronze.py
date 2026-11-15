import json
from datetime import datetime, timezone

from aws_data.lib.claude_bronze import bronze_schema, parse_raw_file, raw_prefix
from tests.claude_envelopes import envelope, one_line

SOURCE_KEY = (
    "claude/raw/session-status/2026/11/13/statusline-1-2026-11-13-10-32-19-6738f2f0"
)


def test_a_one_line_file_becomes_one_row_with_the_envelope_and_detail_columns():
    df = parse_raw_file(one_line(envelope()), SOURCE_KEY)

    assert df.height == 1
    row = df.row(0, named=True)
    assert row["id"] == "3f1c2b9e-7d4a-4e8b-9a61-0c2d5e7f8a90"
    assert row["time"] == datetime(2026, 11, 13, 10, 32, 19, tzinfo=timezone.utc)
    assert row["account"] == "123456789012"
    assert row["region"] == "ap-southeast-1"
    assert row["detail_type"] == "session-status"
    assert row["source_file"] == SOURCE_KEY
    assert row["client_ip"] == "203.0.113.7"
    assert row["request_id"] == "9b8a7c6d-5e4f-4321-8765-1a2b3c4d5e6f"
    assert row["machine"] == "laptop"
    assert json.loads(row["payload"])["session_id"] == "session-0001"


def test_the_payload_round_trips_to_the_delivered_payload():
    delivery = envelope()

    df = parse_raw_file(one_line(delivery), SOURCE_KEY)

    assert json.loads(df.row(0, named=True)["payload"]) == delivery["detail"]["payload"]


def test_a_multi_line_file_gives_one_row_per_envelope_and_ignores_blank_lines():
    body = (
        one_line(envelope(id="first"))
        + b"\n   \n"
        + one_line(envelope(id="second", time="2026-11-13T11:00:00Z"))
    )

    df = parse_raw_file(body, SOURCE_KEY)

    assert df["id"].to_list() == ["first", "second"]


def test_a_test_delivery_with_the_minimal_payload_is_kept_like_any_other_row():
    test_delivery = envelope(
        id="test-delivery",
        machine=None,
        payload={"session_id": "statusline-test"},
    )

    df = parse_raw_file(one_line(test_delivery), SOURCE_KEY)

    row = df.row(0, named=True)
    assert row["id"] == "test-delivery"
    assert row["machine"] is None
    assert json.loads(row["payload"]) == {"session_id": "statusline-test"}


def test_the_columns_follow_the_declared_schema_in_order():
    df = parse_raw_file(one_line(envelope()), SOURCE_KEY)

    assert list(df.schema.items()) == list(bronze_schema().items())
    assert list(bronze_schema()) == [
        "id",
        "time",
        "account",
        "region",
        "detail_type",
        "source_file",
        "client_ip",
        "request_id",
        "machine",
        "payload",
    ]


def test_the_raw_prefix_is_the_session_status_folder_in_claude_raw():
    assert raw_prefix() == "claude/raw/session-status/"
