import json
from datetime import datetime, timezone

import polars as pl
import pytest

from aws_data.lib.github_bronze import TYPE_COLUMNS, bronze_schema, parse_raw_file
from tests.github_envelopes import envelope, one_line

SOURCE_KEY = "github/raw/check_run/2026/09/08/webhooks-2-2026-09-08-17-17-42-abc"

COMMON_COLUMNS = [
    ("id", pl.Utf8),
    ("time", pl.Datetime("us", "UTC")),
    ("account", pl.Utf8),
    ("region", pl.Utf8),
    ("detail_type", pl.Utf8),
    ("source_file", pl.Utf8),
    ("action", pl.Utf8),
    ("repository_id", pl.Int64),
    ("repository_name", pl.Utf8),
    ("organization_id", pl.Int64),
    ("organization_name", pl.Utf8),
    ("sender_id", pl.Int64),
    ("sender_name", pl.Utf8),
]


def test_check_run_schema_is_the_common_columns_then_its_typed_columns_then_payload():
    schema = bronze_schema("check_run")

    assert list(schema.items()) == [
        *COMMON_COLUMNS,
        ("check_run_id", pl.Int64),
        ("check_run_name", pl.Utf8),
        ("check_run_head_sha", pl.Utf8),
        ("check_run_status", pl.Utf8),
        ("check_run_conclusion", pl.Utf8),
        ("check_run_started_at", pl.Datetime("us", "UTC")),
        ("check_run_completed_at", pl.Datetime("us", "UTC")),
        ("payload", pl.Utf8),
    ]


def test_a_type_without_a_spec_keeps_the_common_schema_with_payload_last():
    assert list(bronze_schema("an_unlisted_event_type").items()) == [
        *COMMON_COLUMNS,
        ("payload", pl.Utf8),
    ]


def test_a_one_line_file_becomes_one_row_with_the_envelope_and_payload_columns():
    df = parse_raw_file(one_line(envelope()), SOURCE_KEY, "check_run")

    assert df.height == 1
    row = df.row(0, named=True)
    assert row["id"] == "60201608-a89e-7b82-3f96-90a129cbf3b0"
    assert row["time"] == datetime(2026, 9, 8, 17, 17, 42, tzinfo=timezone.utc)
    assert row["account"] == "307985306317"
    assert row["region"] == "ap-southeast-1"
    assert row["detail_type"] == "check_run"
    assert row["source_file"] == SOURCE_KEY
    assert row["action"] == "created"
    assert row["repository_id"] == 1234
    assert row["repository_name"] == "snacker-tracker/reporter-serverless"
    assert row["organization_id"] == 5678
    assert row["organization_name"] == "snacker-tracker"
    assert row["sender_id"] == 91011
    assert row["sender_name"] == "octocat"
    assert json.loads(row["payload"])["check_run"]["name"] == "deploy-prod"


def test_a_check_run_delivery_fills_its_typed_columns_from_the_payload():
    check_run = {
        "id": 93092064248,
        "name": "Analyze (python)",
        "head_sha": "0b43ae6c2f1d9e8a7b5c4d3e2f1a0b9c8d7e6f5a",
        "status": "completed",
        "conclusion": "success",
        "started_at": "2026-08-08T10:31:39Z",
        "completed_at": "2026-08-08T10:32:16Z",
    }

    df = parse_raw_file(
        one_line(envelope(check_run=check_run)), SOURCE_KEY, "check_run"
    )

    row = df.row(0, named=True)
    assert row["check_run_id"] == 93092064248
    assert row["check_run_name"] == "Analyze (python)"
    assert row["check_run_head_sha"] == "0b43ae6c2f1d9e8a7b5c4d3e2f1a0b9c8d7e6f5a"
    assert row["check_run_status"] == "completed"
    assert row["check_run_conclusion"] == "success"
    assert row["check_run_started_at"] == datetime(
        2026, 8, 8, 10, 31, 39, tzinfo=timezone.utc
    )
    assert row["check_run_completed_at"] == datetime(
        2026, 8, 8, 10, 32, 16, tzinfo=timezone.utc
    )


def test_a_check_run_field_missing_from_a_delivery_reads_null_and_is_kept():
    created = {
        "id": 93092064248,
        "name": "Analyze (python)",
        "head_sha": "0b43ae6c2f1d9e8a7b5c4d3e2f1a0b9c8d7e6f5a",
        "status": "queued",
        "started_at": "2026-08-08T10:31:39Z",
    }

    df = parse_raw_file(
        one_line(envelope(action="created", check_run=created)), SOURCE_KEY, "check_run"
    )

    row = df.row(0, named=True)
    assert row["check_run_status"] == "queued"
    assert row["check_run_conclusion"] is None
    assert row["check_run_completed_at"] is None
    assert row["check_run_started_at"] == datetime(
        2026, 8, 8, 10, 31, 39, tzinfo=timezone.utc
    )


def test_check_run_columns_follow_its_schema_and_the_payload_round_trips():
    delivery = envelope()

    df = parse_raw_file(one_line(delivery), SOURCE_KEY, "check_run")

    assert df.schema == bronze_schema("check_run")
    assert json.loads(df.row(0, named=True)["payload"]) == delivery["detail"]["payload"]


def test_a_malformed_timestamp_reads_null_and_keeps_the_row():
    check_run = {
        "id": 93092064248,
        "name": "Analyze (python)",
        "head_sha": "0b43ae6c2f1d9e8a7b5c4d3e2f1a0b9c8d7e6f5a",
        "status": "queued",
        "started_at": "",
    }

    df = parse_raw_file(
        one_line(envelope(id="bad-time", check_run=check_run)), SOURCE_KEY, "check_run"
    )

    assert df["id"].to_list() == ["bad-time"]
    assert df["check_run_started_at"].to_list() == [None]


def test_a_spec_entry_can_be_a_value_derived_from_the_payload(monkeypatch):
    monkeypatch.setitem(
        TYPE_COLUMNS,
        "push",
        {"push_commit_count": (lambda payload: len(payload["commits"]), pl.Int64)},
    )
    delivery = envelope(id="push-1", detail_type="push", action=None)
    delivery["detail"]["payload"]["commits"] = [{"id": "a"}, {"id": "b"}, {"id": "c"}]

    df = parse_raw_file(one_line(delivery), SOURCE_KEY, "push")

    assert df["push_commit_count"].to_list() == [3]
    assert df.schema["push_commit_count"] == pl.Int64


@pytest.mark.parametrize(
    ("started_at", "expected"),
    [
        ("2026-08-08T10:31:39Z", datetime(2026, 8, 8, 10, 31, 39, tzinfo=timezone.utc)),
        (
            "2026-08-08T10:31:39.123Z",
            datetime(2026, 8, 8, 10, 31, 39, 123000, tzinfo=timezone.utc),
        ),
        (
            "2026-08-08T12:31:39+02:00",
            datetime(2026, 8, 8, 10, 31, 39, tzinfo=timezone.utc),
        ),
    ],
)
def test_timestamp_forms_become_utc_datetimes(started_at, expected):
    check_run = {"id": 93092064248, "started_at": started_at}

    df = parse_raw_file(
        one_line(envelope(check_run=check_run)), SOURCE_KEY, "check_run"
    )

    assert df["check_run_started_at"].to_list() == [expected]


def test_a_delivery_with_no_check_run_key_reads_every_check_run_column_null():
    delivery = envelope(detail_type="check_run")
    del delivery["detail"]["payload"]["check_run"]

    df = parse_raw_file(one_line(delivery), SOURCE_KEY, "check_run")

    assert df.height == 1
    row = df.row(0, named=True)
    for column in [name for name in df.columns if name.startswith("check_run_")]:
        assert row[column] is None, column


def test_the_payload_column_keeps_everything_type_specific():
    df = parse_raw_file(one_line(envelope()), SOURCE_KEY, "check_run")

    payload = json.loads(df.row(0, named=True)["payload"])
    assert payload["check_run"]["status"] == "queued"
    assert payload["check_run"]["conclusion"] is None


def test_a_multi_line_file_becomes_one_row_per_envelope_in_file_order():
    first = envelope(id="first", time="2026-09-08T17:17:42Z", action="created")
    second = envelope(id="second", time="2026-09-08T17:18:00Z", action="completed")
    body = one_line(first) + one_line(second)

    df = parse_raw_file(body, SOURCE_KEY, "check_run")

    assert df["id"].to_list() == ["first", "second"]
    assert df["action"].to_list() == ["created", "completed"]
    assert df["source_file"].to_list() == [SOURCE_KEY, SOURCE_KEY]


def test_an_event_with_no_action_has_a_null_action_and_is_kept():
    body = one_line(envelope(id="push-1", detail_type="push", action=None))

    df = parse_raw_file(body, SOURCE_KEY, "push")

    assert df.height == 1
    assert df["detail_type"].to_list() == ["push"]
    assert df["action"].to_list() == [None]


def test_envelopes_sharing_an_id_are_both_kept_as_separate_rows():
    same_id = "duplicate-delivery"
    body = one_line(envelope(id=same_id, action="created")) + one_line(
        envelope(id=same_id, action="created")
    )

    df = parse_raw_file(body, SOURCE_KEY, "check_run")

    assert df.height == 2
    assert df["id"].to_list() == [same_id, same_id]
