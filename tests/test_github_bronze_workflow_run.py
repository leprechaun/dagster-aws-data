import json
from datetime import datetime, timezone

import polars as pl

from aws_data.lib.github_bronze import bronze_schema, parse_raw_file
from tests.github_envelopes import envelope, one_line

SOURCE_KEY = "github/raw/workflow_run/2026/08/08/webhooks-2-2026-08-08-10-32-19-abc"


def workflow_run_delivery(workflow_run: dict, *, id: str = "wr-delivery-1"):
    delivery = envelope(
        id=id,
        time="2026-08-08T10:32:19Z",
        detail_type="workflow_run",
        action="completed",
    )
    delivery["detail"]["payload"]["workflow_run"] = workflow_run
    return delivery


def test_workflow_run_schema_is_the_common_columns_then_its_typed_columns_then_payload():
    schema = bronze_schema("workflow_run")

    assert list(schema.items()) == [
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
        ("workflow_run_id", pl.Int64),
        ("workflow_run_name", pl.Utf8),
        ("workflow_run_workflow_id", pl.Int64),
        ("workflow_run_check_suite_id", pl.Int64),
        ("workflow_run_run_number", pl.Int64),
        ("workflow_run_run_attempt", pl.Int64),
        ("workflow_run_event", pl.Utf8),
        ("workflow_run_head_branch", pl.Utf8),
        ("workflow_run_head_sha", pl.Utf8),
        ("workflow_run_status", pl.Utf8),
        ("workflow_run_conclusion", pl.Utf8),
        ("workflow_run_run_started_at", pl.Datetime("us", "UTC")),
        ("workflow_run_updated_at", pl.Datetime("us", "UTC")),
        ("payload", pl.Utf8),
    ]


def test_a_workflow_run_delivery_fills_its_typed_columns_from_the_payload():
    workflow_run = {
        "id": 31252968611,
        "name": "Code Quality: Scheduled",
        "head_branch": "master",
        "head_sha": "0b43ae61866d4b9a7e74a02fbee4e8099f0b5c04",
        "run_number": 60,
        "run_attempt": 2,
        "event": "dynamic",
        "status": "completed",
        "conclusion": "success",
        "workflow_id": 95071896,
        "check_suite_id": 84802647620,
        "run_started_at": "2026-08-08T10:31:35Z",
        "updated_at": "2026-08-08T10:32:18Z",
    }

    df = parse_raw_file(
        one_line(workflow_run_delivery(workflow_run)), SOURCE_KEY, "workflow_run"
    )

    row = df.row(0, named=True)
    assert row["workflow_run_id"] == 31252968611
    assert row["workflow_run_name"] == "Code Quality: Scheduled"
    assert row["workflow_run_workflow_id"] == 95071896
    assert row["workflow_run_check_suite_id"] == 84802647620
    assert row["workflow_run_run_number"] == 60
    assert row["workflow_run_run_attempt"] == 2
    assert row["workflow_run_event"] == "dynamic"
    assert row["workflow_run_head_branch"] == "master"
    assert row["workflow_run_head_sha"] == "0b43ae61866d4b9a7e74a02fbee4e8099f0b5c04"
    assert row["workflow_run_status"] == "completed"
    assert row["workflow_run_conclusion"] == "success"
    assert row["workflow_run_run_started_at"] == datetime(
        2026, 8, 8, 10, 31, 35, tzinfo=timezone.utc
    )
    assert row["workflow_run_updated_at"] == datetime(
        2026, 8, 8, 10, 32, 18, tzinfo=timezone.utc
    )


def test_an_in_progress_workflow_run_reads_a_null_conclusion_and_is_kept():
    workflow_run = {
        "id": 31252968999,
        "name": "CI",
        "head_branch": "feature/bronze",
        "head_sha": "9f8e7d6c5b4a39281706f5e4d3c2b1a0998877665",
        "run_number": 412,
        "run_attempt": 1,
        "event": "push",
        "status": "in_progress",
        "conclusion": None,
        "workflow_id": 95071000,
        "check_suite_id": 84802650000,
        "updated_at": "2026-08-08T10:33:00Z",
    }

    df = parse_raw_file(
        one_line(workflow_run_delivery(workflow_run, id="wr-in-progress")),
        SOURCE_KEY,
        "workflow_run",
    )

    assert df["id"].to_list() == ["wr-in-progress"]
    row = df.row(0, named=True)
    assert row["workflow_run_status"] == "in_progress"
    assert row["workflow_run_conclusion"] is None
    assert row["workflow_run_run_started_at"] is None
    assert row["workflow_run_updated_at"] == datetime(
        2026, 8, 8, 10, 33, 0, tzinfo=timezone.utc
    )


def test_a_malformed_workflow_run_timestamp_reads_null_and_keeps_the_row():
    workflow_run = {
        "id": 31252968777,
        "name": "CI",
        "status": "queued",
        "run_started_at": "not-a-time",
        "updated_at": "2026-08-08T10:33:00Z",
    }

    df = parse_raw_file(
        one_line(workflow_run_delivery(workflow_run, id="wr-bad-time")),
        SOURCE_KEY,
        "workflow_run",
    )

    assert df["id"].to_list() == ["wr-bad-time"]
    assert df["workflow_run_run_started_at"].to_list() == [None]
    assert df["workflow_run_updated_at"].to_list() == [
        datetime(2026, 8, 8, 10, 33, 0, tzinfo=timezone.utc)
    ]


def test_workflow_run_columns_follow_its_schema_and_the_payload_round_trips():
    delivery = workflow_run_delivery({"id": 31252968611, "status": "completed"})

    df = parse_raw_file(one_line(delivery), SOURCE_KEY, "workflow_run")

    assert df.schema == bronze_schema("workflow_run")
    assert json.loads(df.row(0, named=True)["payload"]) == delivery["detail"]["payload"]
