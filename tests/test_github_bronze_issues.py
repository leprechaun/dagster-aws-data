from datetime import datetime, timezone

import polars as pl

from aws_data.lib.github_bronze import bronze_schema, parse_raw_file
from tests.github_envelopes import envelope, one_line

SOURCE_KEY = "github/raw/issues/2026/09/10/webhooks-2-2026-09-10-23-11-06-abc"


def issues_delivery(*, action: str, issue: dict, label: dict | None = None) -> dict:
    delivery = envelope(
        id="issues-delivery-1",
        time="2026-09-10T23:11:06Z",
        detail_type="issues",
        action=action,
    )
    payload = delivery["detail"]["payload"]
    del payload["check_run"]
    payload["issue"] = issue
    if label is not None:
        payload["label"] = label
    return delivery


def test_issues_schema_is_the_common_columns_then_its_typed_columns_then_payload():
    assert list(bronze_schema("issues").items()) == [
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
        ("issue_id", pl.Int64),
        ("issue_number", pl.Int64),
        ("issue_title", pl.Utf8),
        ("issue_state", pl.Utf8),
        ("issue_state_reason", pl.Utf8),
        ("issue_created_at", pl.Datetime("us", "UTC")),
        ("issue_closed_at", pl.Datetime("us", "UTC")),
        ("issue_label_name", pl.Utf8),
        ("payload", pl.Utf8),
    ]


def test_a_closed_issue_delivery_fills_its_issue_columns_from_the_payload():
    issue = {
        "id": 4776617947,
        "number": 4,
        "title": "Dependency Dashboard",
        "state": "closed",
        "state_reason": "completed",
        "created_at": "2026-08-01T09:00:00Z",
        "closed_at": "2026-09-10T23:11:06Z",
    }

    df = parse_raw_file(
        one_line(issues_delivery(action="closed", issue=issue)), SOURCE_KEY, "issues"
    )

    row = df.row(0, named=True)
    assert row["detail_type"] == "issues"
    assert row["action"] == "closed"
    assert row["issue_id"] == 4776617947
    assert row["issue_number"] == 4
    assert row["issue_title"] == "Dependency Dashboard"
    assert row["issue_state"] == "closed"
    assert row["issue_state_reason"] == "completed"
    assert row["issue_created_at"] == datetime(2026, 8, 1, 9, 0, 0, tzinfo=timezone.utc)
    assert row["issue_closed_at"] == datetime(
        2026, 9, 10, 23, 11, 6, tzinfo=timezone.utc
    )
    assert row["issue_label_name"] is None


def test_a_labeled_issue_delivery_fills_the_label_name_from_the_top_level_label():
    issue = {
        "id": 4776617947,
        "number": 4,
        "title": "Dependency Dashboard",
        "state": "open",
        "state_reason": None,
        "created_at": "2026-08-01T09:00:00Z",
        "closed_at": None,
    }

    df = parse_raw_file(
        one_line(
            issues_delivery(
                action="labeled", issue=issue, label={"name": "dependencies"}
            )
        ),
        SOURCE_KEY,
        "issues",
    )

    row = df.row(0, named=True)
    assert row["action"] == "labeled"
    assert row["issue_state"] == "open"
    assert row["issue_label_name"] == "dependencies"


def test_an_issue_delivery_missing_optional_fields_reads_null_and_is_kept():
    issue = {
        "id": 4776617948,
        "number": 5,
        "title": "Crash on startup",
        "state": "open",
        "created_at": "2026-08-02T10:00:00Z",
    }

    df = parse_raw_file(
        one_line(issues_delivery(action="opened", issue=issue)),
        SOURCE_KEY,
        "issues",
    )

    assert df.height == 1
    row = df.row(0, named=True)
    assert row["issue_id"] == 4776617948
    assert row["issue_number"] == 5
    assert row["issue_title"] == "Crash on startup"
    assert row["issue_state"] == "open"
    assert row["issue_state_reason"] is None
    assert row["issue_closed_at"] is None
    assert row["issue_label_name"] is None
