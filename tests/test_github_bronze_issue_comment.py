import json
from datetime import UTC, datetime

import polars as pl

from aws_data.lib.github_bronze import bronze_schema, parse_raw_file
from tests.github_envelopes import envelope, one_line

SOURCE_KEY = "github/raw/issue_comment/2026/11/15/webhooks-2-2026-11-15-10-08-10-abc"


def issue_comment_delivery(*, id: str, payload: dict) -> bytes:
    delivery = envelope(id=id, time="2026-11-15T10:08:09Z")
    delivery["detail-type"] = "issue_comment"
    delivery["detail"]["payload"] = payload
    return one_line(delivery)


def issue_comment_payload(*, comment: dict, issue_number: int = 148) -> dict:
    return {
        "action": "created",
        "issue": {"number": issue_number, "id": 6400985295},
        "comment": comment,
        "repository": {"id": 1428184726, "full_name": "snacker-tracker/interface"},
        "organization": {"id": 59108816, "login": "snacker-tracker"},
        "sender": {"id": 355637, "login": "leprechaun"},
    }


def test_an_issue_comment_schema_is_the_common_columns_then_its_typed_columns_then_payload():
    assert list(bronze_schema("issue_comment").items()) == [
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
        ("comment_id", pl.Int64),
        ("comment_issue_number", pl.Int64),
        ("comment_author_association", pl.Utf8),
        ("comment_created_at", pl.Datetime("us", "UTC")),
        ("payload", pl.Utf8),
    ]


def test_an_issue_comment_delivery_fills_its_typed_columns_from_the_payload():
    body = issue_comment_delivery(
        id="fc96ad00-88b1-8d2b-efaa-4b0515d94196",
        payload=issue_comment_payload(
            comment={
                "id": 6892767430,
                "author_association": "MEMBER",
                "created_at": "2026-11-15T10:08:07Z",
            },
            issue_number=148,
        ),
    )

    df = parse_raw_file(body, SOURCE_KEY, "issue_comment")

    row = df.row(0, named=True)
    assert row["comment_id"] == 6892767430
    assert row["comment_issue_number"] == 148
    assert row["comment_author_association"] == "MEMBER"
    assert row["comment_created_at"] == datetime(
        2026, 11, 15, 10, 8, 7, tzinfo=UTC
    )


def test_an_issue_comment_field_missing_from_a_delivery_reads_null_and_is_kept():
    body = issue_comment_delivery(
        id="no-association",
        payload=issue_comment_payload(
            comment={"id": 6892767430, "created_at": "2026-11-15T10:08:07Z"},
        ),
    )

    df = parse_raw_file(body, SOURCE_KEY, "issue_comment")

    assert df["id"].to_list() == ["no-association"]
    row = df.row(0, named=True)
    assert row["comment_id"] == 6892767430
    assert row["comment_author_association"] is None


def test_a_malformed_issue_comment_timestamp_reads_null_and_keeps_the_row():
    body = issue_comment_delivery(
        id="bad-time",
        payload=issue_comment_payload(
            comment={"id": 6892767430, "author_association": "NONE", "created_at": ""},
        ),
    )

    df = parse_raw_file(body, SOURCE_KEY, "issue_comment")

    assert df["id"].to_list() == ["bad-time"]
    assert df["comment_created_at"].to_list() == [None]
    assert df["comment_author_association"].to_list() == ["NONE"]


def test_issue_comment_columns_follow_its_schema_and_the_payload_round_trips():
    payload = issue_comment_payload(
        comment={
            "id": 6892767430,
            "author_association": "MEMBER",
            "created_at": "2026-11-15T10:08:07Z",
        },
    )

    df = parse_raw_file(
        issue_comment_delivery(id="round-trip", payload=payload),
        SOURCE_KEY,
        "issue_comment",
    )

    assert df.schema == bronze_schema("issue_comment")
    assert json.loads(df.row(0, named=True)["payload"]) == payload
