import json
from datetime import UTC, datetime

import polars as pl

from aws_data.lib.github_bronze import bronze_schema, parse_raw_file
from tests.github_envelopes import one_line

SOURCE_KEY = "github/raw/repository/2026/11/15/webhooks-2-2026-11-15-09-00-00-abc"


def repository_envelope(action: str) -> dict:
    """A repository delivery as EventBridge sends it: the payload carries only
    the action, the repository, the organization and the sender."""
    return {
        "version": "0",
        "id": "3f1c2d4e-5a6b-4c7d-8e9f-0a1b2c3d4e5f",
        "detail-type": "repository",
        "source": "github",
        "account": "307985306317",
        "time": "2026-11-15T09:00:00Z",
        "region": "ap-southeast-1",
        "resources": [],
        "detail": {
            "client_ip": "140.82.115.245",
            "request_id": "9b8a7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d",
            "payload": {
                "action": action,
                "repository": {
                    "id": 1234,
                    "full_name": "snacker-tracker/reporter-serverless",
                },
                "organization": {"id": 5678, "login": "snacker-tracker"},
                "sender": {"id": 91011, "login": "octocat"},
            },
        },
    }


def test_a_repository_schema_is_the_common_columns_then_payload():
    schema = bronze_schema("repository")

    assert schema == {
        "id": pl.Utf8,
        "time": pl.Datetime("us", "UTC"),
        "account": pl.Utf8,
        "region": pl.Utf8,
        "detail_type": pl.Utf8,
        "source_file": pl.Utf8,
        "action": pl.Utf8,
        "repository_id": pl.Int64,
        "repository_name": pl.Utf8,
        "organization_id": pl.Int64,
        "organization_name": pl.Utf8,
        "sender_id": pl.Int64,
        "sender_name": pl.Utf8,
        "payload": pl.Utf8,
    }
    assert list(schema) == [
        "id",
        "time",
        "account",
        "region",
        "detail_type",
        "source_file",
        "action",
        "repository_id",
        "repository_name",
        "organization_id",
        "organization_name",
        "sender_id",
        "sender_name",
        "payload",
    ]


def test_an_archived_repository_delivery_becomes_one_row_with_the_repository_name():
    df = parse_raw_file(
        one_line(repository_envelope("archived")), SOURCE_KEY, "repository"
    )

    assert df.height == 1
    row = df.row(0, named=True)
    assert row["detail_type"] == "repository"
    assert row["action"] == "archived"
    assert row["time"] == datetime(2026, 11, 15, 9, 0, 0, tzinfo=UTC)
    assert row["repository_id"] == 1234
    assert row["repository_name"] == "snacker-tracker/reporter-serverless"
    assert row["organization_name"] == "snacker-tracker"
    assert row["sender_name"] == "octocat"
    assert json.loads(row["payload"])["action"] == "archived"
