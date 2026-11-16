import json
from datetime import UTC, datetime

import polars as pl

from aws_data.lib.github_bronze import bronze_schema, parse_raw_file
from tests.github_envelopes import envelope, one_line

SOURCE_KEY = "github/raw/deployment/2026/08/23/webhooks-2-2026-08-23-14-44-11-abc"


def a_deployment_delivery(
    *, id: str = "363c6dae-88f1-1d70-631a-7abaec9adbc1", deployment: dict
):
    delivery = envelope(
        id=id,
        time="2026-08-23T14:44:11Z",
        detail_type="deployment",
        action="created",
    )
    payload = delivery["detail"]["payload"]
    del payload["check_run"]
    payload["deployment"] = deployment
    return delivery


def test_deployment_schema_is_the_common_columns_then_its_typed_columns_then_payload():
    assert list(bronze_schema("deployment").items()) == [
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
        ("deployment_id", pl.Int64),
        ("deployment_environment", pl.Utf8),
        ("deployment_task", pl.Utf8),
        ("deployment_ref", pl.Utf8),
        ("deployment_sha", pl.Utf8),
        ("deployment_created_at", pl.Datetime("us", "UTC")),
        ("payload", pl.Utf8),
    ]


def test_a_deployment_delivery_fills_its_typed_columns_from_the_payload():
    delivery = a_deployment_delivery(
        deployment={
            "id": 6049179907,
            "task": "deploy",
            "environment": "dev",
            "ref": "master",
            "sha": "a6571f15c97fb9f35d019f02f13722d4bac9889b",
            "created_at": "2026-08-23T14:44:10Z",
        }
    )

    df = parse_raw_file(one_line(delivery), SOURCE_KEY, "deployment")

    assert df.height == 1
    row = df.row(0, named=True)
    assert row["deployment_id"] == 6049179907
    assert row["deployment_environment"] == "dev"
    assert row["deployment_task"] == "deploy"
    assert row["deployment_ref"] == "master"
    assert row["deployment_sha"] == "a6571f15c97fb9f35d019f02f13722d4bac9889b"
    assert row["deployment_created_at"] == datetime(
        2026, 8, 23, 14, 44, 10, tzinfo=UTC
    )
    assert row["action"] == "created"
    assert row["repository_name"] == "snacker-tracker/reporter-serverless"
    assert json.loads(row["payload"])["deployment"]["id"] == 6049179907


def test_a_deployment_field_missing_from_a_delivery_reads_null_and_is_kept():
    delivery = a_deployment_delivery(
        deployment={"id": 6049179907, "created_at": "2026-08-23T14:44:10Z"}
    )

    df = parse_raw_file(one_line(delivery), SOURCE_KEY, "deployment")

    assert df.height == 1
    row = df.row(0, named=True)
    assert row["deployment_id"] == 6049179907
    assert row["deployment_created_at"] == datetime(
        2026, 8, 23, 14, 44, 10, tzinfo=UTC
    )
    assert row["deployment_environment"] is None
    assert row["deployment_task"] is None
    assert row["deployment_ref"] is None
    assert row["deployment_sha"] is None
