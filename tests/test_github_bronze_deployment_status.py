from datetime import datetime, timezone

import polars as pl

from aws_data.lib.github_bronze import bronze_schema, parse_raw_file
from tests.github_envelopes import envelope, one_line

SOURCE_KEY = (
    "github/raw/deployment_status/2026/02/03/webhooks-2-2026-02-03-14-17-09-abc"
)


def deployment_status_delivery(
    *, deployment_status: dict, deployment: dict | None = None
):
    delivery = envelope(
        id="deployment-status-1",
        time="2026-02-03T14:17:09Z",
        detail_type="deployment_status",
        action="created",
    )
    payload = delivery["detail"]["payload"]
    del payload["check_run"]
    payload["deployment_status"] = deployment_status
    if deployment is not None:
        payload["deployment"] = deployment
    return delivery


def test_deployment_status_schema_is_the_common_columns_then_its_typed_columns_then_payload():
    assert list(bronze_schema("deployment_status").items()) == [
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
        ("deployment_status_id", pl.Int64),
        ("deployment_status_state", pl.Utf8),
        ("deployment_status_environment", pl.Utf8),
        ("deployment_status_deployment_id", pl.Int64),
        ("deployment_status_created_at", pl.Datetime("us", "UTC")),
        ("payload", pl.Utf8),
    ]


def test_a_deployment_status_delivery_fills_its_typed_columns_from_the_payload():
    delivery = deployment_status_delivery(
        deployment_status={
            "id": 10689594056,
            "state": "in_progress",
            "environment": "dev",
            "created_at": "2026-02-03T14:17:09Z",
        },
        deployment={"id": 3760321203, "environment": "dev"},
    )

    df = parse_raw_file(one_line(delivery), SOURCE_KEY, "deployment_status")

    assert df.height == 1
    row = df.row(0, named=True)
    assert row["deployment_status_id"] == 10689594056
    assert row["deployment_status_state"] == "in_progress"
    assert row["deployment_status_environment"] == "dev"
    assert row["deployment_status_deployment_id"] == 3760321203
    assert row["deployment_status_created_at"] == datetime(
        2026, 2, 3, 14, 17, 9, tzinfo=timezone.utc
    )


def test_a_deployment_status_field_missing_from_a_delivery_reads_null_and_is_kept():
    delivery = deployment_status_delivery(
        deployment_status={"id": 10689594056, "created_at": "2026-02-03T14:17:09Z"},
    )

    df = parse_raw_file(one_line(delivery), SOURCE_KEY, "deployment_status")

    assert df.height == 1
    row = df.row(0, named=True)
    assert row["deployment_status_id"] == 10689594056
    assert row["deployment_status_state"] is None
    assert row["deployment_status_environment"] is None
    assert row["deployment_status_deployment_id"] is None
    assert row["deployment_status_created_at"] == datetime(
        2026, 2, 3, 14, 17, 9, tzinfo=timezone.utc
    )
