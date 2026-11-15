import json

import polars as pl

from aws_data.lib.github_bronze import bronze_schema, parse_raw_file
from tests.github_envelopes import envelope, one_line

SOURCE_KEY = "github/raw/delete/2026/09/08/webhooks-2-2026-09-08-17-17-42-abc"

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


def test_delete_schema_is_the_common_columns_then_its_typed_columns_then_payload():
    assert list(bronze_schema("delete").items()) == [
        *COMMON_COLUMNS,
        ("delete_ref", pl.Utf8),
        ("delete_ref_type", pl.Utf8),
        ("delete_pusher_type", pl.Utf8),
        ("payload", pl.Utf8),
    ]


def test_a_delete_delivery_fills_its_typed_columns_from_the_payload():
    delivery = envelope(id="delete-1", detail_type="delete", action=None)
    delivery["detail"]["payload"]["ref"] = "feature/bronze-delete"
    delivery["detail"]["payload"]["ref_type"] = "branch"
    delivery["detail"]["payload"]["pusher_type"] = "user"

    df = parse_raw_file(one_line(delivery), SOURCE_KEY, "delete")

    assert df.height == 1
    row = df.row(0, named=True)
    assert row["id"] == "delete-1"
    assert row["detail_type"] == "delete"
    assert row["action"] is None
    assert row["delete_ref"] == "feature/bronze-delete"
    assert row["delete_ref_type"] == "branch"
    assert row["delete_pusher_type"] == "user"
    assert json.loads(row["payload"])["ref"] == "feature/bronze-delete"


def test_a_delete_delivery_missing_its_optional_fields_reads_null_and_is_kept():
    delivery = envelope(id="delete-2", detail_type="delete", action=None)
    delivery["detail"]["payload"]["ref"] = "v1.0.0"
    delivery["detail"]["payload"]["ref_type"] = "tag"

    df = parse_raw_file(one_line(delivery), SOURCE_KEY, "delete")

    assert df.height == 1
    row = df.row(0, named=True)
    assert row["delete_ref"] == "v1.0.0"
    assert row["delete_ref_type"] == "tag"
    assert row["delete_pusher_type"] is None
