import json

import polars as pl

from aws_data.lib.github_bronze import bronze_schema, parse_raw_file
from tests.github_envelopes import envelope, one_line

SOURCE_KEY = "github/raw/label/2026/11/15/webhooks-2-2026-11-15-06-45-07-abc"


def label_delivery(label: dict, *, id: str = "label-delivery-1"):
    delivery = envelope(id=id, detail_type="label", action="created")
    delivery["detail"]["payload"]["label"] = label
    return delivery


def test_label_schema_is_the_common_columns_then_its_label_columns_then_payload():
    assert list(bronze_schema("label").items()) == [
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
        ("label_id", pl.Int64),
        ("label_name", pl.Utf8),
        ("label_color", pl.Utf8),
        ("payload", pl.Utf8),
    ]


def test_a_label_delivery_fills_its_label_columns_from_the_payload():
    delivery = label_delivery(
        {
            "id": 12779272138,
            "node_id": "LA_kwDOVSBels8AAAAC-bQ3yg",
            "url": "https://api.github.com/repos/snacker-tracker/interface/labels/spec",
            "name": "spec",
            "color": "5319e7",
            "default": False,
            "description": "",
            "archived_at": None,
            "archived_by": None,
        }
    )

    df = parse_raw_file(one_line(delivery), SOURCE_KEY, "label")

    assert df.height == 1
    row = df.row(0, named=True)
    assert row["detail_type"] == "label"
    assert row["action"] == "created"
    assert row["label_id"] == 12779272138
    assert row["label_name"] == "spec"
    assert row["label_color"] == "5319e7"
    assert json.loads(row["payload"])["label"]["description"] == ""


def test_a_label_delivery_missing_its_color_reads_null_and_is_kept():
    delivery = label_delivery(
        {"id": 12904904458, "name": "feature:stepdag"}, id="no-color"
    )

    df = parse_raw_file(one_line(delivery), SOURCE_KEY, "label")

    assert df["id"].to_list() == ["no-color"]
    assert df["label_id"].to_list() == [12904904458]
    assert df["label_name"].to_list() == ["feature:stepdag"]
    assert df["label_color"].to_list() == [None]


def test_a_label_delivery_missing_all_its_label_fields_reads_null_and_is_kept():
    delivery = label_delivery({}, id="empty-label")

    df = parse_raw_file(one_line(delivery), SOURCE_KEY, "label")

    assert df["id"].to_list() == ["empty-label"]
    assert df["label_id"].to_list() == [None]
    assert df["label_name"].to_list() == [None]
    assert df["label_color"].to_list() == [None]
