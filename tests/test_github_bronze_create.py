import json

import polars as pl

from aws_data.lib.github_bronze import bronze_schema, parse_raw_file
from tests.github_envelopes import envelope, one_line

SOURCE_KEY = "github/raw/create/2026/11/11/webhooks-2-2026-11-11-14-56-37-abc"


def create_delivery(payload_fields: dict, *, id: str = "create-1") -> dict:
    delivery = envelope(id=id, detail_type="create", action=None)
    delivery["detail"]["payload"] = {
        "repository": {"id": 1428184726, "full_name": "snacker-tracker/interface"},
        "organization": {"id": 59108816, "login": "snacker-tracker"},
        "sender": {"id": 29139614, "login": "renovate[bot]"},
        **payload_fields,
    }
    return delivery


def test_a_create_delivery_schema_is_the_common_columns_then_its_typed_columns_then_payload():
    assert list(bronze_schema("create").items()) == [
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
        ("create_ref", pl.Utf8),
        ("create_ref_type", pl.Utf8),
        ("create_master_branch", pl.Utf8),
        ("create_pusher_type", pl.Utf8),
        ("payload", pl.Utf8),
    ]


def test_a_branch_create_delivery_fills_its_create_columns_from_the_payload():
    delivery = create_delivery(
        {
            "ref": "renovate/terraform-monorepo",
            "ref_type": "branch",
            "master_branch": "master",
            "pusher_type": "user",
        }
    )

    df = parse_raw_file(one_line(delivery), SOURCE_KEY, "create")

    row = df.row(0, named=True)
    assert row["create_ref"] == "renovate/terraform-monorepo"
    assert row["create_ref_type"] == "branch"
    assert row["create_master_branch"] == "master"
    assert row["create_pusher_type"] == "user"
    assert json.loads(row["payload"]) == delivery["detail"]["payload"]


def test_a_tag_create_delivery_keeps_its_ref_type_as_tag():
    delivery = create_delivery(
        {
            "ref": "v1.2.3",
            "ref_type": "tag",
            "master_branch": "master",
            "pusher_type": "user",
        },
        id="create-tag",
    )

    df = parse_raw_file(one_line(delivery), SOURCE_KEY, "create")

    assert df["id"].to_list() == ["create-tag"]
    assert df["create_ref"].to_list() == ["v1.2.3"]
    assert df["create_ref_type"].to_list() == ["tag"]


def test_a_create_delivery_missing_optional_fields_reads_null_and_keeps_the_row():
    delivery = create_delivery(
        {"ref": "feature/x", "ref_type": "branch"}, id="create-sparse"
    )

    df = parse_raw_file(one_line(delivery), SOURCE_KEY, "create")

    assert df["id"].to_list() == ["create-sparse"]
    assert df["create_ref"].to_list() == ["feature/x"]
    assert df["create_ref_type"].to_list() == ["branch"]
    assert df["create_master_branch"].to_list() == [None]
    assert df["create_pusher_type"].to_list() == [None]


def test_a_create_delivery_with_no_ref_reads_its_create_columns_null():
    delivery = create_delivery({"ref_type": "branch"}, id="create-no-ref")

    df = parse_raw_file(one_line(delivery), SOURCE_KEY, "create")

    assert df["id"].to_list() == ["create-no-ref"]
    assert df["create_ref"].to_list() == [None]
    assert df["create_ref_type"].to_list() == ["branch"]
