import json
from datetime import UTC, datetime

import polars as pl

from aws_data.lib.github_bronze import bronze_schema, parse_raw_file
from tests.github_envelopes import envelope, one_line

SOURCE_KEY = "github/raw/release/2026/10/24/webhooks-2-2026-10-24-10-30-40-abc"


def release_delivery(
    *, id: str = "44fff932-f12d-81db-7bde-a81080e4ce84", action: str, release: dict
):
    delivery = envelope(
        id=id,
        time="2026-10-24T10:30:40Z",
        detail_type="release",
        action=action,
    )
    payload = delivery["detail"]["payload"]
    del payload["check_run"]
    payload["release"] = release
    return delivery


def test_release_schema_is_the_common_columns_then_its_typed_columns_then_payload():
    assert list(bronze_schema("release").items()) == [
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
        ("release_id", pl.Int64),
        ("release_tag_name", pl.Utf8),
        ("release_name", pl.Utf8),
        ("release_draft", pl.Boolean),
        ("release_prerelease", pl.Boolean),
        ("release_published_at", pl.Datetime("us", "UTC")),
        ("payload", pl.Utf8),
    ]


def test_a_published_release_delivery_fills_its_typed_columns_from_the_payload():
    release = {
        "id": 422663834,
        "tag_name": "v0.1.55",
        "name": "v0.1.55",
        "draft": False,
        "prerelease": False,
        "published_at": "2026-10-24T10:30:39Z",
    }

    df = parse_raw_file(
        one_line(release_delivery(action="published", release=release)),
        SOURCE_KEY,
        "release",
    )

    row = df.row(0, named=True)
    assert row["id"] == "44fff932-f12d-81db-7bde-a81080e4ce84"
    assert row["action"] == "published"
    assert row["repository_name"] == "snacker-tracker/reporter-serverless"
    assert row["release_id"] == 422663834
    assert row["release_tag_name"] == "v0.1.55"
    assert row["release_name"] == "v0.1.55"
    assert row["release_draft"] is False
    assert row["release_prerelease"] is False
    assert row["release_published_at"] == datetime(
        2026, 10, 24, 10, 30, 39, tzinfo=UTC
    )


def test_a_draft_release_delivery_reads_null_published_at_and_is_kept():
    release = {
        "id": 422663999,
        "tag_name": "v0.2.0",
        "name": None,
        "draft": True,
        "prerelease": False,
        "published_at": None,
    }

    df = parse_raw_file(
        one_line(release_delivery(action="created", release=release)),
        SOURCE_KEY,
        "release",
    )

    assert df.height == 1
    row = df.row(0, named=True)
    assert row["release_draft"] is True
    assert row["release_tag_name"] == "v0.2.0"
    assert row["release_name"] is None
    assert row["release_published_at"] is None


def test_a_release_delivery_missing_optional_fields_reads_them_null_and_keeps_the_row():
    release = {"id": 422663834, "tag_name": "v0.1.56"}

    df = parse_raw_file(
        one_line(release_delivery(action="prereleased", release=release)),
        SOURCE_KEY,
        "release",
    )

    assert df.height == 1
    row = df.row(0, named=True)
    assert row["release_id"] == 422663834
    assert row["release_tag_name"] == "v0.1.56"
    assert row["release_name"] is None
    assert row["release_draft"] is None
    assert row["release_prerelease"] is None
    assert row["release_published_at"] is None


def test_a_release_delivery_with_no_release_key_reads_every_release_column_null():
    delivery = release_delivery(action="deleted", release={})
    del delivery["detail"]["payload"]["release"]

    df = parse_raw_file(one_line(delivery), SOURCE_KEY, "release")

    assert df.height == 1
    row = df.row(0, named=True)
    for column in [name for name in df.columns if name.startswith("release_")]:
        assert row[column] is None, column


def test_release_columns_follow_its_schema_and_the_payload_round_trips():
    delivery = release_delivery(
        action="published",
        release={
            "id": 422663834,
            "tag_name": "v0.1.55",
            "name": "v0.1.55",
            "draft": False,
            "prerelease": True,
            "published_at": "2026-10-24T10:30:39Z",
        },
    )

    df = parse_raw_file(one_line(delivery), SOURCE_KEY, "release")

    assert df.schema == bronze_schema("release")
    assert json.loads(df.row(0, named=True)["payload"]) == delivery["detail"]["payload"]
