import json

import polars as pl

from aws_data.lib.github_bronze import bronze_schema, parse_raw_file
from tests.github_envelopes import envelope, one_line

SOURCE_KEY = "github/raw/push/2026/08/24/webhooks-2-2026-08-24-15-02-30-abc"


def push_delivery(payload: dict, id: str = "push-delivery-1"):
    delivery = envelope(id=id, detail_type="push", action=None)
    delivery["detail"]["payload"] = payload
    return delivery


def test_a_push_delivery_fills_each_push_column_from_the_payload():
    payload = {
        "ref": "refs/heads/main",
        "before": "1111111111111111111111111111111111111111",
        "after": "2222222222222222222222222222222222222222",
        "forced": False,
        "created": False,
        "deleted": False,
        "pusher": {"name": "octocat", "email": "octocat@example.com"},
        "repository": {"id": 1234, "full_name": "snacker-tracker/reporter-serverless"},
        "organization": {"id": 5678, "login": "snacker-tracker"},
        "sender": {"id": 91011, "login": "octocat"},
        "commits": [
            {"id": "1111111", "message": "First"},
            {"id": "2222222", "message": "Second"},
        ],
        "head_commit": {
            "id": "2222222222222222222222222222222222222222",
            "message": "Second commit",
        },
    }

    df = parse_raw_file(one_line(push_delivery(payload)), SOURCE_KEY, "push")

    assert df.height == 1
    row = df.row(0, named=True)
    assert row["push_ref"] == "refs/heads/main"
    assert row["push_before"] == "1111111111111111111111111111111111111111"
    assert row["push_after"] == "2222222222222222222222222222222222222222"
    assert row["push_forced"] is False
    assert row["push_created"] is False
    assert row["push_deleted"] is False
    assert row["push_pusher_name"] == "octocat"
    assert row["push_commit_count"] == 2
    assert row["push_head_commit_id"] == "2222222222222222222222222222222222222222"
    assert row["push_head_commit_message"] == "Second commit"
    assert json.loads(row["payload"])["ref"] == "refs/heads/main"


def test_a_forced_push_to_a_new_branch_fills_the_booleans_and_keeps_the_row():
    payload = {
        "ref": "refs/heads/feature/x",
        "before": "0000000000000000000000000000000000000000",
        "after": "3333333333333333333333333333333333333333",
        "forced": True,
        "created": True,
        "deleted": False,
        "pusher": {"name": "octocat"},
        "commits": [{"id": "3333333", "message": "Only"}],
        "head_commit": {
            "id": "3333333333333333333333333333333333333333",
            "message": "Only",
        },
    }

    df = parse_raw_file(one_line(push_delivery(payload)), SOURCE_KEY, "push")

    row = df.row(0, named=True)
    assert row["push_forced"] is True
    assert row["push_created"] is True
    assert row["push_deleted"] is False
    assert row["push_commit_count"] == 1


def test_a_branch_deletion_has_null_head_commit_and_zero_commits_and_is_kept():
    payload = {
        "ref": "refs/heads/old-branch",
        "before": "4444444444444444444444444444444444444444",
        "after": "0000000000000000000000000000000000000000",
        "forced": False,
        "created": False,
        "deleted": True,
        "pusher": {"name": "octocat"},
        "commits": [],
        "head_commit": None,
    }

    df = parse_raw_file(one_line(push_delivery(payload)), SOURCE_KEY, "push")

    assert df.height == 1
    row = df.row(0, named=True)
    assert row["push_ref"] == "refs/heads/old-branch"
    assert row["push_deleted"] is True
    assert row["push_commit_count"] == 0
    assert row["push_head_commit_id"] is None
    assert row["push_head_commit_message"] is None


def test_a_push_delivery_with_null_or_missing_commits_counts_zero_and_keeps_the_row():
    null_commits = {
        "ref": "refs/heads/main",
        "pusher": {"name": "octocat"},
        "commits": None,
        "head_commit": None,
    }
    missing_commits = {
        "ref": "refs/heads/main",
        "pusher": {"name": "octocat"},
        "head_commit": None,
    }
    body = one_line(push_delivery(null_commits, id="a")) + one_line(
        push_delivery(missing_commits, id="b")
    )

    df = parse_raw_file(body, SOURCE_KEY, "push")

    assert df["id"].to_list() == ["a", "b"]
    assert df["push_commit_count"].to_list() == [0, 0]


def test_a_push_field_missing_from_a_delivery_reads_null_and_is_kept():
    payload = {"ref": "refs/heads/main"}

    df = parse_raw_file(one_line(push_delivery(payload)), SOURCE_KEY, "push")

    assert df.height == 1
    row = df.row(0, named=True)
    assert row["push_ref"] == "refs/heads/main"
    assert row["push_before"] is None
    assert row["push_after"] is None
    assert row["push_forced"] is None
    assert row["push_created"] is None
    assert row["push_deleted"] is None
    assert row["push_pusher_name"] is None
    assert row["push_head_commit_id"] is None
    assert row["push_head_commit_message"] is None


def test_the_push_schema_is_the_common_columns_then_its_push_columns_then_payload():
    assert list(bronze_schema("push").items()) == [
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
        ("push_ref", pl.Utf8),
        ("push_before", pl.Utf8),
        ("push_after", pl.Utf8),
        ("push_forced", pl.Boolean),
        ("push_created", pl.Boolean),
        ("push_deleted", pl.Boolean),
        ("push_pusher_name", pl.Utf8),
        ("push_commit_count", pl.Int64),
        ("push_head_commit_id", pl.Utf8),
        ("push_head_commit_message", pl.Utf8),
        ("payload", pl.Utf8),
    ]
