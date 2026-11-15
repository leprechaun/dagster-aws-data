import json
from datetime import datetime, timezone

from aws_data.lib.github_bronze import parse_check_run_file
from tests.github_envelopes import envelope, one_line

SOURCE_KEY = "github/raw/check_run/2026/09/08/webhooks-2-2026-09-08-17-17-42-abc"


def test_a_one_line_file_becomes_one_row_with_the_envelope_and_payload_columns():
    df = parse_check_run_file(one_line(envelope()), SOURCE_KEY)

    assert df.height == 1
    row = df.row(0, named=True)
    assert row["id"] == "60201608-a89e-7b82-3f96-90a129cbf3b0"
    assert row["time"] == datetime(2026, 9, 8, 17, 17, 42, tzinfo=timezone.utc)
    assert row["account"] == "307985306317"
    assert row["region"] == "ap-southeast-1"
    assert row["detail_type"] == "check_run"
    assert row["source_file"] == SOURCE_KEY
    assert row["action"] == "created"
    assert row["repository_id"] == 1234
    assert row["repository_name"] == "snacker-tracker/reporter-serverless"
    assert row["organization_id"] == 5678
    assert row["organization_name"] == "snacker-tracker"
    assert row["sender_id"] == 91011
    assert row["sender_name"] == "octocat"
    assert row["check_run_id"] == 102162572568
    assert row["check_run_status"] == "queued"
    assert row["check_run_conclusion"] is None
    assert json.loads(row["payload"])["check_run"]["name"] == "deploy-prod"


def test_a_multi_line_file_becomes_one_row_per_envelope_in_file_order():
    first = envelope(id="first", time="2026-09-08T17:17:42Z", action="created")
    second = envelope(
        id="second",
        time="2026-09-08T17:18:00Z",
        action="completed",
        check_run={
            "id": 7,
            "status": "completed",
            "conclusion": "success",
            "name": "test",
        },
    )
    body = one_line(first) + one_line(second)

    df = parse_check_run_file(body, SOURCE_KEY)

    assert df["id"].to_list() == ["first", "second"]
    assert df["action"].to_list() == ["created", "completed"]
    assert df["check_run_conclusion"].to_list() == [None, "success"]
    assert df["source_file"].to_list() == [SOURCE_KEY, SOURCE_KEY]


def test_a_missing_optional_field_becomes_null_and_the_row_is_kept():
    no_conclusion = {"id": 8, "status": "in_progress", "name": "lint"}
    body = one_line(envelope(id="no-conclusion", check_run=no_conclusion))

    df = parse_check_run_file(body, SOURCE_KEY)

    assert df.height == 1
    assert df["check_run_status"].to_list() == ["in_progress"]
    assert df["check_run_conclusion"].to_list() == [None]


def test_envelopes_sharing_an_id_are_both_kept_as_separate_rows():
    same_id = "duplicate-delivery"
    body = one_line(envelope(id=same_id, action="created")) + one_line(
        envelope(id=same_id, action="created")
    )

    df = parse_check_run_file(body, SOURCE_KEY)

    assert df.height == 2
    assert df["id"].to_list() == [same_id, same_id]

