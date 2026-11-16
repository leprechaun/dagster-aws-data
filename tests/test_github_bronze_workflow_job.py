from datetime import UTC, datetime

import polars as pl

from aws_data.lib.github_bronze import bronze_schema, parse_raw_file
from tests.github_envelopes import envelope, one_line

SOURCE_KEY = "github/raw/workflow_job/2026/08/08/webhooks-2-2026-08-08-10-32-19-abc"


def workflow_job_delivery(workflow_job: dict, action: str = "completed") -> dict:
    delivery = envelope(id="job-delivery", detail_type="workflow_job", action=action)
    payload = delivery["detail"]["payload"]
    del payload["check_run"]
    payload["workflow_job"] = workflow_job
    return delivery


def test_workflow_job_schema_is_the_common_columns_then_its_typed_columns_then_payload():
    schema = bronze_schema("workflow_job")

    assert list(schema.items())[13:] == [
        ("workflow_job_id", pl.Int64),
        ("workflow_job_run_id", pl.Int64),
        ("workflow_job_workflow_name", pl.Utf8),
        ("workflow_job_name", pl.Utf8),
        ("workflow_job_run_attempt", pl.Int64),
        ("workflow_job_head_branch", pl.Utf8),
        ("workflow_job_head_sha", pl.Utf8),
        ("workflow_job_status", pl.Utf8),
        ("workflow_job_conclusion", pl.Utf8),
        ("workflow_job_started_at", pl.Datetime("us", "UTC")),
        ("workflow_job_completed_at", pl.Datetime("us", "UTC")),
        ("workflow_job_runner_name", pl.Utf8),
        ("payload", pl.Utf8),
    ]


def test_a_completed_workflow_job_delivery_fills_its_typed_columns_from_the_payload():
    job = {
        "id": 93092064248,
        "run_id": 31252968611,
        "workflow_name": "Code Quality: Scheduled",
        "name": "Analyze (python)",
        "run_attempt": 1,
        "head_branch": "master",
        "head_sha": "0b43ae61866d4b9a7e74a02fbee4e8099f0b5c04",
        "status": "completed",
        "conclusion": "success",
        "started_at": "2026-08-08T10:31:39Z",
        "completed_at": "2026-08-08T10:32:16Z",
        "runner_name": "GitHub Actions 1000000001",
    }

    df = parse_raw_file(
        one_line(workflow_job_delivery(job)), SOURCE_KEY, "workflow_job"
    )

    assert df.height == 1
    row = df.row(0, named=True)
    assert row["workflow_job_id"] == 93092064248
    assert row["workflow_job_run_id"] == 31252968611
    assert row["workflow_job_workflow_name"] == "Code Quality: Scheduled"
    assert row["workflow_job_name"] == "Analyze (python)"
    assert row["workflow_job_run_attempt"] == 1
    assert row["workflow_job_head_branch"] == "master"
    assert row["workflow_job_head_sha"] == "0b43ae61866d4b9a7e74a02fbee4e8099f0b5c04"
    assert row["workflow_job_status"] == "completed"
    assert row["workflow_job_conclusion"] == "success"
    assert row["workflow_job_started_at"] == datetime(
        2026, 8, 8, 10, 31, 39, tzinfo=UTC
    )
    assert row["workflow_job_completed_at"] == datetime(
        2026, 8, 8, 10, 32, 16, tzinfo=UTC
    )
    assert row["workflow_job_runner_name"] == "GitHub Actions 1000000001"


def test_a_queued_workflow_job_reads_conclusion_completed_at_and_runner_null_and_is_kept():
    job = {
        "id": 93165403089,
        "run_id": 31282271357,
        "workflow_name": "Scheduled",
        "name": "build",
        "run_attempt": 1,
        "head_branch": "master",
        "head_sha": "0b43ae61866d4b9a7e74a02fbee4e8099f0b5c04",
        "status": "queued",
        "conclusion": None,
        "started_at": "2026-08-08T22:41:27Z",
        "completed_at": None,
        "runner_name": None,
    }

    df = parse_raw_file(
        one_line(workflow_job_delivery(job, action="queued")),
        SOURCE_KEY,
        "workflow_job",
    )

    assert df.height == 1
    row = df.row(0, named=True)
    assert row["workflow_job_id"] == 93165403089
    assert row["workflow_job_status"] == "queued"
    assert row["workflow_job_conclusion"] is None
    assert row["workflow_job_completed_at"] is None
    assert row["workflow_job_runner_name"] is None


def test_a_workflow_job_delivery_with_no_workflow_job_key_reads_every_column_null():
    delivery = envelope(id="no-job", detail_type="workflow_job", action="queued")
    delivery["detail"]["payload"].pop("check_run")

    df = parse_raw_file(one_line(delivery), SOURCE_KEY, "workflow_job")

    assert df.height == 1
    for column in [name for name in df.columns if name.startswith("workflow_job_")]:
        assert df[column].to_list() == [None], column


def test_a_malformed_workflow_job_timestamp_reads_null_and_keeps_the_row():
    job = {"id": 1, "started_at": "not-a-time", "completed_at": "2026-08-08T10:32:16Z"}

    df = parse_raw_file(
        one_line(workflow_job_delivery(job)), SOURCE_KEY, "workflow_job"
    )

    assert df["workflow_job_started_at"].to_list() == [None]
    assert df["workflow_job_completed_at"].to_list() == [
        datetime(2026, 8, 8, 10, 32, 16, tzinfo=UTC)
    ]
