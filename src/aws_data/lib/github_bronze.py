"""Parsing GitHub webhook Raw files into Bronze rows.

Pure functions only: bytes in, rows out. Nothing here touches S3 or Delta.

Every event type has the envelope and the fields common to GitHub payloads.
A type with a spec in TYPE_COLUMNS adds its own typed columns after those, and
the full payload as JSON always comes last.
"""

import json
from collections.abc import Callable
from datetime import datetime
from typing import Any

import polars as pl

from aws_data.lib.github_s3 import SOURCE_PREFIX, github_raw_key

COMMON_SCHEMA = {
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
}


def _push_commit_count(payload: dict) -> int:
    """How many commits a push carried; 0 when the list is missing or null."""
    return len(payload.get("commits") or [])


# Per event type: column name -> (source, Polars dtype). The source is a dotted
# path into the payload, or a function of the payload for a derived value.
# A type without an entry keeps only the common columns.
TYPE_COLUMNS: dict[str, dict[str, tuple[str | Callable[[dict], Any], pl.DataType]]] = {
    "check_run": {
        "check_run_id": ("check_run.id", pl.Int64),
        "check_run_name": ("check_run.name", pl.Utf8),
        "check_run_head_sha": ("check_run.head_sha", pl.Utf8),
        "check_run_status": ("check_run.status", pl.Utf8),
        "check_run_conclusion": ("check_run.conclusion", pl.Utf8),
        "check_run_started_at": ("check_run.started_at", pl.Datetime("us", "UTC")),
        "check_run_completed_at": ("check_run.completed_at", pl.Datetime("us", "UTC")),
    },
    "check_suite": {
        "check_suite_id": ("check_suite.id", pl.Int64),
        "check_suite_head_branch": ("check_suite.head_branch", pl.Utf8),
        "check_suite_head_sha": ("check_suite.head_sha", pl.Utf8),
        "check_suite_status": ("check_suite.status", pl.Utf8),
        "check_suite_conclusion": ("check_suite.conclusion", pl.Utf8),
        "check_suite_created_at": ("check_suite.created_at", pl.Datetime("us", "UTC")),
        "check_suite_updated_at": ("check_suite.updated_at", pl.Datetime("us", "UTC")),
    },
    "workflow_run": {
        "workflow_run_id": ("workflow_run.id", pl.Int64),
        "workflow_run_name": ("workflow_run.name", pl.Utf8),
        "workflow_run_workflow_id": ("workflow_run.workflow_id", pl.Int64),
        "workflow_run_check_suite_id": ("workflow_run.check_suite_id", pl.Int64),
        "workflow_run_run_number": ("workflow_run.run_number", pl.Int64),
        "workflow_run_run_attempt": ("workflow_run.run_attempt", pl.Int64),
        "workflow_run_event": ("workflow_run.event", pl.Utf8),
        "workflow_run_head_branch": ("workflow_run.head_branch", pl.Utf8),
        "workflow_run_head_sha": ("workflow_run.head_sha", pl.Utf8),
        "workflow_run_status": ("workflow_run.status", pl.Utf8),
        "workflow_run_conclusion": ("workflow_run.conclusion", pl.Utf8),
        "workflow_run_run_started_at": (
            "workflow_run.run_started_at",
            pl.Datetime("us", "UTC"),
        ),
        "workflow_run_updated_at": (
            "workflow_run.updated_at",
            pl.Datetime("us", "UTC"),
        ),
    },
    "workflow_job": {
        "workflow_job_id": ("workflow_job.id", pl.Int64),
        "workflow_job_run_id": ("workflow_job.run_id", pl.Int64),
        "workflow_job_workflow_name": ("workflow_job.workflow_name", pl.Utf8),
        "workflow_job_name": ("workflow_job.name", pl.Utf8),
        "workflow_job_run_attempt": ("workflow_job.run_attempt", pl.Int64),
        "workflow_job_head_branch": ("workflow_job.head_branch", pl.Utf8),
        "workflow_job_head_sha": ("workflow_job.head_sha", pl.Utf8),
        "workflow_job_status": ("workflow_job.status", pl.Utf8),
        "workflow_job_conclusion": ("workflow_job.conclusion", pl.Utf8),
        "workflow_job_started_at": (
            "workflow_job.started_at",
            pl.Datetime("us", "UTC"),
        ),
        "workflow_job_completed_at": (
            "workflow_job.completed_at",
            pl.Datetime("us", "UTC"),
        ),
        "workflow_job_runner_name": ("workflow_job.runner_name", pl.Utf8),
    },
    "push": {
        "push_ref": ("ref", pl.Utf8),
        "push_before": ("before", pl.Utf8),
        "push_after": ("after", pl.Utf8),
        "push_forced": ("forced", pl.Boolean),
        "push_created": ("created", pl.Boolean),
        "push_deleted": ("deleted", pl.Boolean),
        "push_pusher_name": ("pusher.name", pl.Utf8),
        "push_commit_count": (_push_commit_count, pl.Int64),
        "push_head_commit_id": ("head_commit.id", pl.Utf8),
        "push_head_commit_message": ("head_commit.message", pl.Utf8),
    },
    "create": {
        "create_ref": ("ref", pl.Utf8),
        "create_ref_type": ("ref_type", pl.Utf8),
        "create_master_branch": ("master_branch", pl.Utf8),
        "create_pusher_type": ("pusher_type", pl.Utf8),
    },
    "delete": {
        "delete_ref": ("ref", pl.Utf8),
        "delete_ref_type": ("ref_type", pl.Utf8),
        "delete_pusher_type": ("pusher_type", pl.Utf8),
    },
    "deployment": {
        "deployment_id": ("deployment.id", pl.Int64),
        "deployment_environment": ("deployment.environment", pl.Utf8),
        "deployment_task": ("deployment.task", pl.Utf8),
        "deployment_ref": ("deployment.ref", pl.Utf8),
        "deployment_sha": ("deployment.sha", pl.Utf8),
        "deployment_created_at": ("deployment.created_at", pl.Datetime("us", "UTC")),
    },
}


def bronze_schema(event_type: str) -> dict[str, pl.DataType]:
    """The full ordered Bronze schema for one event type: the common columns,
    then the type's typed columns, then the full payload as JSON last."""
    typed = {
        name: dtype for name, (_, dtype) in TYPE_COLUMNS.get(event_type, {}).items()
    }
    return {**COMMON_SCHEMA, **typed, "payload": pl.Utf8}


def _dig(payload: dict, path: str):
    """The value at a dotted path, or None if any level is missing."""
    value = payload
    for key in path.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def _typed_value(value, dtype: pl.DataType):
    if value is None or not isinstance(dtype, pl.Datetime):
        return value
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _source_value(payload: dict, source: str | Callable[[dict], Any]):
    return source(payload) if callable(source) else _dig(payload, source)


def _typed_columns(payload: dict, event_type: str) -> dict:
    return {
        name: _typed_value(_source_value(payload, source), dtype)
        for name, (source, dtype) in TYPE_COLUMNS.get(event_type, {}).items()
    }


def _row(envelope: dict, source_key: str, event_type: str) -> dict:
    payload = envelope["detail"]["payload"]
    repository = payload.get("repository", {})
    organization = payload.get("organization", {})
    sender = payload.get("sender", {})
    row = {
        "id": envelope["id"],
        "time": datetime.fromisoformat(envelope["time"].replace("Z", "+00:00")),
        "account": envelope["account"],
        "region": envelope["region"],
        "detail_type": envelope["detail-type"],
        "source_file": source_key,
        "action": payload.get("action"),
        "repository_id": repository.get("id"),
        "repository_name": repository.get("full_name"),
        "organization_id": organization.get("id"),
        "organization_name": organization.get("login"),
        "sender_id": sender.get("id"),
        "sender_name": sender.get("login"),
    }
    row.update(_typed_columns(payload, event_type))
    row["payload"] = json.dumps(payload)
    return row


def parse_raw_file(body: bytes, source_key: str, event_type: str) -> pl.DataFrame:
    """One row per envelope in a Raw file, one-line or multi-line JSON."""
    rows = [
        _row(json.loads(line), source_key, event_type)
        for line in body.decode().splitlines()
        if line.strip()
    ]
    return pl.DataFrame(rows, schema=bronze_schema(event_type))


def raw_prefix(event_type: str) -> str:
    """The Raw prefix holding every file of one event type: `github/raw/<event_type>/`."""
    return github_raw_key(f"{SOURCE_PREFIX}/{event_type}/")
