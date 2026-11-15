"""Parsing GitHub webhook Raw files into Bronze rows.

Pure functions only: bytes in, rows out. Nothing here touches S3 or Delta.

Every event type shares one schema: the envelope, the fields common to GitHub
payloads, and the full payload as JSON for everything type-specific.
"""

import json
from datetime import datetime

import polars as pl

from aws_data.lib.github_s3 import SOURCE_PREFIX, github_raw_key

BRONZE_SCHEMA = {
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
    "payload": pl.Utf8,
}


def _row(envelope: dict, source_key: str) -> dict:
    payload = envelope["detail"]["payload"]
    repository = payload.get("repository", {})
    organization = payload.get("organization", {})
    sender = payload.get("sender", {})
    return {
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
        "payload": json.dumps(payload),
    }


def parse_raw_file(body: bytes, source_key: str) -> pl.DataFrame:
    """One row per envelope in a Raw file, one-line or multi-line JSON."""
    rows = [
        _row(json.loads(line), source_key)
        for line in body.decode().splitlines()
        if line.strip()
    ]
    return pl.DataFrame(rows, schema=BRONZE_SCHEMA)


def raw_prefix(event_type: str) -> str:
    """The Raw prefix holding every file of one event type: `github/raw/<event_type>/`."""
    return github_raw_key(f"{SOURCE_PREFIX}/{event_type}/")


def event_types_with_new_files(copied_by_type: dict[str, list[int]]) -> list[str]:
    """The event types where at least one Raw mirror run copied a file, sorted.
    A Bronze table only needs rebuilding when its type gained a file."""
    return sorted(
        t for t, counts in copied_by_type.items() if any(n > 0 for n in counts)
    )
