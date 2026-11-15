"""Parsing GitHub webhook Raw files into Bronze rows.

Pure functions only: bytes in, rows out. Nothing here touches S3 or Delta.
"""

import json
from datetime import date, datetime

import polars as pl

from aws_data.lib.github_s3 import SOURCE_PREFIX, github_raw_key

CHECK_RUN_SCHEMA = {
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
    "check_run_id": pl.Int64,
    "check_run_status": pl.Utf8,
    "check_run_conclusion": pl.Utf8,
    "payload": pl.Utf8,
}


def _check_run_row(envelope: dict, source_key: str) -> dict:
    payload = envelope["detail"]["payload"]
    check_run = payload["check_run"]
    return {
        "id": envelope["id"],
        "time": datetime.fromisoformat(envelope["time"].replace("Z", "+00:00")),
        "account": envelope["account"],
        "region": envelope["region"],
        "detail_type": envelope["detail-type"],
        "source_file": source_key,
        "action": payload["action"],
        "repository_id": payload["repository"]["id"],
        "repository_name": payload["repository"]["full_name"],
        "organization_id": payload["organization"]["id"],
        "organization_name": payload["organization"]["login"],
        "sender_id": payload["sender"]["id"],
        "sender_name": payload["sender"]["login"],
        "check_run_id": check_run["id"],
        "check_run_status": check_run["status"],
        "check_run_conclusion": check_run.get("conclusion"),
        "payload": json.dumps(payload),
    }


def parse_check_run_file(body: bytes, source_key: str) -> pl.DataFrame:
    """One row per envelope in a Raw check_run file, one-line or multi-line JSON."""
    rows = [
        _check_run_row(json.loads(line), source_key)
        for line in body.decode().splitlines()
        if line.strip()
    ]
    return pl.DataFrame(rows, schema=CHECK_RUN_SCHEMA)


def rows_for_utc_day(df: pl.DataFrame, day: date) -> pl.DataFrame:
    """The rows whose envelope `time` falls on `day` in UTC."""
    return df.filter(pl.col("time").dt.date() == day)


def raw_check_run_prefix(day: date) -> str:
    """The Raw prefix holding one UTC day of check_run files: `github/raw/check_run/yyyy/mm/dd/`."""
    return github_raw_key(f"{SOURCE_PREFIX}/check_run/{day:%Y/%m/%d}/")
