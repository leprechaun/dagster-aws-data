"""Parsing Claude Session Status Raw files into Bronze rows.

Pure functions only: bytes in, rows out. Nothing here touches S3 or Delta.

Each row is one Claude Session Status: the envelope, the fields of its `detail`,
then the full statusline payload as JSON. Columns for the payload's nested
fields go between `detail` and `payload` in bronze_schema().
"""

import json
from datetime import datetime

import polars as pl

from aws_data.lib.claude_s3 import CLAUDE_SOURCE_PREFIX, SESSION_STATUS, claude_raw_key


def bronze_schema() -> dict[str, pl.DataType]:
    """The full ordered Bronze schema: the envelope, then the detail fields,
    then the payload as JSON last."""
    return {
        "id": pl.Utf8,
        "time": pl.Datetime("us", "UTC"),
        "account": pl.Utf8,
        "region": pl.Utf8,
        "detail_type": pl.Utf8,
        "source_file": pl.Utf8,
        "client_ip": pl.Utf8,
        "request_id": pl.Utf8,
        "machine": pl.Utf8,
        "payload": pl.Utf8,
    }


def _row(envelope: dict, source_key: str) -> dict:
    detail = envelope["detail"]
    return {
        "id": envelope["id"],
        "time": datetime.fromisoformat(envelope["time"].replace("Z", "+00:00")),
        "account": envelope["account"],
        "region": envelope["region"],
        "detail_type": envelope["detail-type"],
        "source_file": source_key,
        "client_ip": detail.get("client_ip"),
        "request_id": detail.get("request_id"),
        "machine": detail.get("machine"),
        "payload": json.dumps(detail["payload"]),
    }


def parse_raw_file(body: bytes, source_key: str) -> pl.DataFrame:
    """One row per envelope in a Raw file; blank lines are ignored."""
    rows = [
        _row(json.loads(line), source_key)
        for line in body.decode().splitlines()
        if line.strip()
    ]
    return pl.DataFrame(rows, schema=bronze_schema())


def raw_prefix() -> str:
    """The Raw prefix holding every Claude Session Status file: `claude/raw/session-status/`."""
    return claude_raw_key(f"{CLAUDE_SOURCE_PREFIX}/{SESSION_STATUS}/")
