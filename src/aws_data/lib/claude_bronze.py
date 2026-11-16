"""Parsing Claude Session Status Raw files into Bronze rows.

Pure functions only: bytes in, rows out. Nothing here touches S3 or Delta.

Each row is one Claude Session Status: the envelope, the fields of its `detail`,
then each top-level key of the statusline payload as its own column (nested
where the payload nests), then the full payload as JSON. Keys the schema does
not declare survive only in the JSON.
"""

import json
from datetime import datetime
from typing import Any

import polars as pl

from aws_data.lib.claude_s3 import CLAUDE_SOURCE_PREFIX, SESSION_STATUS, claude_raw_key

ENVELOPE_AND_DETAIL_COLUMNS: dict[str, pl.DataType] = {
    "id": pl.Utf8,
    "time": pl.Datetime("us", "UTC"),
    "account": pl.Utf8,
    "region": pl.Utf8,
    "detail_type": pl.Utf8,
    "source_file": pl.Utf8,
    "client_ip": pl.Utf8,
    "request_id": pl.Utf8,
    "machine": pl.Utf8,
}

# The statusline payload's top-level keys, each with its exact name and nested
# shape. Epoch fields stay Int64 Unix seconds as delivered.
PAYLOAD_COLUMNS: dict[str, pl.DataType] = {
    "session_id": pl.Utf8,
    "session_name": pl.Utf8,
    "prompt_id": pl.Utf8,
    "transcript_path": pl.Utf8,
    "cwd": pl.Utf8,
    "scratchpad_dir": pl.Utf8,
    "version": pl.Utf8,
    "model": pl.Struct({"id": pl.Utf8, "display_name": pl.Utf8}),
    "workspace": pl.Struct(
        {
            "current_dir": pl.Utf8,
            "project_dir": pl.Utf8,
            "added_dirs": pl.List(pl.Utf8),
            "repo": pl.Struct({"host": pl.Utf8, "owner": pl.Utf8, "name": pl.Utf8}),
        }
    ),
    "effort": pl.Struct({"level": pl.Utf8}),
    "output_style": pl.Struct({"name": pl.Utf8}),
    "cost": pl.Struct(
        {
            "total_cost_usd": pl.Float64,
            "total_duration_ms": pl.Int64,
            "total_api_duration_ms": pl.Int64,
            "total_lines_added": pl.Int64,
            "total_lines_removed": pl.Int64,
        }
    ),
    "context_window": pl.Struct(
        {
            "total_input_tokens": pl.Int64,
            "total_output_tokens": pl.Int64,
            "context_window_size": pl.Int64,
            "current_usage": pl.Struct(
                {
                    "input_tokens": pl.Int64,
                    "output_tokens": pl.Int64,
                    "cache_creation_input_tokens": pl.Int64,
                    "cache_read_input_tokens": pl.Int64,
                }
            ),
            "used_percentage": pl.Int64,
            "remaining_percentage": pl.Int64,
        }
    ),
    "exceeds_200k_tokens": pl.Boolean,
    "prompt_cache": pl.Struct(
        {
            "warm": pl.Boolean,
            "caching_observed": pl.Boolean,
            "ttl": pl.Int64,
            "expires_at": pl.Int64,
            "requests": pl.Int64,
            "misses": pl.Int64,
            "expected_rebuilds": pl.Int64,
            "hit_ratio": pl.Float64,
            "cache_write_tokens": pl.Int64,
            "miss_recache_tokens": pl.Int64,
            "last_miss_at": pl.Int64,
            "last_miss_cause": pl.Utf8,
            "recache_tokens_if_cold": pl.Int64,
        }
    ),
    "fast_mode": pl.Boolean,
    "thinking": pl.Struct({"enabled": pl.Boolean}),
    "rate_limits": pl.Struct(
        {
            "five_hour": pl.Struct(
                {"used_percentage": pl.Int64, "resets_at": pl.Int64}
            ),
            "seven_day": pl.Struct(
                {"used_percentage": pl.Int64, "resets_at": pl.Int64}
            ),
        }
    ),
}


def bronze_schema() -> dict[str, pl.DataType]:
    """The full ordered Bronze schema: the envelope and detail columns, the
    payload columns, then the full payload as JSON last."""
    return {
        **ENVELOPE_AND_DETAIL_COLUMNS,
        **PAYLOAD_COLUMNS,
        "payload": pl.Utf8,
    }


def _project(value: Any, dtype: pl.DataType, path: str) -> Any:
    """The value shaped to the declared dtype, `path` naming it for errors.

    A struct keeps only its declared fields (missing ones null), a list keeps
    its items, and an integral float where an Int64 is declared becomes that
    integer. An integer where a Float64 is declared becomes a float. A
    fractional float where an Int64 is declared, or a bool where a Float64 is,
    raises rather than being truncated or coerced.
    """
    if value is None:
        return None
    if isinstance(dtype, pl.Struct):
        if not isinstance(value, dict):
            return None
        return {
            field.name: _project(
                value.get(field.name), field.dtype, f"{path}.{field.name}"
            )
            for field in dtype.fields
        }
    if isinstance(dtype, pl.List):
        return [_project(item, dtype.inner, path) for item in value]
    if dtype == pl.Int64 and isinstance(value, float):
        if not value.is_integer():
            raise ValueError(
                f"{path} is {value!r}, not an integer, but is declared Int64"
            )
        return int(value)
    if dtype == pl.Float64:
        if isinstance(value, bool):
            raise ValueError(f"{path} is a bool, but is declared Float64")
        if isinstance(value, int):
            return float(value)
    return value


def _row(envelope: dict, source_key: str) -> dict:
    detail = envelope["detail"]
    payload = detail["payload"]
    row = {
        "id": envelope["id"],
        "time": datetime.fromisoformat(envelope["time"]),
        "account": envelope["account"],
        "region": envelope["region"],
        "detail_type": envelope["detail-type"],
        "source_file": source_key,
        "client_ip": detail.get("client_ip"),
        "request_id": detail.get("request_id"),
        "machine": detail.get("machine"),
    }
    for name, dtype in PAYLOAD_COLUMNS.items():
        row[name] = _project(payload.get(name), dtype, name)
    row["payload"] = json.dumps(payload)
    return row


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
