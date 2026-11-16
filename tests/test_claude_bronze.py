import json
from datetime import UTC, datetime

import polars as pl
import pytest

from aws_data.lib.claude_bronze import bronze_schema, parse_raw_file, raw_prefix
from tests.claude_envelopes import (
    envelope,
    minimal_payload,
    one_line,
    statusline_payload,
)

SOURCE_KEY = (
    "claude/raw/session-status/2026/11/13/statusline-1-2026-11-13-10-32-19-6738f2f0"
)


def test_a_one_line_file_becomes_one_row_with_the_envelope_and_detail_columns():
    df = parse_raw_file(one_line(envelope()), SOURCE_KEY)

    assert df.height == 1
    row = df.row(0, named=True)
    assert row["id"] == "3f1c2b9e-7d4a-4e8b-9a61-0c2d5e7f8a90"
    assert row["time"] == datetime(2026, 11, 13, 10, 32, 19, tzinfo=UTC)
    assert row["account"] == "123456789012"
    assert row["region"] == "ap-southeast-1"
    assert row["detail_type"] == "session-status"
    assert row["source_file"] == SOURCE_KEY
    assert row["client_ip"] == "203.0.113.7"
    assert row["request_id"] == "9b8a7c6d-5e4f-4321-8765-1a2b3c4d5e6f"
    assert row["machine"] == "laptop"
    assert json.loads(row["payload"])["session_id"] == "session-0001"


def test_the_payload_round_trips_to_the_delivered_payload():
    delivery = envelope()

    df = parse_raw_file(one_line(delivery), SOURCE_KEY)

    assert json.loads(df.row(0, named=True)["payload"]) == delivery["detail"]["payload"]


def test_a_multi_line_file_gives_one_row_per_envelope_and_ignores_blank_lines():
    body = (
        one_line(envelope(id="first"))
        + b"\n   \n"
        + one_line(envelope(id="second", time="2026-11-13T11:00:00Z"))
    )

    df = parse_raw_file(body, SOURCE_KEY)

    assert df["id"].to_list() == ["first", "second"]


def test_a_test_delivery_with_the_minimal_payload_is_kept_like_any_other_row():
    test_delivery = envelope(
        id="test-delivery",
        machine=None,
        payload={"session_id": "statusline-test"},
    )

    df = parse_raw_file(one_line(test_delivery), SOURCE_KEY)

    row = df.row(0, named=True)
    assert row["id"] == "test-delivery"
    assert row["machine"] is None
    assert json.loads(row["payload"]) == {"session_id": "statusline-test"}


def test_the_schema_is_the_envelope_then_detail_then_payload_columns_then_payload():
    assert list(bronze_schema().items()) == [
        ("id", pl.Utf8),
        ("time", pl.Datetime("us", "UTC")),
        ("account", pl.Utf8),
        ("region", pl.Utf8),
        ("detail_type", pl.Utf8),
        ("source_file", pl.Utf8),
        ("client_ip", pl.Utf8),
        ("request_id", pl.Utf8),
        ("machine", pl.Utf8),
        ("session_id", pl.Utf8),
        ("session_name", pl.Utf8),
        ("prompt_id", pl.Utf8),
        ("transcript_path", pl.Utf8),
        ("cwd", pl.Utf8),
        ("scratchpad_dir", pl.Utf8),
        ("version", pl.Utf8),
        ("model", pl.Struct({"id": pl.Utf8, "display_name": pl.Utf8})),
        (
            "workspace",
            pl.Struct(
                {
                    "current_dir": pl.Utf8,
                    "project_dir": pl.Utf8,
                    "added_dirs": pl.List(pl.Utf8),
                    "repo": pl.Struct(
                        {"host": pl.Utf8, "owner": pl.Utf8, "name": pl.Utf8}
                    ),
                }
            ),
        ),
        ("effort", pl.Struct({"level": pl.Utf8})),
        ("output_style", pl.Struct({"name": pl.Utf8})),
        (
            "cost",
            pl.Struct(
                {
                    "total_cost_usd": pl.Float64,
                    "total_duration_ms": pl.Int64,
                    "total_api_duration_ms": pl.Int64,
                    "total_lines_added": pl.Int64,
                    "total_lines_removed": pl.Int64,
                }
            ),
        ),
        (
            "context_window",
            pl.Struct(
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
        ),
        ("exceeds_200k_tokens", pl.Boolean),
        (
            "prompt_cache",
            pl.Struct(
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
        ),
        ("fast_mode", pl.Boolean),
        ("thinking", pl.Struct({"enabled": pl.Boolean})),
        (
            "rate_limits",
            pl.Struct(
                {
                    "five_hour": pl.Struct(
                        {"used_percentage": pl.Int64, "resets_at": pl.Int64}
                    ),
                    "seven_day": pl.Struct(
                        {"used_percentage": pl.Int64, "resets_at": pl.Int64}
                    ),
                }
            ),
        ),
        ("payload", pl.Utf8),
    ]


def test_the_dataframe_takes_the_declared_schema():
    df = parse_raw_file(one_line(envelope()), SOURCE_KEY)

    assert df.schema == bronze_schema()


def test_a_full_payload_fills_every_column_with_its_delivered_values():
    row = parse_raw_file(one_line(envelope()), SOURCE_KEY).row(0, named=True)

    assert row["session_id"] == "session-0001"
    assert row["session_name"] == "bench-refactor"
    assert row["prompt_id"] == "prompt-0007"
    assert row["transcript_path"] == "/home/user-a/transcripts/session-0001.jsonl"
    assert row["cwd"] == "/home/user-a/projects/sample-app"
    assert row["scratchpad_dir"] == "/home/user-a/scratch/session-0001"
    assert row["version"] == "1.2.3"
    assert row["exceeds_200k_tokens"] is False
    assert row["fast_mode"] is False
    assert row["model"] == {"id": "model-alpha", "display_name": "Model Alpha"}
    assert row["workspace"] == {
        "current_dir": "/home/user-a/projects/sample-app",
        "project_dir": "/home/user-a/projects/sample-app",
        "added_dirs": ["/home/user-a/shared"],
        "repo": {"host": "github.example", "owner": "sample-org", "name": "sample-app"},
    }
    assert row["effort"] == {"level": "medium"}
    assert row["output_style"] == {"name": "default"}
    assert row["cost"] == {
        "total_cost_usd": 0.42,
        "total_duration_ms": 183000,
        "total_api_duration_ms": 9100,
        "total_lines_added": 57,
        "total_lines_removed": 12,
    }
    assert row["context_window"] == {
        "total_input_tokens": 15234,
        "total_output_tokens": 4312,
        "context_window_size": 200000,
        "current_usage": {
            "input_tokens": 8500,
            "output_tokens": 1200,
            "cache_creation_input_tokens": 300,
            "cache_read_input_tokens": 6000,
        },
        "used_percentage": 8,
        "remaining_percentage": 92,
    }
    assert row["prompt_cache"] == {
        "warm": True,
        "caching_observed": True,
        "ttl": 300,
        "expires_at": 1763029939,
        "requests": 12,
        "misses": 1,
        "expected_rebuilds": 1,
        "hit_ratio": 0.92,
        "cache_write_tokens": 4000,
        "miss_recache_tokens": 900,
        "last_miss_at": 1763029000,
        "last_miss_cause": "ttl_expired",
        "recache_tokens_if_cold": 5200,
    }
    assert row["thinking"] == {"enabled": True}
    assert row["rate_limits"] == {
        "five_hour": {"used_percentage": 23, "resets_at": 1763040000},
        "seven_day": {"used_percentage": 41, "resets_at": 1763500000},
    }


def test_a_fractional_float_where_an_integer_is_declared_raises_naming_the_field():
    payload = statusline_payload()
    payload["rate_limits"]["five_hour"]["used_percentage"] = 12.5

    with pytest.raises(ValueError, match="rate_limits.five_hour.used_percentage"):
        parse_raw_file(one_line(envelope(payload=payload)), SOURCE_KEY)


def test_an_integral_float_where_an_integer_is_declared_is_read_as_that_integer():
    payload = statusline_payload()
    payload["rate_limits"]["five_hour"]["used_percentage"] = 12.0

    row = parse_raw_file(one_line(envelope(payload=payload)), SOURCE_KEY).row(
        0, named=True
    )

    assert row["rate_limits"]["five_hour"]["used_percentage"] == 12
    assert isinstance(row["rate_limits"]["five_hour"]["used_percentage"], int)


def test_a_bool_where_a_float_is_declared_raises_rather_than_becoming_a_float():
    payload = statusline_payload()
    payload["cost"]["total_cost_usd"] = True

    with pytest.raises(ValueError, match="cost.total_cost_usd"):
        parse_raw_file(one_line(envelope(payload=payload)), SOURCE_KEY)


def test_an_integer_total_cost_comes_out_as_a_float():
    payload = statusline_payload()
    payload["cost"]["total_cost_usd"] = 0

    df = parse_raw_file(one_line(envelope(payload=payload)), SOURCE_KEY)

    cost = df.row(0, named=True)["cost"]
    assert cost["total_cost_usd"] == 0.0
    assert isinstance(cost["total_cost_usd"], float)
    assert df.schema["cost"] == pl.Struct(
        {
            "total_cost_usd": pl.Float64,
            "total_duration_ms": pl.Int64,
            "total_api_duration_ms": pl.Int64,
            "total_lines_added": pl.Int64,
            "total_lines_removed": pl.Int64,
        }
    )


def test_blocks_and_keys_missing_from_a_payload_read_null():
    payload = statusline_payload()
    del payload["prompt_cache"]
    del payload["rate_limits"]
    del payload["session_name"]
    del payload["prompt_id"]
    del payload["workspace"]["repo"]
    payload["context_window"]["current_usage"] = None

    row = parse_raw_file(one_line(envelope(payload=payload)), SOURCE_KEY).row(
        0, named=True
    )

    assert row["prompt_cache"] is None
    assert row["rate_limits"] is None
    assert row["session_name"] is None
    assert row["prompt_id"] is None
    assert row["workspace"]["repo"] is None
    assert row["workspace"]["project_dir"] == "/home/user-a/projects/sample-app"
    assert row["context_window"]["current_usage"] is None
    assert row["context_window"]["used_percentage"] == 8


def test_a_minimal_test_delivery_reads_the_missing_blocks_as_null():
    df = parse_raw_file(one_line(envelope(payload=minimal_payload())), SOURCE_KEY)

    row = df.row(0, named=True)
    assert row["session_id"] == "statusline-test"
    assert row["cost"] is None
    assert row["context_window"] is None
    assert row["effort"] is None


def test_epoch_fields_stay_unix_seconds_as_delivered():
    row = parse_raw_file(one_line(envelope()), SOURCE_KEY).row(0, named=True)

    assert row["prompt_cache"]["expires_at"] == 1763029939
    assert row["prompt_cache"]["last_miss_at"] == 1763029000
    assert row["rate_limits"]["five_hour"]["resets_at"] == 1763040000
    assert row["rate_limits"]["seven_day"]["resets_at"] == 1763500000


def test_miss_causes_is_not_a_column_but_is_kept_in_the_payload():
    df = parse_raw_file(one_line(envelope()), SOURCE_KEY)

    assert "miss_causes" not in df.columns
    assert "miss_causes" not in [f.name for f in df.schema["prompt_cache"].fields]
    assert (
        json.loads(df.row(0, named=True)["payload"])["prompt_cache"]["miss_causes"]
        == {}
    )


def test_an_undeclared_key_is_dropped_from_the_columns_and_kept_in_the_payload():
    payload = statusline_payload()
    payload["some_future_key"] = {"anything": 1}
    payload["model"]["some_future_field"] = "kept-in-payload-only"

    df = parse_raw_file(one_line(envelope(payload=payload)), SOURCE_KEY)

    row = df.row(0, named=True)
    assert row["model"] == {"id": "model-alpha", "display_name": "Model Alpha"}
    assert "some_future_key" not in df.columns
    delivered = json.loads(row["payload"])
    assert delivered["some_future_key"] == {"anything": 1}
    assert delivered["model"]["some_future_field"] == "kept-in-payload-only"


def test_the_raw_prefix_is_the_session_status_folder_in_claude_raw():
    assert raw_prefix() == "claude/raw/session-status/"
