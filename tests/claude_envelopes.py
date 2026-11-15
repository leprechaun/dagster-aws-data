"""Builders for Claude Session Status envelopes, shared by the Bronze tests."""

import json


def statusline_payload(session_id: str = "session-0001") -> dict:
    """A full Claude Session Status payload, with neutral values."""
    return {
        "session_id": session_id,
        "session_name": "bench-refactor",
        "prompt_id": "prompt-0007",
        "transcript_path": "/home/user-a/transcripts/session-0001.jsonl",
        "cwd": "/home/user-a/projects/sample-app",
        "scratchpad_dir": "/home/user-a/scratch/session-0001",
        "version": "1.2.3",
        "model": {"id": "model-alpha", "display_name": "Model Alpha"},
        "workspace": {
            "current_dir": "/home/user-a/projects/sample-app",
            "project_dir": "/home/user-a/projects/sample-app",
            "added_dirs": ["/home/user-a/shared"],
            "repo": {
                "host": "github.example",
                "owner": "sample-org",
                "name": "sample-app",
            },
        },
        "effort": {"level": "medium"},
        "output_style": {"name": "default"},
        "cost": {
            "total_cost_usd": 0.42,
            "total_duration_ms": 183000,
            "total_api_duration_ms": 9100,
            "total_lines_added": 57,
            "total_lines_removed": 12,
        },
        "context_window": {
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
        },
        "exceeds_200k_tokens": False,
        "prompt_cache": {
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
            "miss_causes": {},
        },
        "fast_mode": False,
        "thinking": {"enabled": True},
        "rate_limits": {
            "five_hour": {"used_percentage": 23, "resets_at": 1763040000},
            "seven_day": {"used_percentage": 41, "resets_at": 1763500000},
        },
    }


def minimal_payload() -> dict:
    """The shape of a test delivery: no cost, context window or effort."""
    return {"session_id": "statusline-test"}


def envelope(
    *,
    id: str = "3f1c2b9e-7d4a-4e8b-9a61-0c2d5e7f8a90",
    time: str = "2026-11-13T10:32:19Z",
    account: str = "123456789012",
    region: str = "ap-southeast-1",
    client_ip: str = "203.0.113.7",
    request_id: str = "9b8a7c6d-5e4f-4321-8765-1a2b3c4d5e6f",
    machine: str | None = "laptop",
    payload: dict | None = None,
) -> dict:
    detail = {
        "client_ip": client_ip,
        "request_id": request_id,
        "payload": payload if payload is not None else statusline_payload(),
    }
    if machine is not None:
        detail["machine"] = machine
    return {
        "version": "0",
        "id": id,
        "detail-type": "session-status",
        "source": "claude",
        "account": account,
        "time": time,
        "region": region,
        "resources": [],
        "detail": detail,
    }


def one_line(envelope_: dict) -> bytes:
    return (json.dumps(envelope_) + "\n").encode()
