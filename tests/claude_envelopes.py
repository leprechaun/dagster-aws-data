"""Builders for Claude Session Status envelopes, shared by the Bronze tests."""

import json


def statusline_payload(session_id: str = "session-0001") -> dict:
    return {
        "session_id": session_id,
        "model": {"id": "model-alpha", "display_name": "Model Alpha"},
        "cost": {
            "total_cost_usd": 0.42,
            "total_duration_ms": 183000,
            "total_api_duration_ms": 9100,
            "total_lines_added": 57,
            "total_lines_removed": 12,
        },
    }


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
