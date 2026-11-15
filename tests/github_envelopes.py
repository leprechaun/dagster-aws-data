"""Builders for GitHub webhook envelopes, shared by the Bronze tests."""

import json


def envelope(
    *,
    id: str = "60201608-a89e-7b82-3f96-90a129cbf3b0",
    time: str = "2026-09-08T17:17:42Z",
    action: str = "created",
    check_run: dict | None = None,
) -> dict:
    return {
        "version": "0",
        "id": id,
        "detail-type": "check_run",
        "source": "github",
        "account": "307985306317",
        "time": time,
        "region": "ap-southeast-1",
        "resources": [],
        "detail": {
            "client_ip": "140.82.115.245",
            "request_id": "48e0f59b-838e-491d-b18a-3cf0626a944a",
            "payload": {
                "action": action,
                "check_run": check_run
                if check_run is not None
                else {
                    "id": 102162572568,
                    "status": "queued",
                    "conclusion": None,
                    "name": "deploy-prod",
                },
                "repository": {"id": 1234, "full_name": "snacker-tracker/reporter-serverless"},
                "organization": {"id": 5678, "login": "snacker-tracker"},
                "sender": {"id": 91011, "login": "octocat"},
            },
        },
    }


def one_line(envelope_: dict) -> bytes:
    return (json.dumps(envelope_) + "\n").encode()
