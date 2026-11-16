from datetime import UTC, datetime

import polars as pl

from aws_data.lib.github_bronze import bronze_schema, parse_raw_file
from tests.github_envelopes import envelope, one_line

SOURCE_KEY = "github/raw/dependabot_alert/2026/11/04/webhooks-2-2026-11-04-07-34-58-abc"


def dependabot_delivery(*, id: str, time: str, action: str, alert: dict):
    delivery = envelope(id=id, time=time, detail_type="dependabot_alert", action=action)
    payload = delivery["detail"]["payload"]
    del payload["check_run"]
    payload["alert"] = alert
    return delivery


def test_dependabot_alert_schema_is_the_common_columns_then_its_typed_columns_then_payload():
    assert list(bronze_schema("dependabot_alert").items()) == [
        ("id", pl.Utf8),
        ("time", pl.Datetime("us", "UTC")),
        ("account", pl.Utf8),
        ("region", pl.Utf8),
        ("detail_type", pl.Utf8),
        ("source_file", pl.Utf8),
        ("action", pl.Utf8),
        ("repository_id", pl.Int64),
        ("repository_name", pl.Utf8),
        ("organization_id", pl.Int64),
        ("organization_name", pl.Utf8),
        ("sender_id", pl.Int64),
        ("sender_name", pl.Utf8),
        ("alert_number", pl.Int64),
        ("alert_state", pl.Utf8),
        ("alert_package_name", pl.Utf8),
        ("alert_severity", pl.Utf8),
        ("alert_ghsa_id", pl.Utf8),
        ("alert_created_at", pl.Datetime("us", "UTC")),
        ("payload", pl.Utf8),
    ]


def test_a_withdrawn_dependabot_alert_delivery_fills_its_alert_columns():
    alert = {
        "number": 171,
        "state": "withdrawn",
        "dependency": {
            "package": {"ecosystem": "npm", "name": "lodash"},
            "manifest_path": "yarn.lock",
            "scope": "development",
            "relationship": "transitive",
        },
        "security_advisory": {
            "ghsa_id": "GHSA-f23m-r3pf-42rh",
            "cve_id": "CVE-2026-2950",
        },
        "created_at": "2026-04-02T09:09:38Z",
        "withdrawn_at": "2026-11-04T07:34:56Z",
    }
    delivery = dependabot_delivery(
        id="b0160580-50ad-6dfc-7bbf-b17b13e705e5",
        time="2026-11-04T07:34:58Z",
        action="withdrawn",
        alert=alert,
    )

    df = parse_raw_file(one_line(delivery), SOURCE_KEY, "dependabot_alert")

    row = df.row(0, named=True)
    assert row["action"] == "withdrawn"
    assert row["alert_number"] == 171
    assert row["alert_state"] == "withdrawn"
    assert row["alert_package_name"] == "lodash"
    assert row["alert_ghsa_id"] == "GHSA-f23m-r3pf-42rh"
    assert row["alert_created_at"] == datetime(
        2026, 4, 2, 9, 9, 38, tzinfo=UTC
    )


def test_a_created_dependabot_alert_delivery_is_keyed_by_its_alert_number():
    alert = {
        "number": 74,
        "state": "open",
        "dependency": {
            "package": {"ecosystem": "npm", "name": "axios"},
            "manifest_path": "package-lock.json",
            "scope": "runtime",
            "relationship": "direct",
        },
        "security_advisory": {
            "ghsa_id": "GHSA-jr5f-v2jv-69x6",
            "severity": "high",
        },
        "created_at": "2025-03-07T15:16:52Z",
    }
    delivery = dependabot_delivery(
        id="e4b3a542-f1a2-3295-1aa6-55d83321fb01",
        time="2025-03-07T15:16:53Z",
        action="created",
        alert=alert,
    )

    df = parse_raw_file(one_line(delivery), SOURCE_KEY, "dependabot_alert")

    assert df.height == 1
    row = df.row(0, named=True)
    assert row["alert_number"] == 74
    assert row["alert_state"] == "open"
    assert row["alert_package_name"] == "axios"
    assert row["alert_severity"] == "high"
    assert row["alert_ghsa_id"] == "GHSA-jr5f-v2jv-69x6"
    assert row["alert_created_at"] == datetime(
        2025, 3, 7, 15, 16, 52, tzinfo=UTC
    )


def test_a_withdrawn_dependabot_alert_delivery_reads_a_missing_severity_null():
    alert = {
        "number": 171,
        "state": "withdrawn",
        "dependency": {"package": {"ecosystem": "npm", "name": "lodash"}},
        "security_advisory": {"ghsa_id": "GHSA-f23m-r3pf-42rh"},
        "created_at": "2026-04-02T09:09:38Z",
    }
    delivery = dependabot_delivery(
        id="b0160580-50ad-6dfc-7bbf-b17b13e705e5",
        time="2026-11-04T07:34:58Z",
        action="withdrawn",
        alert=alert,
    )

    df = parse_raw_file(one_line(delivery), SOURCE_KEY, "dependabot_alert")

    assert df.height == 1
    assert df["alert_severity"].to_list() == [None]
