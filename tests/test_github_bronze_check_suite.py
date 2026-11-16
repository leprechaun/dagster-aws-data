from datetime import UTC, datetime

import polars as pl

from aws_data.lib.github_bronze import bronze_schema, parse_raw_file
from tests.github_envelopes import envelope, one_line

SOURCE_KEY = "github/raw/check_suite/2026/08/08/webhooks-2-2026-08-08-10-32-19-abc"


def _check_suite_delivery(
    check_suite: dict, *, id: str = "5dabbc2c-2851-453d-78e1-274befb671b0"
):
    delivery = envelope(
        id=id,
        time="2026-08-08T10:32:19Z",
        detail_type="check_suite",
        action="completed",
    )
    payload = delivery["detail"]["payload"]
    del payload["check_run"]
    payload["check_suite"] = check_suite
    return delivery


def test_check_suite_schema_is_the_common_columns_then_its_typed_columns_then_payload():
    schema = bronze_schema("check_suite")

    assert list(schema.items())[-8:] == [
        ("check_suite_id", pl.Int64),
        ("check_suite_head_branch", pl.Utf8),
        ("check_suite_head_sha", pl.Utf8),
        ("check_suite_status", pl.Utf8),
        ("check_suite_conclusion", pl.Utf8),
        ("check_suite_created_at", pl.Datetime("us", "UTC")),
        ("check_suite_updated_at", pl.Datetime("us", "UTC")),
        ("payload", pl.Utf8),
    ]


def test_a_check_suite_delivery_fills_its_typed_columns_from_the_payload():
    check_suite = {
        "id": 84802647620,
        "node_id": "CS_kwDOKUTPHs8AAAATvqG2RA",
        "head_branch": "master",
        "head_sha": "0b43ae61866d4b9a7e74a02fbee4e8099f0b5c04",
        "status": "completed",
        "conclusion": "success",
        "created_at": "2026-08-08T10:31:35Z",
        "updated_at": "2026-08-08T10:32:18Z",
    }

    df = parse_raw_file(
        one_line(_check_suite_delivery(check_suite)), SOURCE_KEY, "check_suite"
    )

    row = df.row(0, named=True)
    assert row["check_suite_id"] == 84802647620
    assert row["check_suite_head_branch"] == "master"
    assert row["check_suite_head_sha"] == "0b43ae61866d4b9a7e74a02fbee4e8099f0b5c04"
    assert row["check_suite_status"] == "completed"
    assert row["check_suite_conclusion"] == "success"
    assert row["check_suite_created_at"] == datetime(
        2026, 8, 8, 10, 31, 35, tzinfo=UTC
    )
    assert row["check_suite_updated_at"] == datetime(
        2026, 8, 8, 10, 32, 18, tzinfo=UTC
    )


def test_a_check_suite_still_running_has_no_conclusion_and_is_kept():
    check_suite = {
        "id": 84802647620,
        "head_branch": "master",
        "head_sha": "0b43ae61866d4b9a7e74a02fbee4e8099f0b5c04",
        "status": "in_progress",
        "created_at": "2026-08-08T10:31:35Z",
        "updated_at": "2026-08-08T10:31:40Z",
    }

    df = parse_raw_file(
        one_line(_check_suite_delivery(check_suite)), SOURCE_KEY, "check_suite"
    )

    assert df.height == 1
    row = df.row(0, named=True)
    assert row["check_suite_status"] == "in_progress"
    assert row["check_suite_conclusion"] is None
    assert row["check_suite_updated_at"] == datetime(
        2026, 8, 8, 10, 31, 40, tzinfo=UTC
    )


def test_a_check_suite_delivery_with_no_check_suite_key_reads_every_check_suite_column_null():
    delivery = envelope(detail_type="check_suite", action="completed")
    del delivery["detail"]["payload"]["check_run"]

    df = parse_raw_file(one_line(delivery), SOURCE_KEY, "check_suite")

    assert df.height == 1
    row = df.row(0, named=True)
    for column in [name for name in df.columns if name.startswith("check_suite_")]:
        assert row[column] is None, column


def test_a_malformed_check_suite_timestamp_reads_null_and_keeps_the_row():
    check_suite = {
        "id": 84802647620,
        "head_branch": "master",
        "head_sha": "0b43ae61866d4b9a7e74a02fbee4e8099f0b5c04",
        "status": "queued",
        "created_at": "",
    }

    df = parse_raw_file(
        one_line(_check_suite_delivery(check_suite, id="bad-time")),
        SOURCE_KEY,
        "check_suite",
    )

    assert df["id"].to_list() == ["bad-time"]
    assert df["check_suite_created_at"].to_list() == [None]
    assert df["check_suite_status"].to_list() == ["queued"]
