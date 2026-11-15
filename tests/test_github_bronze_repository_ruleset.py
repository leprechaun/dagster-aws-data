import polars as pl

from aws_data.lib.github_bronze import bronze_schema, parse_raw_file
from tests.github_envelopes import envelope, one_line

SOURCE_KEY = (
    "github/raw/repository_ruleset/2026/08/07/webhooks-2-2026-08-07-12-59-35-abc"
)


def ruleset_delivery(repository_ruleset: dict | None, action: str = "edited"):
    delivery = envelope(id="ruleset-1", detail_type="repository_ruleset", action=action)
    payload = delivery["detail"]["payload"]
    del payload["check_run"]
    if repository_ruleset is not None:
        payload["repository_ruleset"] = repository_ruleset
    return delivery


def test_repository_ruleset_schema_is_the_common_columns_then_its_typed_columns_then_payload():
    assert list(bronze_schema("repository_ruleset").items()) == [
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
        ("repository_ruleset_id", pl.Int64),
        ("repository_ruleset_name", pl.Utf8),
        ("repository_ruleset_target", pl.Utf8),
        ("repository_ruleset_enforcement", pl.Utf8),
        ("payload", pl.Utf8),
    ]


def test_a_repository_ruleset_delivery_fills_its_typed_columns_from_the_payload():
    delivery = ruleset_delivery(
        {
            "id": 19442619,
            "name": "Code Quality Copilot review for default branch",
            "target": "branch",
            "enforcement": "disabled",
        }
    )

    df = parse_raw_file(one_line(delivery), SOURCE_KEY, "repository_ruleset")

    row = df.row(0, named=True)
    assert row["id"] == "ruleset-1"
    assert row["detail_type"] == "repository_ruleset"
    assert row["action"] == "edited"
    assert row["repository_ruleset_id"] == 19442619
    assert row["repository_ruleset_name"] == (
        "Code Quality Copilot review for default branch"
    )
    assert row["repository_ruleset_target"] == "branch"
    assert row["repository_ruleset_enforcement"] == "disabled"


def test_a_repository_ruleset_field_missing_from_a_delivery_reads_null_and_is_kept():
    delivery = ruleset_delivery({"id": 19442619, "target": "branch"})

    df = parse_raw_file(one_line(delivery), SOURCE_KEY, "repository_ruleset")

    assert df.height == 1
    row = df.row(0, named=True)
    assert row["repository_ruleset_id"] == 19442619
    assert row["repository_ruleset_target"] == "branch"
    assert row["repository_ruleset_name"] is None
    assert row["repository_ruleset_enforcement"] is None


def test_a_repository_ruleset_delivery_with_no_ruleset_key_reads_its_columns_null_and_is_kept():
    delivery = ruleset_delivery(None)

    df = parse_raw_file(one_line(delivery), SOURCE_KEY, "repository_ruleset")

    assert df.height == 1
    row = df.row(0, named=True)
    for column in [
        name for name in df.columns if name.startswith("repository_ruleset_")
    ]:
        assert row[column] is None, column
