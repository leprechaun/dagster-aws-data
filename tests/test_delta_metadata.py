import dagster as dg
import polars as pl
import pytest
from dagster_delta import LocalConfig
from deltalake import DeltaTable, write_deltalake

from aws_data.defs.resources import (
    DescribedDeltaLakePolarsIOManager,
    column,
    column_metadata_updates,
    sync_column_metadata,
    sync_table_description,
)

# sync_table_description(dt, description) — sets the Delta-level description,
# committing only when it changes. Runs against a local table in tmp_path.


def _table(tmp_path) -> DeltaTable:
    write_deltalake(str(tmp_path), pl.DataFrame({"a": [1]}))
    return DeltaTable(str(tmp_path))


def test_sync_table_description_sets_description(tmp_path):
    dt = _table(tmp_path)
    assert sync_table_description(dt, "hello") is True
    assert DeltaTable(str(tmp_path)).metadata().description == "hello"


def test_sync_table_description_skips_commit_when_unchanged(tmp_path):
    dt = _table(tmp_path)
    sync_table_description(dt, "hello")
    version = DeltaTable(str(tmp_path)).version()
    assert sync_table_description(DeltaTable(str(tmp_path)), "hello") is False
    assert DeltaTable(str(tmp_path)).version() == version


def test_sync_table_description_ignores_missing_description(tmp_path):
    dt = _table(tmp_path)
    assert sync_table_description(dt, None) is False
    assert DeltaTable(str(tmp_path)).version() == 0


# DescribedDeltaLakePolarsIOManager — copies each asset's description onto its
# Delta table; assets without one are left alone.


def _io_manager(tmp_path) -> DescribedDeltaLakePolarsIOManager:
    return DescribedDeltaLakePolarsIOManager(
        root_uri=str(tmp_path), storage_options=LocalConfig()
    )


def test_io_manager_syncs_asset_descriptions(tmp_path):
    @dg.asset(key_prefix=["demo"], description="described table")
    def described() -> pl.DataFrame:
        return pl.DataFrame({"a": [1]})

    @dg.asset(key_prefix=["demo"])
    def undescribed() -> pl.DataFrame:
        return pl.DataFrame({"a": [1]})

    result = dg.materialize(
        [described, undescribed], resources={"io_manager": _io_manager(tmp_path)}
    )

    assert result.success
    described_dt = DeltaTable(str(tmp_path / "demo" / "described"))
    assert described_dt.metadata().description == "described table"
    assert (
        DeltaTable(str(tmp_path / "demo" / "undescribed")).metadata().description
        is None
    )


# column(name, description, **tags) — builds a type-less TableColumn.


def test_column_carries_description_and_tags_without_a_type():
    col = column("temp", "Skin temperature", unit="celsius")
    assert (col.name, col.type, col.description, col.tags) == (
        "temp",
        "",
        "Skin temperature",
        {"unit": "celsius"},
    )


def test_column_without_tags_has_none():
    assert not column("temp").tags


# column_metadata_updates(schema, current) — pure diff of declared column
# descriptions/tags against the Delta fields' existing metadata.

_SCHEMA = dg.TableSchema(columns=[column("temp", "Skin temperature", unit="celsius")])


def test_column_metadata_updates_returns_declared_metadata():
    assert column_metadata_updates(_SCHEMA, {"temp": {}}) == {
        "temp": {"unit": "celsius", "comment": "Skin temperature"},
    }


def test_column_metadata_updates_skips_unchanged_columns():
    current = {
        "temp": {"unit": "celsius", "comment": "Skin temperature", "other": "kept"}
    }
    assert column_metadata_updates(_SCHEMA, current) == {}


def test_column_metadata_updates_detects_changed_tag():
    current = {"temp": {"unit": "fahrenheit", "comment": "Skin temperature"}}
    assert column_metadata_updates(_SCHEMA, current) == {
        "temp": {"unit": "celsius", "comment": "Skin temperature"},
    }


def test_column_metadata_updates_ignores_missing_schema():
    assert column_metadata_updates(None, {"temp": {}}) == {}


def test_column_metadata_updates_rejects_undeclared_table_column():
    with pytest.raises(ValueError, match="temp"):
        column_metadata_updates(_SCHEMA, {"other": {}})


# sync_column_metadata(dt, schema) — writes the diff onto a local table.


def test_sync_column_metadata_sets_and_then_skips(tmp_path):
    write_deltalake(str(tmp_path), pl.DataFrame({"temp": [36.5]}))
    assert sync_column_metadata(DeltaTable(str(tmp_path)), _SCHEMA) == ["temp"]
    field = DeltaTable(str(tmp_path)).schema().fields[0]
    assert dict(field.metadata) == {"unit": "celsius", "comment": "Skin temperature"}
    version = DeltaTable(str(tmp_path)).version()
    assert sync_column_metadata(DeltaTable(str(tmp_path)), _SCHEMA) == []
    assert DeltaTable(str(tmp_path)).version() == version


def test_column_metadata_survives_partition_overwrite(tmp_path):
    @dg.asset(
        key_prefix=["demo"],
        partitions_def=dg.StaticPartitionsDefinition(["a", "b"]),
        metadata={"partition_expr": "part", "dagster/column_schema": _SCHEMA},
    )
    def partitioned(context) -> pl.DataFrame:
        return pl.DataFrame({"part": [context.partition_key], "temp": [36.5]})

    resources = {"io_manager": _io_manager(tmp_path)}
    for key in ["a", "b", "a"]:
        assert dg.materialize(
            [partitioned], partition_key=key, resources=resources
        ).success

    dt = DeltaTable(str(tmp_path / "demo" / "partitioned"))
    temp = next(f for f in dt.schema().fields if f.name == "temp")
    assert dict(temp.metadata) == {"unit": "celsius", "comment": "Skin temperature"}
    # metadata written once, then preserved by later partition writes
    assert [h["operation"] for h in dt.history()].count("UPDATE FIELD METADATA") == 1


# The asset column declarations must cover exactly the columns each table has,
# or the IO manager rejects the write (or a column silently goes undescribed).


def test_column_declarations_match_the_tables_they_describe():
    from aws_data.defs.cloudfront.cloudfront_requests import (
        CLOUDFRONT_COLUMNS,
        CLOUDFRONT_SCHEMA,
    )
    from aws_data.defs.cloudtrail.cloudtrail_activity import ACTIVITY_COLUMNS
    from aws_data.defs.cloudtrail.cloudtrail_events import (
        CLOUDTRAIL_COLUMNS,
        CLOUDTRAIL_SCHEMA,
    )
    from aws_data.lib.cloudtrail_activity import RECORD_COLUMNS, activity_from_bronze

    bronze = pl.LazyFrame(schema={c: CLOUDTRAIL_SCHEMA[c] for c in RECORD_COLUMNS})
    activity_columns = activity_from_bronze(bronze).collect_schema().names()

    assert [c.name for c in CLOUDTRAIL_COLUMNS.columns] == list(CLOUDTRAIL_SCHEMA)
    assert [c.name for c in CLOUDFRONT_COLUMNS.columns] == list(CLOUDFRONT_SCHEMA)
    assert [c.name for c in ACTIVITY_COLUMNS.columns] == activity_columns
