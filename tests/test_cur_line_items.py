import io
from datetime import date, datetime, timezone

import dagster as dg
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from dagster._core.test_utils import freeze_time
from dagster_delta import DeltaLakePolarsIOManager, LocalConfig
from deltalake import DeltaTable

from aws_data.defs.cur.cur_line_items import clean_line_items, cur_line_items
from aws_data.defs.resources import DescribedDeltaLakePolarsIOManager
from tests.conftest import AFTER_THE_DAYS_UNDER_TEST, FakeS3Client, FakeS3Resource

EXPORT = "cur-reports/with-resource-ids-split-hourly-parquet-overwrite"
RAW = f"aws-data/raw/cur/{EXPORT}/data"


def parquet(**columns) -> bytes:
    """A delivered CUR file: one Parquet file with a column per CUR field."""
    buffer = io.BytesIO()
    pl.DataFrame(columns).write_parquet(buffer)
    return buffer.getvalue()


def raw_file(billing_period: str, name: str = "part-00001.snappy.parquet") -> str:
    return f"{RAW}/BILLING_PERIOD={billing_period}/{name}"


def build(raw: FakeS3Client, tmp_path, partition_key: str = "2026-11-01") -> dg.ExecuteInProcessResult:
    """Runs the asset; a failed run is returned rather than raised."""
    with freeze_time(AFTER_THE_DAYS_UNDER_TEST):
        return dg.materialize(
            [cur_line_items],
            partition_key=partition_key,
            raise_on_error=False,
            resources={
                "minio_s3": FakeS3Resource(raw),
                "io_manager": DeltaLakePolarsIOManager(root_uri=str(tmp_path), storage_options=LocalConfig()),
            },
        )


def bronze(tmp_path) -> pl.DataFrame:
    return pl.read_delta(str(tmp_path / "bronze" / "cur_line_items"))


def test_a_billing_periods_line_items_are_written_to_bronze_with_their_billing_period(tmp_path):
    raw = FakeS3Client(
        objects={
            raw_file("2026-11"): parquet(
                line_item_usage_account_id=["111111111111", "222222222222"],
                line_item_unblended_cost=[1.5, 2.5],
            )
        }
    )

    assert build(raw, tmp_path).success

    table = bronze(tmp_path).sort("line_item_usage_account_id")
    assert table["line_item_usage_account_id"].to_list() == ["111111111111", "222222222222"]
    assert table["line_item_unblended_cost"].to_list() == [1.5, 2.5]
    assert table["billing_period"].to_list() == [date(2026, 11, 1), date(2026, 11, 1)]
    assert table["ingested_at"].null_count() == 0


def reported(result: dg.ExecuteInProcessResult) -> dict:
    """What the run reported about itself, as plain values."""
    [materialization] = result.asset_materializations_for_node("aws_data__bronze__cur_line_items")
    return {name: value.value for name, value in materialization.metadata.items() if name in COUNTS}


COUNTS = {"files_read", "line_items_read"}


def test_building_a_billing_period_again_replaces_it_without_changing_the_row_count(tmp_path):
    raw = FakeS3Client(objects={raw_file("2026-11"): parquet(line_item_usage_account_id=["1", "2", "3"])})
    assert build(raw, tmp_path).success

    assert build(raw, tmp_path).success

    assert bronze(tmp_path).height == 3


def test_a_restated_billing_period_replaces_the_rows_it_had_and_leaves_other_billing_periods_alone(tmp_path):
    raw = FakeS3Client(
        objects={
            raw_file("2026-10"): parquet(line_item_usage_account_id=["october"]),
            raw_file("2026-11"): parquet(line_item_usage_account_id=["first", "delivery"]),
        }
    )
    assert build(raw, tmp_path, "2026-10-01").success
    assert build(raw, tmp_path, "2026-11-01").success
    restated = FakeS3Client(
        objects={raw_file("2026-10"): b"unused", raw_file("2026-11"): parquet(line_item_usage_account_id=["restated"])}
    )

    assert build(restated, tmp_path, "2026-11-01").success

    table = bronze(tmp_path).sort("billing_period", "line_item_usage_account_id")
    assert table.select("billing_period", "line_item_usage_account_id").rows() == [
        (date(2026, 10, 1), "october"),
        (date(2026, 11, 1), "restated"),
    ]


def test_a_column_that_first_appears_in_a_later_billing_period_is_accepted_and_null_before(tmp_path):
    raw = FakeS3Client(
        objects={
            raw_file("2026-10"): parquet(line_item_usage_account_id=["old"]),
            raw_file("2026-11"): parquet(line_item_usage_account_id=["new"], savings_plan_effective_cost=[0.25]),
        }
    )

    assert build(raw, tmp_path, "2026-10-01").success
    assert build(raw, tmp_path, "2026-11-01").success

    table = bronze(tmp_path).sort("billing_period")
    assert table["savings_plan_effective_cost"].to_list() == [None, 0.25]


def test_files_of_one_billing_period_are_combined_even_when_their_columns_differ(tmp_path):
    raw = FakeS3Client(
        objects={
            raw_file("2026-11", "part-00001.snappy.parquet"): parquet(line_item_usage_account_id=["a"], extra=["x"]),
            raw_file("2026-11", "part-00002.snappy.parquet"): parquet(line_item_usage_account_id=["b"]),
        }
    )

    result = build(raw, tmp_path)

    assert result.success
    assert bronze(tmp_path).sort("line_item_usage_account_id")["extra"].to_list() == ["x", None]
    assert reported(result) == {"files_read": 2, "line_items_read": 2}


def test_an_unreadable_file_fails_the_run_and_leaves_the_billing_period_as_it_was(tmp_path):
    good = {raw_file("2026-11", "part-00001.snappy.parquet"): parquet(line_item_usage_account_id=["kept"])}
    assert build(FakeS3Client(objects=good), tmp_path).success
    damaged = {
        **good,
        raw_file("2026-11", "part-00002.snappy.parquet"): b"this is not parquet",
    }

    result = build(FakeS3Client(objects=damaged), tmp_path)

    assert not result.success
    assert bronze(tmp_path)["line_item_usage_account_id"].to_list() == ["kept"]


def test_a_billing_period_with_nothing_in_raw_yet_succeeds_without_writing_and_leaves_it_as_it_was(tmp_path):
    raw = FakeS3Client(objects={raw_file("2026-11"): parquet(line_item_usage_account_id=["kept"])})
    assert build(raw, tmp_path).success

    result = build(FakeS3Client(), tmp_path)

    assert result.success
    assert result.asset_materializations_for_node("aws_data__bronze__cur_line_items") == []
    assert bronze(tmp_path)["line_item_usage_account_id"].to_list() == ["kept"]


def test_a_billing_period_not_delivered_yet_creates_no_table(tmp_path):
    result = build(FakeS3Client(), tmp_path)

    assert result.success
    assert not (tmp_path / "bronze" / "cur_line_items").exists()


def test_clean_line_items_keeps_every_delivered_column_and_adds_the_billing_period_and_ingestion_time():
    delivered = pl.DataFrame({"line_item_usage_account_id": ["1"], "line_item_unblended_cost": [1.5]})

    cleaned = clean_line_items(delivered, date(2026, 11, 1), datetime(2026, 11, 14, 12, 0, tzinfo=timezone.utc))

    assert cleaned.columns == ["line_item_usage_account_id", "line_item_unblended_cost", "billing_period", "ingested_at"]
    assert cleaned.row(0, named=True) == {
        "line_item_usage_account_id": "1",
        "line_item_unblended_cost": 1.5,
        "billing_period": date(2026, 11, 1),
        "ingested_at": datetime(2026, 11, 14, 12, 0, tzinfo=timezone.utc),
    }


def parquet_with_map(account_ids: list[str], resource_tags: list[list[tuple[str, str]] | None]) -> bytes:
    """A delivered CUR file whose resource_tags is a Parquet MAP<string,string>, as AWS writes it."""
    buffer = io.BytesIO()
    pq.write_table(
        pa.table(
            {
                "line_item_usage_account_id": account_ids,
                "resource_tags": pa.array(resource_tags, type=pa.map_(pa.string(), pa.string())),
            }
        ),
        buffer,
    )
    return buffer.getvalue()


def bronze_type_name(tmp_path, column: str) -> str:
    """The Delta type name of a column in the Bronze table."""
    fields = {f.name: f.type for f in DeltaTable(str(tmp_path / "bronze" / "cur_line_items")).schema().fields}
    return fields[column].type


def test_resource_tags_delivered_as_a_parquet_map_stay_maps_in_bronze(tmp_path):
    raw = FakeS3Client(objects={raw_file("2026-11"): parquet_with_map(["1"], [[("team", "data"), ("env", "prod")]])})

    assert build(raw, tmp_path).success

    assert bronze_type_name(tmp_path, "resource_tags") == "map"
    table = pl.read_delta(str(tmp_path / "bronze" / "cur_line_items"))
    assert table.schema["resource_tags"] == pl.Map(pl.String, pl.String)
    assert table["resource_tags"].to_list() == [{"team": "data", "env": "prod"}]


def test_a_map_column_can_be_looked_up_by_key_in_bronze_and_is_null_for_a_key_it_lacks(tmp_path):
    raw = FakeS3Client(
        objects={raw_file("2026-11"): parquet_with_map(["tagged", "other-team"], [[("team", "data")], [("env", "dev")]])}
    )
    assert build(raw, tmp_path).success

    looked_up = bronze(tmp_path).sort("line_item_usage_account_id").select(pl.col("resource_tags").map.get("team"))

    assert looked_up["resource_tags"].to_list() == [None, "data"]


def test_a_map_column_missing_from_one_file_of_a_billing_period_is_null_for_that_files_rows(tmp_path):
    raw = FakeS3Client(
        objects={
            raw_file("2026-11", "part-00001.snappy.parquet"): parquet_with_map(["mapped"], [[("team", "data")]]),
            raw_file("2026-11", "part-00002.snappy.parquet"): parquet(line_item_usage_account_id=["unmapped"]),
        }
    )

    result = build(raw, tmp_path)

    assert result.success
    assert bronze_type_name(tmp_path, "resource_tags") == "map"
    table = bronze(tmp_path).sort("line_item_usage_account_id")
    assert table["resource_tags"].to_list() == [{"team": "data"}, None]


def test_a_map_column_first_delivered_in_a_later_billing_period_is_a_map_and_null_before_it(tmp_path):
    raw = FakeS3Client(
        objects={
            raw_file("2026-10"): parquet(line_item_usage_account_id=["old"]),
            raw_file("2026-11"): parquet_with_map(["new"], [[("team", "data")]]),
        }
    )
    assert build(raw, tmp_path, "2026-10-01").success

    assert build(raw, tmp_path, "2026-11-01").success

    assert bronze_type_name(tmp_path, "resource_tags") == "map"
    table = bronze(tmp_path).sort("billing_period")
    assert table["resource_tags"].to_list() == [None, {"team": "data"}]


def test_column_comments_are_still_written_when_the_table_has_a_map_column(tmp_path):
    raw = FakeS3Client(objects={raw_file("2026-11"): parquet_with_map(["1"], [[("team", "data")]])})

    with freeze_time(AFTER_THE_DAYS_UNDER_TEST):
        assert dg.materialize(
            [cur_line_items],
            partition_key="2026-11-01",
            resources={
                "minio_s3": FakeS3Resource(raw),
                "io_manager": DescribedDeltaLakePolarsIOManager(root_uri=str(tmp_path), storage_options=LocalConfig()),
            },
        ).success

    fields = {f.name: dict(f.metadata) for f in DeltaTable(str(tmp_path / "bronze" / "cur_line_items")).schema().fields}
    assert bronze_type_name(tmp_path, "resource_tags") == "map"
    assert "Billing Period" in fields["billing_period"]["comment"]
    assert "parsed into Bronze" in fields["ingested_at"]["comment"]


def test_the_columns_bronze_adds_are_described_on_the_delta_table(tmp_path):
    raw = FakeS3Client(objects={raw_file("2026-11"): parquet(line_item_usage_account_id=["1"])})

    with freeze_time(AFTER_THE_DAYS_UNDER_TEST):
        assert dg.materialize(
            [cur_line_items],
            partition_key="2026-11-01",
            resources={
                "minio_s3": FakeS3Resource(raw),
                "io_manager": DescribedDeltaLakePolarsIOManager(root_uri=str(tmp_path), storage_options=LocalConfig()),
            },
        ).success

    fields = {f.name: dict(f.metadata) for f in DeltaTable(str(tmp_path / "bronze" / "cur_line_items")).schema().fields}
    assert "Billing Period" in fields["billing_period"]["comment"]
    assert "parsed into Bronze" in fields["ingested_at"]["comment"]
