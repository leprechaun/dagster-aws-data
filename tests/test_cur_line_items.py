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


def test_a_billing_period_with_nothing_in_raw_fails_the_run_and_leaves_the_billing_period_as_it_was(tmp_path):
    raw = FakeS3Client(objects={raw_file("2026-11"): parquet(line_item_usage_account_id=["kept"])})
    assert build(raw, tmp_path).success

    result = build(FakeS3Client(), tmp_path)

    assert not result.success
    assert bronze(tmp_path)["line_item_usage_account_id"].to_list() == ["kept"]


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


def test_resource_tags_delivered_as_a_parquet_map_are_kept_as_key_value_pairs(tmp_path):
    buffer = io.BytesIO()
    pq.write_table(
        pa.table(
            {
                "line_item_usage_account_id": ["1"],
                "resource_tags": pa.array([[("team", "data"), ("env", "prod")]], type=pa.map_(pa.string(), pa.string())),
            }
        ),
        buffer,
    )
    raw = FakeS3Client(objects={raw_file("2026-11"): buffer.getvalue()})

    assert build(raw, tmp_path).success

    assert bronze(tmp_path)["resource_tags"].to_list() == [[{"key": "team", "value": "data"}, {"key": "env", "value": "prod"}]]


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
