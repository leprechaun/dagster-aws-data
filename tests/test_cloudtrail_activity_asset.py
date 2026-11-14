from datetime import date, datetime, timezone

import dagster as dg
import polars as pl
from dagster._core.test_utils import freeze_time
from dagster_delta import DeltaLakePolarsIOManager, LocalConfig
from deltalake import DeltaTable

from aws_data.defs.cloudtrail.cloudtrail_activity import cloudtrail_activity
from aws_data.defs.cloudtrail.cloudtrail_events import cloudtrail_events
from aws_data.lib.cloudtrail_activity import RECORD_COLUMNS
from aws_data.lib.cloudtrail_s3 import CLOUDTRAIL_PARTITIONS_DEF

EVENTS_KEY = dg.AssetKey(["aws-data", "bronze", "cloudtrail_events"])
ACTIVITY_KEY = dg.AssetKey(["aws-data", "gold", "cloudtrail_activity"])
NOON = datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc)


def test_the_table_is_gold_and_sits_beside_the_other_cloudtrail_assets():
    assert cloudtrail_activity.key == ACTIVITY_KEY
    assert cloudtrail_activity.group_names_by_key[ACTIVITY_KEY] == "cloudtrail"


def test_it_is_built_from_bronze_and_is_not_partitioned():
    assert cloudtrail_events.key == EVENTS_KEY
    assert cloudtrail_activity.asset_deps[ACTIVITY_KEY] == {EVENTS_KEY}
    assert cloudtrail_activity.partitions_def is None
    assert "partition_expr" not in cloudtrail_activity.metadata_by_key[ACTIVITY_KEY]


def test_it_reads_all_of_bronze_lazily_and_only_the_columns_it_aggregates():
    bronze_input = cloudtrail_activity.node_def.input_def_named("bronze")

    assert bronze_input.dagster_type.typing_type is pl.LazyFrame
    assert bronze_input.metadata["columns"] == list(RECORD_COLUMNS)
    assert "raw_json" not in RECORD_COLUMNS
    assert cloudtrail_activity.partitions_def is None  # so it is given every partition of bronze


def delta_io_manager(root: str) -> DeltaLakePolarsIOManager:
    return DeltaLakePolarsIOManager(root_uri=root, storage_options=LocalConfig())


def bronze_day(delivery_day: str, *event_times: datetime) -> dg.AssetsDefinition:
    """A stand-in for one Delivery Day of the real bronze asset, with one Record per event time."""

    @dg.asset(
        key=EVENTS_KEY,
        partitions_def=CLOUDTRAIL_PARTITIONS_DEF,
        metadata={"partition_expr": "event_date"},
    )
    def cloudtrail_events() -> pl.DataFrame:
        return pl.DataFrame(
            [
                {
                    "event_date": date.fromisoformat(delivery_day),
                    "event_id": f"{delivery_day}-{i}",
                    "event_time": event_time,
                    "user_identity_arn": "arn:aws:sts::1:assumed-role/admin/lmac",
                    "event_source": "s3.amazonaws.com",
                    "event_name": "GetObject",
                    "recipient_account_id": "111111111111",
                    "aws_region": "us-east-1",
                    "error_code": None,
                    "read_only": True,
                    "raw_json": "{}",
                }
                for i, event_time in enumerate(event_times)
            ],
            # bronze's schema types every column, even one with no value in a given day
            schema_overrides={"event_time": pl.Datetime("us", "UTC"), "error_code": pl.Utf8},
        )

    return cloudtrail_events


def test_every_delivery_day_is_counted_in_one_unpartitioned_table_that_is_rebuilt_in_place(tmp_path):
    root = str(tmp_path / "aws-data")  # the real root also ends in the project prefix
    io_manager = delta_io_manager(root)
    when = lambda day, hour, minute: datetime(2026, 11, day, hour, minute, tzinfo=timezone.utc)

    def materialize(assets, partition_key=None, selection=None):
        with freeze_time(NOON):
            result = dg.materialize(
                assets,
                partition_key=partition_key,
                selection=selection,
                resources={"io_manager": io_manager},
            )
        assert result.success

    # the 12th's last minute was delivered with the 13th's Log Files
    materialize([bronze_day("2026-11-12", when(12, 10, 0), when(12, 10, 5))], "2026-11-12")
    bronze_13th = bronze_day("2026-11-13", when(12, 23, 59), when(13, 0, 1))
    materialize([bronze_13th], "2026-11-13")

    # bronze is read back from Delta, so it is only defined here, not rerun;
    # building twice must replace the table, not add to it
    for _ in range(2):
        materialize([bronze_13th, cloudtrail_activity], selection=[cloudtrail_activity])

    table = pl.read_delta(f"{root}/gold/cloudtrail_activity")
    daily = table.filter(pl.col("grain") == "1d")
    assert daily["record_count"].sum() == 4  # 10:00, 10:05, 23:59 and 00:01
    assert table.height == table.unique(subset=["grain", "bucket_start", "caller", "event_name", "error_code"]).height
    assert DeltaTable(f"{root}/gold/cloudtrail_activity").metadata().partition_columns == []
