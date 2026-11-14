from datetime import date, datetime, timezone

import dagster as dg
import polars as pl
from dagster._core.test_utils import freeze_time
from dagster_deltalake import LocalConfig
from dagster_deltalake_polars import DeltaLakePolarsIOManager

from aws_data.defs.cloudtrail.cloudtrail_activity import cloudtrail_activity
from aws_data.defs.cloudtrail.cloudtrail_events import cloudtrail_events
from aws_data.defs.resources import DeltaTables
from aws_data.lib.cloudtrail_s3 import CLOUDTRAIL_PARTITIONS_DEF

EVENTS_KEY = dg.AssetKey(["aws-data", "bronze", "cloudtrail_events"])
ACTIVITY_KEY = dg.AssetKey(["aws-data", "gold", "cloudtrail_activity"])
NOON = datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc)


def test_the_table_is_gold_and_sits_beside_the_other_cloudtrail_assets():
    assert cloudtrail_activity.key == ACTIVITY_KEY
    assert cloudtrail_activity.group_names_by_key[ACTIVITY_KEY] == "cloudtrail"


def test_it_is_built_from_bronze_and_partitioned_by_the_day_its_buckets_fall_on():
    assert cloudtrail_events.key == EVENTS_KEY
    assert cloudtrail_activity.asset_deps[ACTIVITY_KEY] == {EVENTS_KEY}
    assert cloudtrail_activity.partitions_def == CLOUDTRAIL_PARTITIONS_DEF
    assert (
        cloudtrail_activity.metadata_by_key[ACTIVITY_KEY]["partition_expr"]
        == "bucket_date"
    )


def test_a_day_is_built_from_that_days_and_the_next_days_delivery():
    # Records from a day's last minutes arrive in the next day's folder
    [dep] = [dep for spec in cloudtrail_activity.specs for dep in spec.deps]
    mapped = dep.partition_mapping.get_upstream_mapped_partitions_result_for_partitions(
        downstream_partitions_subset=CLOUDTRAIL_PARTITIONS_DEF.empty_subset().with_partition_keys(
            ["2026-11-12"]
        ),
        downstream_partitions_def=CLOUDTRAIL_PARTITIONS_DEF,
        upstream_partitions_def=CLOUDTRAIL_PARTITIONS_DEF,
        current_time=NOON,
    )

    assert sorted(mapped.partitions_subset.get_partition_keys()) == [
        "2026-11-12",
        "2026-11-13",
    ]


def test_it_reads_bronze_through_the_delta_tables_resource():
    assert "delta_tables" in cloudtrail_activity.required_resource_keys


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
            schema_overrides={"event_time": pl.Datetime("us", "UTC")},
        )

    return cloudtrail_events


def test_a_day_is_built_through_delta_from_both_delivery_days_and_rebuilt_in_place(
    tmp_path,
):
    root = str(tmp_path / "aws-data")  # the real root also ends in the project prefix
    io_manager = delta_io_manager(root)
    when = lambda day, hour, minute: datetime(
        2026, 11, day, hour, minute, tzinfo=timezone.utc
    )

    def materialize(asset, partition_key):
        with freeze_time(NOON):
            result = dg.materialize(
                [asset],
                partition_key=partition_key,
                resources={"io_manager": io_manager},
            )
        assert result.success
        return result

    # the 12th's last minute was delivered with the 13th's Log Files
    materialize(
        bronze_day("2026-11-12", when(12, 10, 0), when(12, 10, 5)), "2026-11-12"
    )
    bronze_13th = bronze_day("2026-11-13", when(12, 23, 59), when(13, 0, 1))
    materialize(bronze_13th, "2026-11-13")

    # bronze is read from Delta by the asset, so it is only defined here (for lineage), not rerun;
    # building the day twice must replace its partition, not add to it
    for _ in range(2):
        with freeze_time(NOON):
            result = dg.materialize(
                [bronze_13th, cloudtrail_activity],
                selection=[cloudtrail_activity],
                partition_key="2026-11-12",
                resources={
                    "io_manager": io_manager,
                    "delta_tables": DeltaTables(root_uri=root),
                },
            )
        assert result.success

    table = pl.read_delta(f"{root}/gold/cloudtrail_activity")
    daily = table.filter(pl.col("grain") == "1d")
    assert daily["record_count"].to_list() == [
        3
    ]  # 10:00, 10:05 and 23:59 on the 12th, not 00:01 on the 13th
    assert table["bucket_date"].unique().to_list() == [date(2026, 11, 12)]
    assert (
        table.height
        == table.unique(
            subset=["grain", "bucket_start", "caller", "event_name", "error_code"]
        ).height
    )
