from datetime import date

import dagster as dg
import polars as pl
from dagster_delta import DeltaLakePolarsIOManager, LocalConfig

DAILY = dg.DailyPartitionsDefinition(start_date="2026-11-10")
WITH_THE_NEXT_DAY = dg.TimeWindowPartitionMapping(start_offset=0, end_offset=1)


@dg.asset(key=["bronze", "things"], partitions_def=DAILY, metadata={"partition_expr": "day"})
def things(context: dg.AssetExecutionContext) -> pl.DataFrame:
    return pl.DataFrame({"day": [date.fromisoformat(context.partition_key)], "n": [1]})


def test_an_input_spanning_two_days_is_read_in_full(tmp_path):
    """The IO manager must filter a mapped two-day input on both days: reading only
    the first drops the last minutes of every day, silently."""
    io_manager = DeltaLakePolarsIOManager(root_uri=str(tmp_path), storage_options=LocalConfig())
    resources = {"io_manager": io_manager}
    days_read: list[list[date]] = []

    @dg.asset(
        key=["gold", "seen"],
        partitions_def=DAILY,
        metadata={"partition_expr": "day"},
        ins={"things": dg.AssetIn(key=things.key, partition_mapping=WITH_THE_NEXT_DAY)},
    )
    def seen(context: dg.AssetExecutionContext, things: pl.LazyFrame) -> pl.DataFrame:
        days_read.append(sorted(things.collect()["day"]))
        return pl.DataFrame({"day": [date.fromisoformat(context.partition_key)], "n": [0]})

    for day in ("2026-11-12", "2026-11-13"):
        assert dg.materialize([things], partition_key=day, resources=resources).success
    assert dg.materialize([things, seen], selection=[seen], partition_key="2026-11-12", resources=resources).success

    assert days_read == [[date(2026, 11, 12), date(2026, 11, 13)]]
