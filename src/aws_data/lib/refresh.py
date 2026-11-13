from datetime import datetime, timedelta, timezone

import dagster as dg


def partitions_to_refresh(now: datetime, partitions_def: dg.TimeWindowPartitionsDefinition) -> list[str]:
    """Yesterday's and today's (UTC) partition keys, oldest first, leaving out
    any that don't exist in `partitions_def` (before its start date)."""
    today = now.astimezone(timezone.utc).date()
    keys = [(today - timedelta(days=days_ago)).isoformat() for days_ago in (1, 0)]
    existing = set(partitions_def.get_partition_keys(current_time=now))
    return [key for key in keys if key in existing]
