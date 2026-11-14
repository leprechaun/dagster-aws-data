from datetime import datetime, timedelta, timezone

import dagster as dg


def partitions_to_refresh(now: datetime, partitions_def: dg.TimeWindowPartitionsDefinition) -> list[str]:
    """Yesterday's and today's (UTC) partition keys, oldest first, leaving out
    any that don't exist in `partitions_def` (before its start date)."""
    today = now.astimezone(timezone.utc).date()
    keys = [(today - timedelta(days=days_ago)).isoformat() for days_ago in (1, 0)]
    existing = set(partitions_def.get_partition_keys(current_time=now))
    return [key for key in keys if key in existing]


def billing_periods_to_refresh(now: datetime, partitions_def: dg.TimeWindowPartitionsDefinition) -> list[str]:
    """Last month's and this month's (UTC) partition keys, oldest first,
    leaving out any that don't exist in `partitions_def` (before its start
    date). A Billing Period stays open to restatement for a while after it
    ends, so the one just closed is refreshed too."""
    this_month = now.astimezone(timezone.utc).date().replace(day=1)
    last_month = (this_month - timedelta(days=1)).replace(day=1)
    keys = [last_month.isoformat(), this_month.isoformat()]
    existing = set(partitions_def.get_partition_keys(current_time=now))
    return [key for key in keys if key in existing]
