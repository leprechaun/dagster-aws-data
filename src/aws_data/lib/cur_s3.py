"""Shared constants and key helpers for the Cost and Usage Report (CUR 2.0,
Parquet), whose objects are laid out as:

    {export}/data/BILLING_PERIOD={yyyy-mm}/{file}.parquet
"""

import dagster as dg

from aws_data.lib.cloudtrail_s3 import PROJECT_PREFIX

# Source: the bucket the Data Export delivers to, and the export's own folder.
CUR_SOURCE_BUCKET = "lmacguire-aws-costs-reporter"
CUR_SOURCE_EXPORT_PREFIX = "cur-reports/with-resource-ids-split-hourly-parquet-overwrite"

# Destination: mirrored 1:1 (same key layout) beside the other Raw files.
CUR_RAW_PREFIX = f"{PROJECT_PREFIX}/raw/cur"

# The first Billing Period the export holds.
CUR_HISTORY_START_DATE = "2022-01-01"

# end_offset=1 makes the Billing Period in progress a partition too.
CUR_PARTITIONS_DEF = dg.MonthlyPartitionsDefinition(start_date=CUR_HISTORY_START_DATE, end_offset=1)


def billing_period(partition_key: str) -> str:
    """The `yyyy-mm` Billing Period of a monthly partition key (`yyyy-mm-01`)."""
    return partition_key[:7]


def billing_period_prefix(export_prefix: str, period: str) -> str:
    return f"{export_prefix}/data/BILLING_PERIOD={period}/"
