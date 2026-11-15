"""S3 layout for GitHub webhook deliveries.

Source objects sit under `data/github/<detail-type>/yyyy/mm/dd/...` in the
firehose bucket and are mirrored to `github/raw/<detail-type>/yyyy/mm/dd/...`
on the storage bucket, unchanged except for dropping the leading `data/`.
"""

from datetime import date

import dagster as dg

SOURCE_BUCKET = "lmacguire-firehose"
# The bucket's own region: S3 requests must go there, not to the default one.
GITHUB_SOURCE_REGION = "ap-southeast-1"
SOURCE_PREFIX = "data/github"
RAW_PREFIX = "github/raw"

# The first day any GitHub delivery appears in the source bucket.
GITHUB_HISTORY_START_DATE = "2025-01-30"

# end_offset=1 makes the day in progress a partition too; the mirror skips what
# it already has, so a partial day can safely be rerun.
GITHUB_PARTITIONS_DEF = dg.DailyPartitionsDefinition(
    start_date=GITHUB_HISTORY_START_DATE, end_offset=1
)


def github_raw_key(source_key: str) -> str:
    """The Raw key for a source key: `data/github/...` becomes `github/raw/...`."""
    return RAW_PREFIX + source_key.removeprefix(SOURCE_PREFIX)


def github_day_prefix(event_type: str, day: date) -> str:
    """The source folder holding one event type's deliveries for one UTC day."""
    return f"{SOURCE_PREFIX}/{event_type}/{day.year:04d}/{day.month:02d}/{day.day:02d}/"
