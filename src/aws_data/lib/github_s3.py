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

# Every event type that is ingested. `default` is left out: its one file is a
# duplicate delivery of a `create` event.
GITHUB_EVENT_TYPES = (
    "check_run",
    "check_suite",
    "create",
    "delete",
    "dependabot_alert",
    "deployment",
    "deployment_status",
    "issue_comment",
    "issues",
    "label",
    "push",
    "release",
    "repository",
    "repository_ruleset",
    "repository_vulnerability_alert",
    "workflow_job",
    "workflow_run",
)

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


def keys_for_days(keys: list[str], event_type: str, days: set[date]) -> list[str]:
    """The source keys under one event type whose `yyyy/mm/dd` folder is one of
    `days`. Keys may sit one folder deeper (by hour); the day is always the three
    segments right after the event type."""
    folder = f"{SOURCE_PREFIX}/{event_type}/"
    selected = []
    for key in keys:
        year, month, day = key.removeprefix(folder).split("/")[:3]
        if date(int(year), int(month), int(day)) in days:
            selected.append(key)
    return selected
