"""S3 layout for GitHub webhook deliveries.

Source objects sit under `data/github/<detail-type>/yyyy/mm/dd/...` in the
firehose bucket and are mirrored to `github/raw/<detail-type>/yyyy/mm/dd/...`
on the storage bucket, unchanged except for dropping the leading `data/`.
"""

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


def github_raw_key(source_key: str) -> str:
    """The Raw key for a source key: `data/github/...` becomes `github/raw/...`."""
    return RAW_PREFIX + source_key.removeprefix(SOURCE_PREFIX)
