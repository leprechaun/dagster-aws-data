"""S3 layout for Claude Session Status files.

Source objects sit under `data/claude/<detail-type>/yyyy/mm/dd/...` in the
firehose bucket and are mirrored to `claude/raw/<detail-type>/yyyy/mm/dd/...`
on the storage bucket, unchanged except for dropping the leading `data/`.
"""

CLAUDE_SOURCE_PREFIX = "data/claude"
CLAUDE_RAW_PREFIX = "claude/raw"

# The detail-type folder of a Claude Session Status file.
SESSION_STATUS = "session-status"


def claude_raw_key(source_key: str) -> str:
    """The Raw key for a source key: `data/claude/...` becomes `claude/raw/...`."""
    return CLAUDE_RAW_PREFIX + source_key.removeprefix(CLAUDE_SOURCE_PREFIX)
