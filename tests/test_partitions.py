from datetime import UTC, datetime

import pytest

from aws_data.lib.cloudfront_s3 import CLOUDFRONT_PARTITIONS_DEF
from aws_data.lib.cloudtrail_s3 import CLOUDTRAIL_PARTITIONS_DEF

MIDDAY = datetime(2026, 11, 13, 12, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    "partitions_def",
    [CLOUDFRONT_PARTITIONS_DEF, CLOUDTRAIL_PARTITIONS_DEF],
    ids=["cloudfront", "cloudtrail"],
)
def test_the_day_in_progress_is_a_partition(partitions_def):
    assert partitions_def.get_partition_keys(current_time=MIDDAY)[-1] == "2026-11-13"


def test_cloudfront_has_a_partition_on_the_day_it_starts():
    first_day_midday = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)

    assert CLOUDFRONT_PARTITIONS_DEF.get_partition_keys(current_time=first_day_midday) == ["2026-10-23"]
