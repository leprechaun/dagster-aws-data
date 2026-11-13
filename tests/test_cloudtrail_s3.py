from datetime import date

import pytest

from aws_data.lib.cloudtrail_s3 import (
    day_prefix,
    list_accounts,
    list_common_prefixes,
    list_day_object_keys,
    list_organizations,
    list_regions,
)
from tests.conftest import FakeS3Client

ROOT = "AWSLogs"
ORG = "o-abc123"


def _key(account: str, region: str, day: str, name: str) -> str:
    return f"{ROOT}/{ORG}/{account}/CloudTrail/{region}/{day}/{name}"


KEYS = [
    _key("111111111111", "us-east-1", "2026/11/10", "a.json.gz"),
    _key("111111111111", "us-east-1", "2026/11/10", "b.json.gz"),
    _key("111111111111", "us-east-1", "2026/11/10", "digest.txt"),
    _key("111111111111", "eu-west-1", "2026/11/10", "c.json.gz"),
    _key("111111111111", "us-east-1", "2026/11/11", "d.json.gz"),
    _key("222222222222", "us-east-1", "2026/11/10", "e.json.gz"),
    f"{ROOT}/o-other/333333333333/CloudTrail/us-east-1/2026/11/10/f.json.gz",
]


@pytest.fixture
def client() -> FakeS3Client:
    return FakeS3Client(KEYS)


class TestDayPrefix:
    def test_builds_full_key_layout(self):
        assert (
            day_prefix(ROOT, ORG, "111111111111", "us-east-1", date(2026, 11, 10))
            == "AWSLogs/o-abc123/111111111111/CloudTrail/us-east-1/2026/11/10/"
        )

    def test_zero_pads_month_and_day(self):
        assert day_prefix(ROOT, ORG, "1", "r", date(2026, 1, 5)).endswith("/2026/01/05/")


class TestListing:
    def test_list_common_prefixes_spans_pages(self):
        small_pages = FakeS3Client(KEYS, page_size=1)
        assert list_common_prefixes(small_pages, "b", f"{ROOT}/") == [
            f"{ROOT}/{ORG}/",
            f"{ROOT}/o-other/",
        ]

    def test_list_organizations(self, client):
        assert list_organizations(client, "b", ROOT) == [ORG, "o-other"]

    def test_list_accounts(self, client):
        assert list_accounts(client, "b", ROOT, ORG) == ["111111111111", "222222222222"]

    def test_list_regions(self, client):
        assert list_regions(client, "b", ROOT, ORG, "111111111111") == ["eu-west-1", "us-east-1"]

    def test_list_regions_unknown_account_is_empty(self, client):
        assert list_regions(client, "b", ROOT, ORG, "999999999999") == []


class TestListDayObjectKeys:
    def test_only_returns_json_gz_for_that_day_and_region(self, client):
        keys = list_day_object_keys(client, "b", ROOT, ORG, "111111111111", "us-east-1", date(2026, 11, 10))
        assert keys == [
            _key("111111111111", "us-east-1", "2026/11/10", "a.json.gz"),
            _key("111111111111", "us-east-1", "2026/11/10", "b.json.gz"),
        ]

    def test_day_without_files_is_empty(self, client):
        assert list_day_object_keys(client, "b", ROOT, ORG, "111111111111", "us-east-1", date(2020, 1, 1)) == []


class TestNamespacing:
    def test_raw_prefix_is_under_project_prefix(self):
        from aws_data.lib.cloudtrail_s3 import PROJECT_PREFIX, RAW_PREFIX

        assert RAW_PREFIX.startswith(f"{PROJECT_PREFIX}/")

    def test_asset_keys_start_with_project_prefix(self):
        from aws_data.defs.cloudtrail.cloudtrail_events import cloudtrail_events
        from aws_data.defs.cloudtrail.cloudtrail_raw_files import cloudtrail_files

        assert cloudtrail_files.key.path == ["aws-data", "raw", "cloudtrail_files"]
        assert cloudtrail_events.key.path == ["aws-data", "bronze", "cloudtrail_events"]


def test_partitions_start_on_the_first_delivery_day():
    from aws_data.lib.cloudtrail_s3 import CLOUDTRAIL_PARTITIONS_DEF

    assert CLOUDTRAIL_PARTITIONS_DEF.get_first_partition_key() == "2026-08-23"


def test_assets_share_one_project_pool():
    from aws_data.defs.cloudtrail.cloudtrail_events import cloudtrail_events
    from aws_data.defs.cloudtrail.cloudtrail_raw_files import cloudtrail_files

    for asset in (cloudtrail_files, cloudtrail_events):
        assert asset.op.pool == "aws-data"
