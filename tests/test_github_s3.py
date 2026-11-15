from datetime import date

from aws_data.lib.github_s3 import RAW_PREFIX, github_day_prefix, github_raw_key
from aws_data.lib.s3 import mirror_prefixes
from tests.conftest import FakeS3Client


def test_raw_key_drops_the_data_segment_and_keeps_the_rest_of_the_key():
    source_key = (
        "data/github/check_run/2026/08/08/webhooks-2-2026-08-08-10-32-19-6738f2f0.json"
    )

    assert github_raw_key(source_key) == (
        "github/raw/check_run/2026/08/08/webhooks-2-2026-08-08-10-32-19-6738f2f0.json"
    )


def test_day_prefix_is_the_source_folder_for_one_event_type_and_day():
    assert (
        github_day_prefix("check_run", date(2026, 8, 8))
        == "data/github/check_run/2026/08/08/"
    )


def test_mirror_copies_one_day_of_one_event_type_to_raw_keys_byte_for_byte():
    source = FakeS3Client(
        objects={
            "data/github/check_run/2026/08/08/a.json": b"one",
            "data/github/check_run/2026/08/08/10/b.json": b"two",
            "data/github/check_run/2026/08/09/c.json": b"other day",
            "data/github/push/2026/08/08/d.json": b"other type",
        }
    )
    dest = FakeS3Client()
    day_prefix = github_day_prefix("check_run", date(2026, 8, 8))

    result = mirror_prefixes(
        source,
        "firehose",
        [day_prefix],
        ".json",
        dest,
        "deltalake",
        RAW_PREFIX,
        dest_key=github_raw_key,
    )

    assert dest.objects == {
        "github/raw/check_run/2026/08/08/a.json": b"one",
        "github/raw/check_run/2026/08/08/10/b.json": b"two",
    }
    assert result.files_copied == 2
    assert result.files_already_cached == 0


def test_rerunning_the_github_mirror_copies_nothing_new():
    source = FakeS3Client(objects={"data/github/check_run/2026/08/08/a.json": b"one"})
    dest = FakeS3Client()
    day_prefix = github_day_prefix("check_run", date(2026, 8, 8))
    mirror = lambda: mirror_prefixes(
        source,
        "firehose",
        [day_prefix],
        ".json",
        dest,
        "deltalake",
        RAW_PREFIX,
        dest_key=github_raw_key,
    )
    mirror()

    result = mirror()

    assert result.files_copied == 0
    assert result.files_already_cached == 1


def test_mirroring_an_empty_day_copies_nothing_and_does_not_fail():
    result = mirror_prefixes(
        FakeS3Client(),
        "firehose",
        [github_day_prefix("check_run", date(2026, 8, 8))],
        ".json",
        FakeS3Client(),
        "deltalake",
        RAW_PREFIX,
        dest_key=github_raw_key,
    )

    assert result.files_copied == 0
    assert result.files_already_cached == 0
