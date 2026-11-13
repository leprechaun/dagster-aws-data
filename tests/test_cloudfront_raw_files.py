from datetime import datetime, timezone

import dagster as dg
from dagster._core.test_utils import freeze_time

from aws_data.defs.cloudfront.cloudfront_raw_files import cloudfront_files
from tests.conftest import FakeS3Client

ACCOUNT = "111111111111"
DISTRIBUTION = "E1EXAMPLE0001"
LOG_FILE = f"AWSLogs/{ACCOUNT}/CloudFront/{DISTRIBUTION}/2026/11/13/09/{DISTRIBUTION}.2026-11-13-09.aaaa1111.gz"


def log_file(account: str, distribution: str, day: str, hour: str, suffix: str = "aaaa1111") -> str:
    year, month, dom = day.split("-")
    return (
        f"AWSLogs/{account}/CloudFront/{distribution}/{year}/{month}/{dom}/{hour}/"
        f"{distribution}.{day}-{hour}.{suffix}.gz"
    )


def reported(result: dg.ExecuteInProcessResult) -> dict:
    """What the run reported about itself, as plain values."""
    [materialization] = result.asset_materializations_for_node("aws_data__raw__cloudfront_files")
    return {name: value.value for name, value in materialization.metadata.items()}


class FakeS3Resource:
    def __init__(self, client: FakeS3Client):
        self._client = client

    def get_client(self) -> FakeS3Client:
        return self._client


# A day's partition only exists once that day has ended, so run "after" the
# days under test instead of depending on today's date.
AFTER_THE_DAYS_UNDER_TEST = datetime(2026, 12, 1, tzinfo=timezone.utc)


def mirror(source: FakeS3Client, raw: FakeS3Client, partition_key: str = "2026-11-13") -> dg.ExecuteInProcessResult:
    with freeze_time(AFTER_THE_DAYS_UNDER_TEST):
        return dg.materialize(
            [cloudfront_files],
            partition_key=partition_key,
            resources={
                "aws_s3": FakeS3Resource(source),
                "minio_s3": FakeS3Resource(raw),
                "raw_files_io_manager": dg.mem_io_manager,
            },
        )


def test_a_log_file_is_mirrored_into_raw_byte_for_byte():
    source = FakeS3Client(objects={LOG_FILE: b"\x1f\x8bpretend-gzip-bytes"})
    raw = FakeS3Client()

    result = mirror(source, raw)

    assert result.success
    assert raw.objects == {f"aws-data/raw/cloudfront/{LOG_FILE}": b"\x1f\x8bpretend-gzip-bytes"}


def test_every_hour_of_every_distribution_of_every_account_is_mirrored_and_other_days_are_not():
    day = {
        log_file("111111111111", "E1EXAMPLE0001", "2026-11-13", "09"): b"a",
        log_file("111111111111", "E1EXAMPLE0001", "2026-11-13", "10"): b"b",
        log_file("111111111111", "E1EXAMPLE0002", "2026-11-13", "09"): b"c",
        log_file("222222222222", "E1EXAMPLE0003", "2026-11-13", "23"): b"d",
    }
    next_day = {log_file("111111111111", "E1EXAMPLE0001", "2026-11-14", "00"): b"e"}
    source = FakeS3Client(objects={**day, **next_day})
    raw = FakeS3Client()

    assert mirror(source, raw).success

    assert raw.objects == {f"aws-data/raw/cloudfront/{key}": body for key, body in day.items()}


def test_folders_that_are_not_account_ids_are_not_mirrored():
    real = log_file("111111111111", "E1EXAMPLE0001", "2026-11-13", "09")
    stray = log_file("not-an-account", "E1EXAMPLE0009", "2026-11-13", "09")
    source = FakeS3Client(objects={real: b"real", stray: b"stray"})
    raw = FakeS3Client()

    assert mirror(source, raw).success

    assert raw.objects == {f"aws-data/raw/cloudfront/{real}": b"real"}


def test_only_gz_log_files_are_mirrored_not_placeholders_or_other_objects():
    real = log_file("111111111111", "E1EXAMPLE0001", "2026-11-13", "09")
    day_folder = "AWSLogs/111111111111/CloudFront/E1EXAMPLE0001/2026/11/13/"
    source = FakeS3Client(
        objects={
            real: b"real",
            day_folder: b"",  # the zero-byte "folder" object S3 consoles create
            f"{day_folder}09/": b"",
            f"{day_folder}09/readme.txt": b"not a log file",
        }
    )
    raw = FakeS3Client()

    assert mirror(source, raw).success

    assert raw.objects == {f"aws-data/raw/cloudfront/{real}": b"real"}


def test_a_second_run_copies_nothing_and_reports_the_file_as_already_cached():
    source = FakeS3Client(objects={log_file("111111111111", "E1EXAMPLE0001", "2026-11-13", "09"): b"x"})
    raw = FakeS3Client()

    first = reported(mirror(source, raw))
    second = reported(mirror(source, raw))

    assert (first["files_copied"], first["files_already_cached"]) == (1, 0)
    assert (second["files_copied"], second["files_already_cached"]) == (0, 1)


def test_the_run_reports_bytes_copied_and_the_accounts_and_distributions_it_found():
    source = FakeS3Client(
        objects={
            log_file("111111111111", "E1EXAMPLE0001", "2026-11-13", "09"): b"12345",
            log_file("111111111111", "E1EXAMPLE0002", "2026-11-13", "09"): b"123",
            log_file("222222222222", "E1EXAMPLE0003", "2026-11-13", "09"): b"12",
        }
    )

    report = reported(mirror(source, FakeS3Client()))

    assert report["bytes_copied"] == 10
    assert report["accounts"] == ["111111111111", "222222222222"]
    assert report["distributions"] == ["E1EXAMPLE0001", "E1EXAMPLE0002", "E1EXAMPLE0003"]


def test_a_day_with_no_log_files_succeeds_and_copies_nothing():
    other_day = log_file("111111111111", "E1EXAMPLE0001", "2026-11-14", "00")
    source = FakeS3Client(objects={other_day: b"x"})
    raw = FakeS3Client()

    result = mirror(source, raw)

    assert result.success
    assert raw.objects == {}
    assert reported(result)["files_copied"] == 0


def test_the_asset_is_registered_under_the_project_namespace_in_the_shared_pool():
    key = cloudfront_files.key
    [spec] = cloudfront_files.specs

    assert key.path == ["aws-data", "raw", "cloudfront_files"]
    assert cloudfront_files.partitions_def.start.date().isoformat() == "2026-11-13"
    assert cloudfront_files.group_names_by_key[key] == "cloudfront"
    assert spec.kinds == {"s3"}
    assert cloudfront_files.op.pool == "aws-data"
    assert cloudfront_files.get_io_manager_key_for_asset_key(key) == "raw_files_io_manager"
