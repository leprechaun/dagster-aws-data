import dagster as dg
from dagster._core.test_utils import freeze_time

from aws_data.defs.cloudtrail.cloudtrail_raw_files import cloudtrail_files
from tests.conftest import AFTER_THE_DAYS_UNDER_TEST, FakeS3Client, FakeS3Resource

ORGANIZATION = "o-a1b2c3d4e5"
ACCOUNT = "111111111111"


def cloudtrail_file(organization: str, account: str, region: str, day: str, name: str = "a1b2c3") -> str:
    year, month, dom = day.split("-")
    return (
        f"AWSLogs/{organization}/{account}/CloudTrail/{region}/{year}/{month}/{dom}/"
        f"{account}_CloudTrail_{region}_{year}{month}{dom}T0000Z_{name}.json.gz"
    )


def mirror(source: FakeS3Client, raw: FakeS3Client, partition_key: str = "2026-11-10") -> dg.ExecuteInProcessResult:
    with freeze_time(AFTER_THE_DAYS_UNDER_TEST):
        return dg.materialize(
            [cloudtrail_files],
            partition_key=partition_key,
            resources={
                "aws_s3": FakeS3Resource(source),
                "minio_s3": FakeS3Resource(raw),
                "raw_files_io_manager": dg.mem_io_manager,
            },
        )


def reported(result: dg.ExecuteInProcessResult) -> dict:
    [materialization] = result.asset_materializations_for_node("aws_data__raw__cloudtrail_files")
    return {name: value.value for name, value in materialization.metadata.items()}


def test_the_days_json_gz_files_are_mirrored_byte_for_byte_and_nothing_else():
    wanted = {
        cloudtrail_file(ORGANIZATION, ACCOUNT, "us-east-1", "2026-11-10", "aaaa"): b"one",
        cloudtrail_file(ORGANIZATION, ACCOUNT, "eu-west-1", "2026-11-10", "bbbb"): b"two",
    }
    unwanted = {
        cloudtrail_file(ORGANIZATION, ACCOUNT, "us-east-1", "2026-11-11"): b"another day",
        f"AWSLogs/{ORGANIZATION}/{ACCOUNT}/CloudTrail/us-east-1/2026/11/10/digest.txt": b"not a log file",
    }
    source = FakeS3Client(objects={**wanted, **unwanted})
    raw = FakeS3Client()

    assert mirror(source, raw).success

    assert raw.objects == {f"aws-data/raw/cloudtrail/{key}": body for key, body in wanted.items()}


def test_folders_that_are_not_organizations_are_not_mirrored():
    real = cloudtrail_file(ORGANIZATION, ACCOUNT, "us-east-1", "2026-11-10")
    # a non-organization trail writes AWSLogs/<account>/CloudTrail/... with no organization level
    stray = "AWSLogs/222222222222/CloudTrail/us-east-1/2026/11/10/222222222222_CloudTrail_us-east-1_x.json.gz"
    source = FakeS3Client(objects={real: b"real", stray: b"stray"})
    raw = FakeS3Client()

    assert mirror(source, raw).success

    assert raw.objects == {f"aws-data/raw/cloudtrail/{real}": b"real"}


def test_a_second_run_copies_nothing_and_the_run_reports_what_it_did():
    key = cloudtrail_file(ORGANIZATION, ACCOUNT, "us-east-1", "2026-11-10")
    source = FakeS3Client(objects={key: b"12345"})
    raw = FakeS3Client()

    first = reported(mirror(source, raw))
    second = reported(mirror(source, raw))

    assert (first["files_copied"], first["files_already_cached"], first["bytes_copied"]) == (1, 0, 5)
    assert (second["files_copied"], second["files_already_cached"], second["bytes_copied"]) == (0, 1, 0)
    assert first["organizations"] == [ORGANIZATION]
    assert first["accounts"] == [ACCOUNT]
    assert first["regions"] == ["us-east-1"]
