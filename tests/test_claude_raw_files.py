import dagster as dg

from aws_data.defs.claude.claude_raw_files import SESSION_STATUS_FILES
from tests.conftest import FakeS3Client, FakeS3Resource

STATUS = (
    "data/claude/session-status/2026/11/13/statusline-1-2026-11-13-10-32-19-6738f2f0"
)


def mirror(source: FakeS3Client, raw: FakeS3Client) -> dg.ExecuteInProcessResult:
    return dg.materialize(
        [SESSION_STATUS_FILES],
        resources={
            "github_source_s3": FakeS3Resource(source),
            "minio_s3": FakeS3Resource(raw),
            "raw_files_io_manager": dg.mem_io_manager,
        },
    )


def test_a_session_status_file_is_mirrored_into_claude_raw_byte_for_byte():
    source = FakeS3Client(objects={STATUS: b'{"detail-type": "session-status"}'})
    raw = FakeS3Client()

    result = mirror(source, raw)

    assert result.success
    assert raw.objects == {
        "claude/raw/session-status/2026/11/13/statusline-1-2026-11-13-10-32-19-6738f2f0": b'{"detail-type": "session-status"}'
    }


def test_only_the_session_status_folder_is_mirrored_and_other_sources_are_not():
    source = FakeS3Client(
        objects={
            STATUS: b"status",
            "data/claude/stats/2026/11/13/stats-1": b"stats",
            "data/claude/session-status-archive/2026/11/13/x": b"sibling",
            "data/test/session-status/2026/11/13/test-1": b"test",
            "data/github/check_run/2026/11/13/delivery-1": b"github",
        }
    )
    raw = FakeS3Client()

    assert mirror(source, raw).success

    assert raw.objects == {
        "claude/raw/session-status/2026/11/13/statusline-1-2026-11-13-10-32-19-6738f2f0": b"status",
    }


def metadata_of(result: dg.ExecuteInProcessResult) -> dict:
    return result.asset_materializations_for_node(SESSION_STATUS_FILES.op.name)[
        0
    ].metadata


def test_rerunning_copies_nothing_new_and_reports_the_files_already_cached():
    source = FakeS3Client(objects={STATUS: b"x"})
    raw = FakeS3Client()
    mirror(source, raw)

    result = mirror(source, raw)

    assert result.success
    assert len(raw.objects) == 1
    metadata = metadata_of(result)
    assert metadata["files_copied"].value == 0
    assert metadata["files_already_cached"].value == 1
    assert metadata["bytes_copied"].value == 0


def test_the_run_reports_files_copied_already_cached_and_bytes_copied():
    source = FakeS3Client(
        objects={
            "data/claude/session-status/2026/11/13/a": b"one",
            "data/claude/session-status/2026/11/14/b": b"three",
        }
    )
    raw = FakeS3Client(objects={"claude/raw/session-status/2026/11/13/a": b"one"})

    result = mirror(source, raw)

    metadata = metadata_of(result)
    assert metadata["files_copied"].value == 1
    assert metadata["files_already_cached"].value == 1
    assert metadata["bytes_copied"].value == len(b"three")


def test_an_empty_source_folder_succeeds_and_copies_nothing():
    raw = FakeS3Client()

    result = mirror(FakeS3Client(), raw)

    assert result.success
    assert raw.objects == {}
    assert metadata_of(result)["files_copied"].value == 0
