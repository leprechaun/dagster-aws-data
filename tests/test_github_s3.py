from aws_data.lib.github_s3 import github_raw_key


def test_raw_key_drops_the_data_segment_and_keeps_the_rest_of_the_key():
    source_key = (
        "data/github/check_run/2026/08/08/webhooks-2-2026-08-08-10-32-19-6738f2f0.json"
    )

    assert github_raw_key(source_key) == (
        "github/raw/check_run/2026/08/08/webhooks-2-2026-08-08-10-32-19-6738f2f0.json"
    )
