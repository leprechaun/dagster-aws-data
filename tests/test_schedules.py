from datetime import datetime, timezone

import dagster as dg
import pytest
from dagster._core.test_utils import freeze_time

from aws_data.defs.cloudfront.cloudfront_raw_files import cloudfront_files
from aws_data.defs.cloudfront.cloudfront_requests import cloudfront_requests
from aws_data.defs.cloudtrail.cloudtrail_activity import cloudtrail_activity
from aws_data.defs.cloudtrail.cloudtrail_events import cloudtrail_events
from aws_data.defs.cloudtrail.cloudtrail_raw_files import cloudtrail_files
from aws_data.defs.cur.cur_line_items import cur_line_items
from aws_data.defs.cur.cur_raw_files import cur_files
from aws_data.defs.resources import resources
from aws_data.defs.schedules import (
    cloudfront_hourly,
    cloudfront_refresh,
    cloudtrail_activity_hourly,
    cloudtrail_activity_refresh,
    cloudtrail_hourly,
    cloudtrail_refresh,
    cur_hourly,
    cur_refresh,
)


def definitions() -> dg.Definitions:
    """The project's real wiring: its resources plus these assets, jobs and schedules."""
    return dg.Definitions.merge(
        resources(),
        dg.Definitions(
            assets=[cloudtrail_files, cloudtrail_events, cloudtrail_activity, cloudfront_files, cloudfront_requests, cur_files, cur_line_items],
            jobs=[cloudtrail_refresh, cloudtrail_activity_refresh, cloudfront_refresh, cur_refresh],
            schedules=[cloudtrail_hourly, cloudtrail_activity_hourly, cloudfront_hourly, cur_hourly],
        ),
    )


def run_requests_at(schedule: dg.ScheduleDefinition, when: datetime) -> list[dg.RunRequest]:
    # Dagster checks the partition keys against its clock, so freeze it at the tick.
    with freeze_time(when):
        context = dg.build_schedule_context(
            scheduled_execution_time=when, repository_def=definitions().get_repository_def()
        )
        return schedule.evaluate_tick(context).run_requests


def test_cloudtrail_refreshes_yesterday_and_today_each_hour():
    requests = run_requests_at(cloudtrail_hourly, datetime(2026, 11, 13, 10, 20, tzinfo=timezone.utc))

    assert [request.partition_key for request in requests] == ["2026-11-12", "2026-11-13"]


def test_cloudfront_refreshes_only_the_days_that_exist():
    requests = run_requests_at(cloudfront_hourly, datetime(2026, 11, 13, 10, 40, tzinfo=timezone.utc))

    assert [request.partition_key for request in requests] == ["2026-11-13"]


@pytest.mark.parametrize(
    "schedule", [cloudtrail_hourly, cloudfront_hourly, cur_hourly], ids=["cloudtrail", "cloudfront", "cur"]
)
def test_each_tick_launches_fresh_runs_instead_of_being_deduplicated(schedule):
    this_hour = run_requests_at(schedule, datetime(2026, 11, 14, 10, 20, tzinfo=timezone.utc))
    next_hour = run_requests_at(schedule, datetime(2026, 11, 14, 11, 20, tzinfo=timezone.utc))

    run_keys = [request.run_key for request in this_hour + next_hour]
    assert len(run_keys) == len(this_hour) + len(next_hour) >= 4
    assert all(run_keys) and len(set(run_keys)) == len(run_keys)


def test_the_schedules_run_hourly_at_staggered_minutes_and_start_on_their_own():
    assert cloudtrail_hourly.cron_schedule == "20 * * * *"
    assert cloudfront_hourly.cron_schedule == "40 * * * *"
    assert cloudtrail_hourly.default_status == dg.DefaultScheduleStatus.RUNNING
    assert cloudfront_hourly.default_status == dg.DefaultScheduleStatus.RUNNING


def selected_asset_keys(job_name: str) -> set[dg.AssetKey]:
    return set(definitions().resolve_job_def(job_name).asset_layer.executable_asset_keys)


def test_each_schedule_runs_a_job_that_refreshes_raw_and_bronze_of_its_own_source():
    assert cloudtrail_hourly.job_name == cloudtrail_refresh.name
    assert cloudfront_hourly.job_name == cloudfront_refresh.name
    assert selected_asset_keys(cloudtrail_refresh.name) == {
        cloudtrail_files.key,
        cloudtrail_events.key,
    }
    assert selected_asset_keys(cloudfront_refresh.name) == {
        cloudfront_files.key,
        cloudfront_requests.key,
    }


def test_the_activity_table_is_rebuilt_hourly_by_its_own_job_after_bronze_has_had_time_to_refresh():
    assert cloudtrail_activity_hourly.job_name == cloudtrail_activity_refresh.name
    assert selected_asset_keys(cloudtrail_activity_refresh.name) == {cloudtrail_activity.key}
    assert cloudtrail_activity_hourly.default_status == dg.DefaultScheduleStatus.RUNNING
    assert cloudtrail_activity_hourly.cron_schedule == "50 * * * *"


def test_each_activity_tick_runs_the_whole_table_once_and_is_not_deduplicated():
    ticks = [
        run_requests_at(cloudtrail_activity_hourly, datetime(2026, 11, 14, hour, 50, tzinfo=timezone.utc))
        for hour in (10, 11)
    ]

    assert [len(requests) for requests in ticks] == [1, 1]
    assert all(request.partition_key is None for requests in ticks for request in requests)


def test_cur_refreshes_last_month_and_this_month_each_hour():
    requests = run_requests_at(cur_hourly, datetime(2026, 11, 14, 10, 0, tzinfo=timezone.utc))

    assert [request.partition_key for request in requests] == ["2026-10-01", "2026-11-01"]


def test_cur_refreshes_a_new_year_as_december_and_january():
    requests = run_requests_at(cur_hourly, datetime(2027, 1, 1, 0, 0, tzinfo=timezone.utc))

    assert [request.partition_key for request in requests] == ["2026-12-01", "2027-01-01"]


def test_the_cur_schedule_runs_hourly_in_the_slot_the_others_leave_free_and_starts_on_its_own():
    assert cur_hourly.cron_schedule == "0 * * * *"
    assert cur_hourly.default_status == dg.DefaultScheduleStatus.RUNNING


def test_the_cur_schedule_runs_a_job_that_refreshes_raw_and_bronze_of_the_cost_and_usage_report():
    assert cur_hourly.job_name == cur_refresh.name
    assert selected_asset_keys(cur_refresh.name) == {cur_files.key, cur_line_items.key}
