"""How long a schedule job has searched, by the server's clock (007 FR-074, group E review)."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast

from hds.models.schedule_job import ScheduleJob
from hds.services.schedule_job import ScheduleJobService

START = datetime(2026, 11, 8, 10, 0, tzinfo=UTC)


def _service(now: datetime) -> ScheduleJobService:
    unused = cast(Any, None)
    return ScheduleJobService(
        unused, unused, unused, unused, unused, unused, unused, unused, now=lambda: now
    )


def _job(started: datetime | None, finished: datetime | None) -> ScheduleJob:
    return cast(ScheduleJob, SimpleNamespace(started_at=started, finished_at=finished))


def test_a_queued_job_has_searched_nothing() -> None:
    assert _service(START).elapsed_seconds(_job(None, None)) is None


def test_a_running_job_counts_to_now_and_a_finished_one_to_its_end() -> None:
    later = START + timedelta(milliseconds=7260)
    assert _service(later).elapsed_seconds(_job(START, None)) == 7
    ended = START + timedelta(seconds=3)
    assert _service(later).elapsed_seconds(_job(START, ended)) == 3


def test_a_clock_behind_the_start_never_reads_negative() -> None:
    assert _service(START - timedelta(seconds=2)).elapsed_seconds(_job(START, None)) == 0
