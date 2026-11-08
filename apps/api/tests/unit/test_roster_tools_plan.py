"""Planned weeks for copy week and templates (007 FR-038, S14, task T064).

Covers: a week copied across a daylight-saving change keeps its local wall-clock times and its
length; a template body round-trips; a person's own times travel as offsets from the shift's start.
"""

import uuid
from datetime import UTC, date, datetime, time, timedelta
from types import SimpleNamespace

from hds.services.roster_tools import (
    PlannedPerson,
    PlannedShift,
    placed,
    plan_from_json,
    plan_to_json,
    to_plan,
)

ZONE = "Asia/Jerusalem"
MORNING = uuid.uuid4()
ANN = uuid.uuid4()


def test_a_planned_shift_keeps_its_local_time_across_the_october_clock_change() -> None:
    """Israel leaves summer time on Sunday 25 October 2026.

    06:00 local is 03:00 UTC the week before and 04:00 UTC the week after.
    """
    plan = PlannedShift(day=1, start=time(6), minutes=480, shift_type_id=MORNING, headcount=1)
    before, _ = placed(plan, date(2026, 10, 18), ZONE)
    after, end = placed(plan, date(2026, 10, 25), ZONE)
    assert before == datetime(2026, 10, 19, 3, tzinfo=UTC)
    assert after == datetime(2026, 10, 26, 4, tzinfo=UTC)
    assert end - after == timedelta(hours=8)


def test_a_stored_shift_becomes_a_plan_with_peoples_own_times_as_offsets() -> None:
    starts = datetime(2026, 11, 9, 4, tzinfo=UTC)  # Monday 06:00 local
    shift = SimpleNamespace(
        starts_at=starts, ends_at=starts + timedelta(hours=8), shift_type_id=MORNING, headcount=2
    )
    people = [
        SimpleNamespace(user_id=ANN, starts_at=starts, ends_at=starts + timedelta(hours=4)),
        SimpleNamespace(user_id=uuid.uuid4(), starts_at=None, ends_at=None),
    ]
    plan = to_plan(shift, ZONE, people)  # type: ignore[arg-type]
    assert (plan.day, plan.start, plan.minutes, plan.headcount) == (1, time(6), 480, 2)
    assert plan.people[0] == PlannedPerson(ANN, 0, 240)
    assert plan.people[1].start_minutes is None


def test_a_template_body_round_trips() -> None:
    plan = [
        PlannedShift(0, time(22), 480, MORNING, 1, (PlannedPerson(ANN, -60, 480),)),
        PlannedShift(5, time(6, 30), 360, MORNING, 2),
    ]
    assert plan_from_json(plan_to_json(plan)) == plan
