"""A temporary worker works only on their projects, between their dates (CEO demo).

A run never offers them a slot outside their engagement; a hand assignment there is a warning.
"""

import uuid
from datetime import UTC, datetime, timedelta

from hds.scheduling import Employee, Engagement, ShiftSlot, validate
from hds.scheduling.eligibility import Roster
from hds.scheduling.problem import UNAVAILABLE
from tests.unit.scheduling.factory import (
    OTHER_PROJECT,
    PROJECT,
    SETTINGS,
    ZONE,
    day,
    problem,
    slot,
)

TEMP = Employee(
    id=uuid.UUID(int=2001),
    is_active=True,
    engagement=Engagement(frozenset({PROJECT}), day(1), day(3)),
)
STAFF = Employee(id=uuid.UUID(int=2002), is_active=True)


def test_a_temporary_worker_may_take_a_slot_on_their_project_within_their_dates() -> None:
    inside = slot(day(2))
    assert Roster(problem([inside], [TEMP])).refusal(TEMP.id, inside) is None


def test_another_project_or_a_day_outside_the_dates_is_unavailable() -> None:
    for outside in (slot(day(2), project=OTHER_PROJECT), slot(day(0)), slot(day(4))):
        roster = Roster(problem([outside], [TEMP]))
        assert roster.refusal(TEMP.id, outside) == UNAVAILABLE, outside


def test_permanent_staff_are_not_limited() -> None:
    anywhere = slot(day(5), project=OTHER_PROJECT)
    assert Roster(problem([anywhere], [STAFF])).refusal(STAFF.id, anywhere) is None


def _shift(on: int, project: uuid.UUID = PROJECT) -> ShiftSlot:
    """A morning shift ``on`` days after the factory's Sunday, with the temporary worker on it."""
    start = datetime.combine(day(on), datetime.min.time(), UTC) + timedelta(hours=4)
    return ShiftSlot(
        id=uuid.uuid4(),
        project_id=project,
        shift_type_id=uuid.UUID(int=11),
        starts_at=start,
        ends_at=start + timedelta(hours=8),
        zone=ZONE,
        headcount=1,
        assigned=(TEMP.id,),
    )


def _warnings(shift: ShiftSlot) -> list[str]:
    found = validate(
        focus=[shift.id], shifts=[shift], entries=[], employees={TEMP.id: TEMP}, settings=SETTINGS
    )
    return [w.code for w in found]


def test_a_hand_assignment_outside_the_engagement_is_a_warning() -> None:
    assert _warnings(_shift(2, OTHER_PROJECT)) == ["warnings.temporary_outside"]
    assert _warnings(_shift(5)) == ["warnings.temporary_outside"]


def test_a_hand_assignment_inside_the_engagement_is_not() -> None:
    assert _warnings(_shift(2)) == []
