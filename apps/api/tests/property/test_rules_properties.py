"""For any shifts of one employee, the rules report exactly the breaks there are (T032).

The reference checks below are written independently of ``hds.scheduling`` from the rule texts:
two shifts overlap when each starts before the other ends; rest is the gap between one's end and
the next's start; a week runs Sunday to Saturday in the organisation's zone and a shift counts in
the week it starts; a blocked entry or absence covers a shift whose start date, shift type and
weekday it matches.

Covers: 003 FR-116, SC-103; docs/hds-spec.md section 5, H1 to H4; Constitution IV.
"""

import uuid
from datetime import UTC, date, datetime, timedelta
from typing import cast
from zoneinfo import ZoneInfo

from hypothesis import given, settings
from hypothesis import strategies as st

from hds.scheduling import Employee, Entry, RuleWarning, Settings, ShiftSlot, validate
from hds.scheduling.types import EntryKind

ZONE = "Asia/Jerusalem"
REST = timedelta(hours=11)
WEEKLY = timedelta(hours=48)
SETTINGS = Settings(min_rest=REST, max_weekly=WEEKLY, week_zone=ZONE)
ANN = uuid.uuid4()
PEOPLE = {ANN: Employee(id=ANN, is_active=True)}
TYPES = (uuid.uuid4(), uuid.uuid4())
START = datetime(2026, 6, 1, tzinfo=UTC)

slots = st.lists(
    st.tuples(
        st.integers(0, 21 * 24 * 4),  # quarter hours from the start of June
        st.integers(1, 12 * 4),  # length in quarter hours
        st.sampled_from(TYPES),
    ),
    min_size=1,
    max_size=10,
)


def build(raw: list[tuple[int, int, uuid.UUID]]) -> list[ShiftSlot]:
    return [
        ShiftSlot(
            id=uuid.uuid4(),
            project_id=uuid.uuid4(),
            shift_type_id=kind,
            starts_at=START + timedelta(minutes=15 * start),
            ends_at=START + timedelta(minutes=15 * (start + length)),
            zone=ZONE,
            headcount=1,
            assigned=(ANN,),
        )
        for start, length, kind in raw
    ]


def local_day(moment: datetime) -> date:
    return moment.astimezone(ZoneInfo(ZONE)).date()


def week_of(moment: datetime) -> date:
    day = local_day(moment)
    return day - timedelta(days=(day.weekday() + 1) % 7)


def pairs(found: list[RuleWarning], code: str) -> set[frozenset[uuid.UUID]]:
    return {frozenset(w.shift_ids) for w in found if w.code == code}


@settings(max_examples=300, deadline=None)
@given(slots)
def test_every_overlap_and_short_rest_is_reported_and_nothing_else(
    raw: list[tuple[int, int, uuid.UUID]],
) -> None:
    shifts = build(raw)

    found = validate(
        focus={s.id for s in shifts},
        shifts=shifts,
        entries=(),
        employees=PEOPLE,
        settings=SETTINGS,
    )

    overlaps, short = set(), set()
    for i, a in enumerate(shifts):
        for b in shifts[i + 1 :]:
            if a.starts_at < b.ends_at and b.starts_at < a.ends_at:
                overlaps.add(frozenset((a.id, b.id)))
    # Rest is between a shift and the next one that starts after it ends, with no shift of the
    # employee starting in between.
    for a in shifts:
        later = [b for b in shifts if b.starts_at >= a.ends_at]
        if not later:
            continue
        nearest = min(b.starts_at for b in later)
        if any(a.starts_at < b.starts_at < nearest for b in shifts if b is not a):
            continue
        for b in later:
            if b.starts_at == nearest and b.starts_at - a.ends_at < REST:
                short.add(frozenset((a.id, b.id)))
    assert pairs(found, "warnings.overlap") == overlaps
    assert pairs(found, "warnings.rest_too_short") == short


@settings(max_examples=300, deadline=None)
@given(slots)
def test_every_week_over_the_maximum_is_reported_with_its_shifts(
    raw: list[tuple[int, int, uuid.UUID]],
) -> None:
    shifts = build(raw)

    found = validate(
        focus={s.id for s in shifts},
        shifts=shifts,
        entries=(),
        employees=PEOPLE,
        settings=SETTINGS,
    )

    weeks: dict[date, list[ShiftSlot]] = {}
    for s in shifts:
        weeks.setdefault(week_of(s.starts_at), []).append(s)
    expected = {
        frozenset(s.id for s in group)
        for group in weeks.values()
        if sum((s.ends_at - s.starts_at for s in group), timedelta()) > WEEKLY
    }
    assert pairs(found, "warnings.weekly_hours_exceeded") == expected


entry_ranges = st.lists(
    st.tuples(
        st.sampled_from(["blocked", "absence"]),
        st.integers(0, 20),  # first day offset
        st.integers(0, 6),  # length in days
        st.one_of(st.none(), st.sampled_from(TYPES)),
        st.one_of(st.none(), st.integers(0, 6)),
    ),
    max_size=5,
)


@settings(max_examples=300, deadline=None)
@given(slots, entry_ranges)
def test_every_shift_a_blocked_entry_or_absence_covers_is_reported_and_no_other(
    raw: list[tuple[int, int, uuid.UUID]],
    raw_entries: list[tuple[str, int, int, uuid.UUID | None, int | None]],
) -> None:
    shifts = build(raw)
    first = date(2026, 6, 1)
    entries = tuple(
        Entry(
            ANN,
            cast(EntryKind, kind),
            first + timedelta(days=offset),
            first + timedelta(days=offset + length),
            None if kind == "absence" else shift_type,
            None if kind == "absence" else weekday,
        )
        for kind, offset, length, shift_type, weekday in raw_entries
    )

    found = validate(
        focus={s.id for s in shifts},
        shifts=shifts,
        entries=entries,
        employees=PEOPLE,
        settings=SETTINGS,
    )

    def covered(s: ShiftSlot, kind: str) -> bool:
        day = local_day(s.starts_at)
        return any(
            e.kind == kind
            and e.starts_on <= day <= e.ends_on
            and e.shift_type_id in (None, s.shift_type_id)
            and e.weekday in (None, (day.weekday() + 1) % 7)
            for e in entries
        )

    for code, kind in (("warnings.blocked", "blocked"), ("warnings.absence", "absence")):
        reported = {next(iter(w.shift_ids)) for w in found if w.code == code}
        assert reported == {s.id for s in shifts if covered(s, kind)}
