"""Property tests: any generated problem gets a proposal that breaks no hard rule (004 T004).

Both solvers run on the same random problems: up to three projects, a week of morning, afternoon and
night slots with headcounts of one to three, up to six candidates (some inactive), random blocked
entries, absences and preferences, fixed shifts of candidates and of others, and random limits. The
result is judged by ``validate``, the rules 003 uses for manual rostering.

Covers: 004 FR-011 to FR-016, FR-020, SC-002. Examples: ``HDS_GENERATE_EXAMPLES`` (200 by default,
1,000 in CI).
"""

import os
import uuid
from datetime import timedelta

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from hds.scheduling import Employee, Entry, Settings, ShiftSlot, validate
from hds.scheduling.engine import generate
from hds.scheduling.problem import NO_ACTIVE_EMPLOYEE, Problem, Result
from tests.unit.scheduling.factory import TYPES, ZONE, day, entry, fixed, person, problem, slot

EXAMPLES = int(os.environ.get("HDS_GENERATE_EXAMPLES", "200"))
HARD = {
    "warnings.overlap",
    "warnings.rest_too_short",
    "warnings.weekly_hours_exceeded",
    "warnings.blocked",
    "warnings.absence",
    "warnings.employee_inactive",
    "warnings.over_headcount",
}
PROJECTS = [uuid.UUID(int=1), uuid.UUID(int=2), uuid.UUID(int=3)]
KINDS = list(TYPES)


@st.composite
def problems(draw: st.DrawFn) -> Problem:
    """A random week."""
    people = [
        person(n, active=draw(st.booleans()) or n == 0) for n in range(draw(st.integers(1, 6)))
    ]
    outsider = person(99)
    cells = draw(
        st.lists(
            st.tuples(st.sampled_from(PROJECTS), st.sampled_from(KINDS), st.integers(0, 6)),
            min_size=1,
            max_size=18,
            unique=True,
        )
    )
    slots = [slot(day(d), kind, project, draw(st.integers(1, 3))) for project, kind, d in cells]
    entries = [
        entry(draw(st.sampled_from(people)), kind, day(d), day(d + span), shift)
        for kind, d, span, shift in draw(
            st.lists(
                st.tuples(
                    st.sampled_from(["blocked", "absence", "open", "close"]),
                    st.integers(-1, 6),
                    st.integers(0, 2),
                    st.sampled_from([None, *KINDS]),
                ),
                max_size=8,
            )
        )
    ]
    entries = [
        e
        if e.kind != "absence"
        else Entry(e.employee_id, "absence", e.starts_on, e.ends_on, None, None)
        for e in entries
    ]
    fixed_shifts = [
        fixed(
            day(d),
            kind,
            draw(st.lists(st.sampled_from([*people, outsider]), max_size=2, unique=True)),
            project,
        )
        for d, kind, project in draw(
            st.lists(
                st.tuples(st.integers(-1, 7), st.sampled_from(KINDS), st.just(uuid.UUID(int=9))),
                max_size=6,
                unique=True,
            )
        )
    ]
    limits = Settings(
        min_rest=timedelta(hours=draw(st.integers(1, 16))),
        max_weekly=timedelta(hours=draw(st.sampled_from([8, 16, 24, 40, 48]))),
        week_zone=ZONE,
    )
    return problem(slots, people, entries, fixed_shifts, settings=limits)


def judged(made: Problem, result: Result) -> list[str]:
    """The hard-rule warnings ``validate`` finds in the proposal, against everything fixed."""
    proposed = [
        ShiftSlot(
            id=uuid.uuid5(uuid.NAMESPACE_URL, s.key),
            project_id=s.project_id,
            shift_type_id=s.shift_type_id,
            starts_at=s.starts_at,
            ends_at=s.ends_at,
            zone=s.zone,
            headcount=s.headcount,
            assigned=result.assigned.get(s.key, ()),
        )
        for s in made.slots
    ]
    everyone: dict[uuid.UUID, Employee] = {p.id: p for p in made.candidates}
    for shift in made.fixed:
        for who in shift.assigned:
            everyone.setdefault(who, Employee(who, True))
    warnings = validate(
        focus={s.id for s in proposed},
        shifts=[*proposed, *made.fixed],
        entries=made.entries,
        employees=everyone,
        settings=made.settings,
    )
    return [w.code for w in warnings if w.code in HARD]


def check(made: Problem, result: Result) -> None:
    """Every property a proposal must have."""
    assert judged(made, result) == []
    active = {p.id for p in made.candidates if p.is_active}
    for s in made.slots:
        on = result.assigned.get(s.key, ())
        assert len(on) <= s.headcount
        assert set(on) <= active
    for gap in result.unfilled:
        on = result.assigned.get(gap.key, ())
        if not active:
            assert gap.reasons[0].code == NO_ACTIVE_EMPLOYEE
            continue
        assert sum(r.employees for r in gap.reasons) == len(active) - len(on)


PROFILE = settings(
    max_examples=EXAMPLES,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)


@PROFILE
@given(problems())
def test_greedy_never_breaks_a_hard_rule(made: Problem) -> None:
    check(made, generate(made, solver="greedy", time_limit=timedelta(seconds=5)))


@PROFILE
@given(problems())
def test_cpsat_never_breaks_a_hard_rule(made: Problem) -> None:
    check(made, generate(made, solver="cpsat", time_limit=timedelta(seconds=5), workers=1))


@PROFILE
@given(problems())
def test_cpsat_fills_at_least_as_many_places_as_greedy(made: Problem) -> None:
    greedy = generate(made, solver="greedy", time_limit=timedelta(seconds=5))
    cpsat = generate(made, solver="cpsat", time_limit=timedelta(seconds=5), workers=1)

    def filled(result: Result) -> int:
        return sum(len(on) for on in result.assigned.values())

    if cpsat.optimal:
        assert filled(cpsat) >= filled(greedy)
