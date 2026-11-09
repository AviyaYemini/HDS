"""Roster generation: one signature over both solvers (004 research G3 to G6).

Layer: scheduling (pure). Standard library only; MUST NOT import anything from the application, a
web framework, a database driver or the network (Constitution III). The CP-SAT module, the one that
imports OR-Tools, is loaded only when it is asked for.
"""

import time
import uuid
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Literal

from . import greedy
from .eligibility import Roster, fill, worked_minutes
from .problem import (
    MINIMUM_REST,
    NO_ACTIVE_EMPLOYEE,
    REASON_CODES,
    WEEKLY_MAXIMUM,
    Blocked,
    Changes,
    EmployeeSummary,
    Problem,
    Reason,
    Result,
    SolverError,
    Unfilled,
)
from .problem import minutes as to_minutes
from .rules import local_day, validate
from .types import Employee, RuleWarning, ShiftSlot

Solver = Literal["greedy", "cpsat"]
_HARD = frozenset(
    (
        "warnings.overlap",
        "warnings.rest_too_short",
        "warnings.weekly_hours_exceeded",
        "warnings.blocked",
        "warnings.absence",
        "warnings.employee_inactive",
        "warnings.over_headcount",
    )
)


def slot_id(key: str) -> uuid.UUID:
    """A stable id for a slot that has no shift yet, so ``validate`` can judge it."""
    return uuid.uuid5(uuid.NAMESPACE_URL, f"hds-slot:{key}")


def generate(
    problem: Problem, *, solver: Solver, time_limit: timedelta, workers: int | None = None
) -> Result:
    """Propose who works each slot of ``problem``.

    Both solvers keep every hard rule (spec FR-011 to FR-016). After solving, a repair pass gives
    any place still empty to a candidate who can take it, and the result is judged again by
    ``validate``; every place still empty is explained (research G6).

    Args:
        problem: The slots, the fixed shifts, the candidates and their entries, and the limits.
        solver: ``greedy`` (the baseline) or ``cpsat``.
        time_limit: The CP-SAT budget, counted from this call, so building the model counts too;
            the best solution found by then is returned.
        workers: CP-SAT search workers; ``1`` makes a run repeatable (tests).

    Returns:
        The proposal. A slot's ``assigned`` holds the candidates chosen for it, not its kept people.

    Raises:
        ValueError: An unknown solver, or a slot's own draft also given as a fixed shift.
        SolverError: A solver broke a hard rule (a bug; the result must not be stored).
    """
    if solver not in ("greedy", "cpsat"):
        raise ValueError(f"unknown solver {solver!r}")
    drafts = {s.shift_id for s in problem.slots if s.shift_id is not None}
    if any(shift.id in drafts for shift in problem.fixed):
        raise ValueError("a slot's own draft must not also be a fixed shift; use Slot.kept")
    deadline_ns = time.monotonic_ns() + time_limit // timedelta(microseconds=1) * 1000
    people = [p.id for p in problem.candidates if p.is_active]
    roster = Roster(problem)
    minutes, nights = _fixed_in_range(problem)
    if solver == "greedy":
        chosen = greedy.solve(problem, roster, people, minutes, nights)
        optimal = False
    else:
        from . import cpsat

        chosen, optimal = cpsat.solve(
            problem, roster, people, minutes, nights, deadline_ns, workers
        )
        by_key = {s.key: s for s in problem.slots}
        for key, on in chosen.items():
            for p in on:
                roster.book(p, by_key[key])

    if _repair(problem, roster, chosen, people, minutes):
        optimal = False
    assigned = {key: tuple(sorted(on, key=str)) for key, on in chosen.items()}
    _check(problem, assigned)
    return Result(
        assigned=assigned,
        unfilled=_unfilled(problem, roster, assigned, people),
        summary=_summary(problem, roster, assigned, people, minutes, nights),
        changes=_changes(problem, assigned),
        optimal=optimal,
    )


def _fixed_in_range(problem: Problem) -> tuple[dict[uuid.UUID, int], dict[uuid.UUID, int]]:
    """Minutes and nights each person already works in the range: fixed shifts and kept places."""
    minutes: Counter[uuid.UUID] = Counter()
    nights: Counter[uuid.UUID] = Counter()
    for shift in problem.fixed:
        if not problem.first_day <= local_day(shift.starts_at, shift.zone) <= problem.last_day:
            continue
        for person in shift.assigned:
            minutes[person] += to_minutes(shift.ends_at - shift.starts_at)
            nights[person] += int(shift.shift_type_id in problem.night_types)
    for slot in problem.slots:
        for person in slot.kept:
            minutes[person] += slot.minutes
            nights[person] += int(slot.night)
    return dict(minutes), dict(nights)


def _repair(
    problem: Problem,
    roster: Roster,
    chosen: dict[str, list[uuid.UUID]],
    people: Sequence[uuid.UUID],
    minutes: Mapping[uuid.UUID, int],
) -> bool:
    """Give empty places to anyone who can take them, fewest minutes first; whether any was given.

    A solver stopped by the time limit may leave a place a free candidate could take; after this
    pass, every empty place has a reason for every candidate. The minutes include what the solver
    already chose, so the pass balances real totals.
    """
    worked = worked_minutes(minutes, problem.slots, chosen)
    return fill(
        problem.slots, roster, people, chosen, lambda p, _: (worked.get(p, 0), str(p)), worked
    )


def _check(problem: Problem, assigned: Mapping[str, tuple[uuid.UUID, ...]]) -> None:
    """Judge the proposal with ``validate`` against everything fixed; refuse a hard-rule warning.

    A slot is judged with its kept people and its chosen ones together, so a headcount can never be
    exceeded unseen. Only warnings the proposal causes count: a rule broken by someone it chose, or
    a headcount it filled past. A rule a kept person already broke by hand (003 allows it) is not
    the solver's doing and does not fail the run.
    """
    proposed = [
        ShiftSlot(
            id=slot_id(s.key),
            project_id=s.project_id,
            shift_type_id=s.shift_type_id,
            starts_at=s.starts_at,
            ends_at=s.ends_at,
            zone=s.zone,
            headcount=s.headcount,
            assigned=(*s.kept, *assigned.get(s.key, ())),
        )
        for s in problem.slots
    ]
    everyone: dict[uuid.UUID, Employee] = {p.id: p for p in problem.candidates}
    for shift in [*problem.fixed, *proposed]:
        for person in shift.assigned:
            everyone.setdefault(person, Employee(person, True))
    chosen = {slot_id(key): set(on) for key, on in assigned.items() if on}

    def caused(w: RuleWarning) -> bool:
        if w.employee_id is None:  # a headcount: only when the proposal put someone there
            return any(i in chosen for i in w.shift_ids)
        return any(w.employee_id in chosen.get(i, ()) for i in w.shift_ids)

    broken = [
        w
        for w in validate(
            focus={s.id for s in proposed},
            shifts=[*proposed, *problem.fixed],
            entries=problem.entries,
            employees=everyone,
            settings=problem.settings,
        )
        if w.code in _HARD and caused(w)
    ]
    if broken:
        raise SolverError(f"the proposal breaks hard rules: {broken}")


def _unfilled(
    problem: Problem,
    roster: Roster,
    assigned: Mapping[str, tuple[uuid.UUID, ...]],
    people: Sequence[uuid.UUID],
) -> tuple[Unfilled, ...]:
    """Every slot with empty places, with each candidate not on it counted under one reason."""
    found = []
    for slot in sorted(problem.slots, key=lambda s: (s.starts_at, s.key)):
        on = assigned.get(slot.key, ())
        places = slot.places - len(on)
        if places <= 0:
            continue
        if not people:
            found.append(Unfilled(slot.key, places, (Reason(NO_ACTIVE_EMPLOYEE, 0),)))
            continue
        counts: Counter[str] = Counter()
        named: dict[str, list[Blocked]] = {}
        for p in people:
            if p in on:
                continue
            code = roster.refusal(p, slot)
            if code is None:  # the repair pass leaves no free candidate behind
                raise SolverError(f"{p} could take an empty place on {slot.key}")
            counts[code] += 1
            if code in (MINIMUM_REST, WEEKLY_MAXIMUM):
                found_detail = roster.detail(p, slot, code)
                if found_detail is not None:
                    named.setdefault(code, []).append(found_detail)
        reasons = tuple(
            Reason(c, counts[c], tuple(sorted(named.get(c, ()), key=lambda b: str(b.user_id))))
            for c in REASON_CODES
            if counts[c]
        )
        # Every active candidate is already on the slot: there is no one else to give it to.
        found.append(Unfilled(slot.key, places, reasons or (Reason(NO_ACTIVE_EMPLOYEE, 0),)))
    return tuple(found)


def _summary(
    problem: Problem,
    roster: Roster,
    assigned: Mapping[str, tuple[uuid.UUID, ...]],
    people: Sequence[uuid.UUID],
    minutes: Mapping[uuid.UUID, int],
    nights: Mapping[uuid.UUID, int],
) -> tuple[EmployeeSummary, ...]:
    """Each candidate's minutes, nights and preferences met in the range."""
    worked = {p: minutes.get(p, 0) for p in people}
    night_count = {p: nights.get(p, 0) for p in people}
    met = dict.fromkeys(people, 0)
    unmet = dict.fromkeys(people, 0)
    for slot in problem.slots:
        for p in assigned.get(slot.key, ()):
            worked[p] += slot.minutes
            night_count[p] += int(slot.night)
            wants, dislikes = roster.preference(p, slot)
            met[p] += int(wants)
            unmet[p] += int(dislikes)
    return tuple(
        EmployeeSummary(p, worked[p], night_count[p], met[p], unmet[p])
        for p in sorted(people, key=str)
    )


def _changes(problem: Problem, assigned: Mapping[str, tuple[uuid.UUID, ...]]) -> Changes:
    """What the proposal adds to and removes from the current drafts."""
    kept = {s.key: set(s.kept) for s in problem.slots}
    now = {(key, p) for key, on in assigned.items() for p in on}
    before = {
        (key, p) for key, on in problem.current.items() for p in on if p not in kept.get(key, ())
    }

    def order(pair: tuple[str, uuid.UUID]) -> tuple[str, str]:
        return pair[0], str(pair[1])

    return Changes(
        added=tuple(sorted(now - before, key=order)),
        removed=tuple(sorted(before - now, key=order)),
    )
