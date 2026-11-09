"""The CP-SAT model: every hard rule as a constraint, the goals solved in order (004 research G4).

Layer: scheduling (pure). The only module of the package that imports a third-party library, the
CP-SAT solver of OR-Tools (research G2); it imports nothing from the application, a web framework, a
database driver or the network (Constitution III), and ``import scheduling`` does not load it.

The goals are solved one after another in one time budget (004 FR-017, 008 research P4): fill
places; the manager's chips; fewest changes when "Keep changes small"; the employees' wishes and
the hours and nights spread, wishes first when "strictly" and spread first when "gently"; long
runs; fewest changes when "Rebalance freely" (today's order). Each stage keeps the best value of
the ones before as a constraint and starts from their solution, the first from the starting point
(research P6), so the order is strict. The result is optimal only when every stage was proven
optimal.
"""

import math
import time
import uuid
from collections.abc import Mapping, Sequence
from datetime import date, timedelta

from ortools.sat.python import cp_model

from . import chips
from .eligibility import Roster, clash
from .problem import Problem, Slot
from .rules import week_start


def _seconds(length: timedelta) -> int:
    """``length`` in whole seconds, rounded up; shift times are whole minutes in practice."""
    return math.ceil(length.total_seconds())


# How the time budget is shared between the stages: filling places matters most, chips next.
_SHARE = {"filled": 4, "chips": 2}
_DONE = (cp_model.OPTIMAL, cp_model.FEASIBLE)


def solve(
    problem: Problem,
    roster: Roster,
    people: Sequence[uuid.UUID],
    minutes: Mapping[uuid.UUID, int],
    nights: Mapping[uuid.UUID, int],
    deadline_ns: int,
    workers: int | None = None,
) -> tuple[dict[str, list[uuid.UUID]], bool]:
    """Assign people to ``problem``'s slots.

    Args:
        problem: The problem.
        roster: The fixed shifts only; it decides which (person, slot) pairs can exist at all.
        people: The active candidates.
        minutes: Each candidate's minutes already worked in the range (fixed shifts).
        nights: Each candidate's nights already worked in the range.
        deadline_ns: When every stage must be done, on ``time.monotonic_ns()``; the caller starts
            the clock before the model is built, so building counts against the limit.
        workers: Search workers; ``1`` with a fixed seed makes a run repeatable (tests).

    Returns:
        The people chosen for each slot, by key, and whether the result is proven optimal.
    """
    settings = problem.settings
    model = cp_model.CpModel()
    x: dict[tuple[uuid.UUID, str], cp_model.IntVar] = {}
    for s in problem.slots:
        if s.places <= 0:
            continue
        for p in people:
            # A person kept on the slot (a lock, research P2) holds a place already.
            if p not in s.kept and roster.refusal(p, s) is None:
                x[p, s.key] = model.new_bool_var(f"x_{p}_{s.key}")
    chosen: dict[str, list[uuid.UUID]] = {s.key: [] for s in problem.slots}
    if not x:
        return chosen, True

    by_key = {s.key: s for s in problem.slots}
    theirs: dict[uuid.UUID, list[Slot]] = {}
    for p, key in x:
        theirs.setdefault(p, []).append(by_key[key])

    for s in problem.slots:
        on = [x[p, s.key] for p in people if (p, s.key) in x]
        if on:
            model.add(sum(on) <= s.places)

    for p, slots in theirs.items():
        slots.sort(key=lambda s: (s.starts_at, s.key))
        for i, a in enumerate(slots):
            for b in slots[i + 1 :]:
                if b.starts_at >= a.ends_at + settings.min_rest:
                    break
                if clash(a.starts_at, a.ends_at, b.starts_at, b.ends_at, settings.min_rest):
                    model.add(x[p, a.key] + x[p, b.key] <= 1)
        # Seconds, as the rule itself counts (``Roster.refusal``, ``validate``), so the model is
        # never stricter than the rule.
        fixed_week: dict[date, int] = {}
        for booking in roster.bookings(p):
            fixed_week[booking.week] = fixed_week.get(booking.week, 0) + _seconds(booking.length)
        weeks: dict[date, list[Slot]] = {}
        for s in slots:
            weeks.setdefault(week_start(s.starts_at, settings.week_zone), []).append(s)
        limit = int(settings.max_weekly.total_seconds())
        for week, inside in weeks.items():
            model.add(
                sum(_seconds(s.length) * x[p, s.key] for s in inside)
                <= limit - fixed_week.get(week, 0)
            )

    stages = _goals(problem, roster, model, x, theirs, people, minutes, nights)
    shares = [_SHARE.get(name, 1) for name, _, _ in stages]
    optimal = True
    values: dict[tuple[uuid.UUID, str], int] | None = None
    # The first stage starts from the starting point: the drafts, or a rerun's proposal (P6).
    for (p, key), var in x.items():
        model.add_hint(var, int(p in problem.starting.get(key, ())))
    for index, (_, expression, maximise) in enumerate(stages):
        remaining = (deadline_ns - time.monotonic_ns()) / 1e9
        if remaining <= 0.01:
            optimal = False
            break
        if maximise:
            model.maximize(expression)
        else:
            model.minimize(expression)
        if values is not None:
            model.clear_hints()  # type: ignore[no-untyped-call]  # untyped in ortools' stubs
            for pair, var in x.items():
                model.add_hint(var, values[pair])
        solver = cp_model.CpSolver()
        solver.parameters.max_time_in_seconds = remaining * shares[index] / sum(shares[index:])
        solver.parameters.random_seed = 0
        if workers is not None:
            solver.parameters.num_workers = workers
        status = solver.solve(model)
        if status not in _DONE:
            optimal = False
            break
        optimal = optimal and status == cp_model.OPTIMAL
        values = {pair: int(solver.value(var)) for pair, var in x.items()}
        best = round(solver.objective_value)
        model.add(expression >= best if maximise else expression <= best)

    if values is not None:
        for (p, key), value in values.items():
            if value:
                chosen[key].append(p)
    else:
        optimal = False
    return chosen, optimal


def _goals(
    problem: Problem,
    roster: Roster,
    model: cp_model.CpModel,
    x: Mapping[tuple[uuid.UUID, str], cp_model.IntVar],
    theirs: Mapping[uuid.UUID, list[Slot]],
    people: Sequence[uuid.UUID],
    minutes: Mapping[uuid.UUID, int],
    nights: Mapping[uuid.UUID, int],
) -> list[tuple[str, cp_model.LinearExprT, bool]]:
    """The goals in order, each named and with whether it is maximised (see the module docstring).

    A stage with nothing to weigh (no chips that still apply) is left out.
    """
    by_key = {s.key: s for s in problem.slots}
    filled = sum(x.values())

    preference = []
    for (p, key), var in x.items():
        wants, dislikes = roster.preference(p, by_key[key])
        if dislikes != wants:
            preference.append(var if dislikes else -var)

    def spread(
        name: str, totals: Mapping[uuid.UUID, cp_model.LinearExprT], top: int
    ) -> cp_model.LinearExprT:
        """The largest total less the smallest, among people who can take at least one slot.

        Someone who can take none (away all range) has a total no choice changes; counting them
        would pin the smallest and make the spread only the largest.
        """
        working = [total for p, total in totals.items() if theirs.get(p)]
        if not working:
            return 0
        high = model.new_int_var(0, top, f"{name}_max")
        low = model.new_int_var(0, top, f"{name}_min")
        for total in working:
            model.add(high >= total)
            model.add(low <= total)
        result = model.new_int_var(0, top, f"{name}_spread")
        model.add(result == high - low)
        return result

    worked_top = max(minutes.values(), default=0) + sum(s.minutes for s in problem.slots)
    hours = {
        p: minutes.get(p, 0) + sum(s.minutes * x[p, s.key] for s in theirs.get(p, ()))
        for p in people
    }
    night_top = max(nights.values(), default=0) + len(problem.slots)
    night_totals = {
        p: nights.get(p, 0) + sum(x[p, s.key] for s in theirs.get(p, ()) if s.night) for p in people
    }

    runs: list[cp_model.IntVar] = []
    span = problem.max_run + 1
    for p, slots in theirs.items():
        fixed_days = {b.day for b in roster.bookings(p)}
        on_day: dict[date, list[cp_model.IntVar]] = {}
        for s in slots:
            on_day.setdefault(s.day, []).append(x[p, s.key])
        first = min(on_day) - timedelta(days=problem.max_run)
        last = max(on_day) + timedelta(days=problem.max_run)
        worked: dict[date, cp_model.LinearExprT] = {}
        day = first
        while day <= last:
            if day in fixed_days:
                worked[day] = 1
            elif day in on_day:
                flag = model.new_bool_var(f"w_{p}_{day}")
                for var in on_day[day]:
                    model.add(flag >= var)
                worked[day] = flag
            else:
                worked[day] = 0
            day += timedelta(days=1)
        days = sorted(worked)
        for i in range(len(days) - span + 1):
            window = days[i : i + span]
            if not any(d in on_day for d in window):
                continue
            too_long = model.new_bool_var(f"run_{p}_{window[0]}")
            model.add(sum(worked[d] for d in window) - problem.max_run <= too_long)
            runs.append(too_long)

    changes = []
    for (p, key), var in x.items():
        changes.append(1 - var if p in problem.starting.get(key, ()) else var)

    controls = problem.controls
    wishes = ("wishes", sum(preference), False)
    balance = [
        ("hours", spread("hours", hours, worked_top), False),
        ("nights", spread("nights", night_totals, night_top), False),
    ]
    middle = [wishes, *balance] if controls.wishes_strictly else [*balance, wishes]
    fewest = ("changes", sum(changes), False)
    goals: list[tuple[str, cp_model.LinearExprT, bool]] = [("filled", filled, True)]
    against = _chip_terms(problem, model, x, theirs)
    if against is not None:
        goals.append(("chips", against, False))
    if controls.keep_changes_small:
        goals.append(fewest)
    goals += [*middle, ("runs", sum(runs), False)]
    if not controls.keep_changes_small:
        goals.append(fewest)
    return goals


def _chip_terms(
    problem: Problem,
    model: cp_model.CpModel,
    x: Mapping[tuple[uuid.UUID, str], cp_model.IntVar],
    theirs: Mapping[uuid.UUID, list[Slot]],
) -> cp_model.LinearExprT | None:
    """How far a choice goes against the chips that still apply, or None when there are none.

    A cap adds its overflow (in shifts, nights or whole hours); ``keep_on`` and ``keep_off`` add
    each slot given outside or inside what they name; ``prefer`` takes one off for each slot given
    inside (research P5). Every chip weighs the same.
    """
    live = [c for c in problem.chips if chips.applies(c, problem)]
    if not live:
        return None
    terms: list[cp_model.LinearExprT] = []
    for i, chip in enumerate(live):
        mine = [s for s in theirs.get(chip.user_id, ()) if (chip.user_id, s.key) in x]
        if chip.kind in chips.CAPS:
            already = chips.fixed_load(problem, chip)
            load = already + sum(chips.unit(chip, s) * x[chip.user_id, s.key] for s in mine)
            per = 60 if chip.kind == "max_hours" else 1
            # The most the overflow can be: all of it, fixed shifts included, never infeasible.
            top = (already + sum(chips.unit(chip, s) for s in mine)) // per + 1
            over = model.new_int_var(0, top, f"chip_{i}_over")
            model.add(over * per >= load - chips.cap_of(chip))
            terms.append(over)
        elif chip.kind == "keep_on":
            terms += [x[chip.user_id, s.key] for s in mine if not chips.matches(chip, s)]
        elif chip.kind == "keep_off":
            terms += [x[chip.user_id, s.key] for s in mine if chips.matches(chip, s)]
        else:
            terms += [-x[chip.user_id, s.key] for s in mine if chips.matches(chip, s)]
    return sum(terms)
