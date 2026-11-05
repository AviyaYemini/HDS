"""Compare the greedy and CP-SAT solvers on random weeks (004 T024, research G13).

Run with ``uv run python -m benchmarks.compare_solvers`` from ``apps/api``. For 10, 30 and 100
employees across three projects, it builds a seeded random week (morning, afternoon and night
needs sized to about five shifts per employee, blocked days, absences and preferences), solves it
with both solvers and prints the places filled, the spread of hours and of nights among those who
all candidates, whether CP-SAT proved its result optimal, and each solver's run time.
"""

import random
import time
import uuid
from dataclasses import dataclass
from datetime import date, timedelta
from datetime import time as clock

from hds.scheduling import Employee, Entry, Problem, Result, Settings, Slot, generate, local_span

ZONE = "Asia/Jerusalem"
SUNDAY = date(2027, 6, 6)
TYPES = {
    "morning": (uuid.UUID(int=11), clock(7), clock(15), False),
    "afternoon": (uuid.UUID(int=12), clock(15), clock(23), False),
    "night": (uuid.UUID(int=13), clock(23), clock(7), True),
}


def week(employees: int, seed: int = 0) -> Problem:
    """A random week for ``employees`` people across three projects."""
    rng = random.Random(seed)
    people = [Employee(uuid.UUID(int=1000 + n), True) for n in range(employees)]
    per_cell = max(1, round(employees * 5 / (3 * 7 * 3)))
    slots = []
    for p in range(3):
        project = uuid.UUID(int=100 + p)
        for d in range(7):
            day = SUNDAY + timedelta(days=d)
            for type_id, start, end, crosses in TYPES.values():
                starts_at, ends_at = local_span(day, start, end, crosses, ZONE)
                slots.append(
                    Slot(
                        key=f"{project}:{type_id}:{day}",
                        shift_id=None,
                        project_id=project,
                        shift_type_id=type_id,
                        starts_at=starts_at,
                        ends_at=ends_at,
                        zone=ZONE,
                        headcount=per_cell,
                        night=crosses,
                    )
                )
    entries = []
    for person in people:
        for d in range(7):
            day = SUNDAY + timedelta(days=d)
            roll = rng.random()
            kind = (
                "blocked"
                if roll < 0.12
                else "open"
                if roll < 0.25
                else "close"
                if roll < 0.35
                else None
            )
            if kind:
                entries.append(Entry(person.id, kind, day, day, None, None))  # type: ignore[arg-type]
        if rng.random() < 0.05:
            entries.append(
                Entry(person.id, "absence", SUNDAY, SUNDAY + timedelta(days=2), None, None)
            )
    return Problem(
        slots=tuple(slots),
        fixed=(),
        candidates=tuple(people),
        entries=tuple(entries),
        settings=Settings(timedelta(hours=11), timedelta(hours=48), ZONE),
        first_day=SUNDAY,
        last_day=SUNDAY + timedelta(days=6),
        night_types=frozenset({TYPES["night"][0]}),
    )


@dataclass(frozen=True)
class Measure:
    """One solver's result on one problem."""

    filled: int
    places: int
    hours_spread: int
    nights_spread: int
    optimal: bool
    seconds: int


def measure(problem: Problem, solver: str, limit: timedelta) -> Measure:
    """Solve ``problem`` with ``solver`` and measure the result."""
    started = time.perf_counter_ns()
    result: Result = generate(problem, solver=solver, time_limit=limit)  # type: ignore[arg-type]
    took = (time.perf_counter_ns() - started) // 1_000_000_000
    hours = [s.minutes // 60 for s in result.summary]
    nights = [s.nights for s in result.summary]
    return Measure(
        filled=sum(len(on) for on in result.assigned.values()),
        places=sum(s.headcount for s in problem.slots),
        hours_spread=max(hours) - min(hours) if hours else 0,
        nights_spread=max(nights) - min(nights) if nights else 0,
        optimal=result.optimal,
        seconds=took,
    )


def main() -> None:
    """Print the comparison table."""
    limit = timedelta(seconds=20)
    print(
        f"{'employees':>9} {'solver':>7} {'filled':>11} {'hours±':>7} {'nights±':>8} "
        f"{'optimal':>8} {'seconds':>8}"
    )
    for size in (10, 30, 100):
        problem = week(size)
        for solver in ("greedy", "cpsat"):
            m = measure(problem, solver, limit)
            print(
                f"{size:>9} {solver:>7} {m.filled:>5}/{m.places:<5} {m.hours_spread:>7} "
                f"{m.nights_spread:>8} {str(m.optimal):>8} {m.seconds:>8}"
            )


if __name__ == "__main__":
    main()
