"""Run the real schedule worker with one stage slowed down, for the drain tests (006 T030).

Usage (from ``apps/api``, with the worker's settings in the environment):
    python -m tests.worker_process <slow-solve|slow-build> <marker file>

``slow-solve`` makes each solve take 3 s more; ``slow-build`` holds a job for 3 s after building
its problem, before the solve. Either writes the marker file when it reaches that point, so a test
knows when to send its signal. Everything else is the worker's own code, run by ``hds.worker.main``.

Covers: none directly; support for ``tests/integration/test_worker_drain.py``.
"""

import asyncio
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

import hds.worker as worker
from hds.scheduling import Result
from hds.services import schedule_job
from hds.services.schedule_inputs import Built
from hds.services.schedule_job import Claim, ScheduleJobService

STALL_SECONDS = 3


def slow_solve(marker: Path) -> None:
    """Make each solve take ``STALL_SECONDS`` longer, after writing the marker."""
    real = schedule_job.solve

    async def solve(claim: Claim, built: Built, time_limit: timedelta, **follow: Any) -> Result:
        # ``follow`` carries the best-so-far and stop hooks (008 FR-027, FR-028) to the real solve.
        marker.write_text("solving")
        await asyncio.sleep(STALL_SECONDS)
        return await real(claim, built, time_limit, **follow)

    setattr(worker, "solve", solve)  # the worker's own reference to the solver


def slow_build(marker: Path) -> None:
    """Hold each job ``STALL_SECONDS`` after building its problem, after writing the marker."""
    real = ScheduleJobService.build

    async def build(self: ScheduleJobService, claim: Claim) -> Any:
        built = await real(self, claim)
        marker.write_text("built")
        await asyncio.sleep(STALL_SECONDS)
        return built

    ScheduleJobService.build = build  # type: ignore[method-assign]


def main() -> None:
    """Slow the chosen stage, then run the worker as ``python -m hds.worker`` would."""
    mode, marker = sys.argv[1], Path(sys.argv[2])
    {"slow-solve": slow_solve, "slow-build": slow_build}[mode](marker)
    worker.main()


if __name__ == "__main__":
    main()
