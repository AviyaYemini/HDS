"""The work log's key figures are exact sums of its rows (007 FR-050, task T087)."""

import uuid
from datetime import date
from decimal import Decimal

from hds.services.reports import WorkLog, WorkLogRow

ANN, BOB = uuid.uuid4(), uuid.uuid4()


def _row(who: uuid.UUID, shifts: int, hours: str) -> WorkLogRow:
    return WorkLogRow(
        who, "x", uuid.uuid4(), "p", shifts, Decimal(hours), date(2026, 11, 1), date(2026, 11, 7)
    )


def test_people_shifts_and_hours_sum_the_rows() -> None:
    log = WorkLog(
        date(2026, 11, 1),
        date(2026, 11, 7),
        [_row(ANN, 2, "7.50"), _row(ANN, 1, "8.25"), _row(BOB, 3, "0.10")],
    )
    assert (log.people, log.shifts, log.hours) == (2, 6, Decimal("15.85"))


def test_an_empty_log_has_zero_figures() -> None:
    log = WorkLog(date(2026, 11, 1), date(2026, 11, 7), [])
    assert (log.people, log.shifts, log.hours) == (0, 0, Decimal(0))
