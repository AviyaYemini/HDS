"""Each person's own times in a shift, and refusing stale roster changes (007 FR-027, FR-041).

Covers the 4-hour reach either side of the shift (clarification Q3), the pairing and order of the
times, and the ETag check: a stale ``If-Match`` is refused, none skips the check.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from hds.core.exceptions import DomainValidationError, PreconditionFailedError
from hds.services.roster import _refuse_bad_own_times, _refuse_stale, etag

START = datetime(2026, 11, 8, 4, tzinfo=UTC)  # 06:00 in Israel
END = START + timedelta(hours=8)


def _shift(updated: datetime | None = None) -> MagicMock:
    shift = MagicMock()
    shift.starts_at, shift.ends_at = START, END
    shift.updated_at = updated or datetime(2026, 11, 7, 12, tzinfo=UTC)
    return shift


@pytest.mark.parametrize(
    ("starts_at", "ends_at"),
    [
        (START, START + timedelta(hours=4)),  # a half shift
        (START, END + timedelta(hours=2)),  # two extra hours
        (START - timedelta(hours=4), END + timedelta(hours=4)),  # the full reach either side
        (None, None),  # the shift's own times
    ],
)
def test_own_times_within_four_hours_of_the_shift_are_accepted(
    starts_at: datetime | None, ends_at: datetime | None
) -> None:
    _refuse_bad_own_times(_shift(), starts_at, ends_at)


@pytest.mark.parametrize(
    ("starts_at", "ends_at"),
    [
        (START - timedelta(hours=4, minutes=1), END),
        (START, END + timedelta(hours=4, minutes=1)),
    ],
)
def test_own_times_beyond_four_hours_are_refused(starts_at: datetime, ends_at: datetime) -> None:
    with pytest.raises(DomainValidationError) as refused:
        _refuse_bad_own_times(_shift(), starts_at, ends_at)
    assert refused.value.code == "assignment.times_out_of_range"


@pytest.mark.parametrize(
    ("starts_at", "ends_at"), [(START, None), (None, END), (END, START), (START, START)]
)
def test_half_given_or_backwards_times_are_refused(
    starts_at: datetime | None, ends_at: datetime | None
) -> None:
    with pytest.raises(DomainValidationError) as refused:
        _refuse_bad_own_times(_shift(), starts_at, ends_at)
    assert refused.value.code == "assignment.times_invalid"


def test_the_etag_follows_the_shifts_last_change() -> None:
    before = _shift()
    after = _shift(before.updated_at + timedelta(seconds=1))
    assert etag(before) != etag(after)
    assert etag(before) == etag(_shift(before.updated_at))


def test_a_stale_if_match_is_refused_and_none_skips_the_check() -> None:
    shift = _shift()
    _refuse_stale(shift, None)
    _refuse_stale(shift, etag(shift))
    with pytest.raises(PreconditionFailedError) as refused:
        _refuse_stale(shift, '"2000-01-01T00:00:00+00:00"')
    assert refused.value.code == "roster.changed"
