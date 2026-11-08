"""Pricing one employee on one shift: the hours at each rate and the gross pay.

Layer: payroll (pure). Standard library only; MUST NOT import anything from the application, a web
framework, a database driver or the network (005 research R2).

The shift is cut at every instant where a rate can change: the start and end of each weekly rest
window, each local midnight (special days are local dates), and the two overtime boundaries, which
are measured in real elapsed time from the start so that overtime is the last hours of the shift
(005 FR-011). Each part then takes one rate (FR-012, FR-013). Hours are real hours, so a night on
which the clocks change lasts 7 or 9 hours (FR-009). Pay is summed exactly and rounded half up to
the agora once per line (FR-014).

A local wall time becomes an instant as "the first moment the site's clock shows that time or
later": a time the clocks skip (spring forward) starts at the jump, and a time the clocks show twice
(fall back) counts from its first showing.
"""

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta, tzinfo
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal, localcontext
from itertools import pairwise
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .settings import PaySettings, RestWindow, SpecialDay

MICROS_PER_HOUR = 3_600_000_000
NIGHT_START, NIGHT_END = time(22, 0), time(6, 0)
NIGHT_MINIMUM = timedelta(hours=2)
"""A shift is a night shift when at least this much of it falls between 22:00 and 06:00 (FR-010)."""
HUNDRED = Decimal(100)
CENT = Decimal("0.01")
_MICRO = timedelta(microseconds=1)
_WEEK = timedelta(days=7)


@dataclass(frozen=True)
class PayPart:
    """Time of the shift paid at one rate, in percent of the base rate.

    ``pay`` is the part's share of the line's pay, to the agora: the parts of a line add up to its
    pay exactly (see :func:`split_pay`). ``None`` when the line has no pay.
    """

    rate_pct: Decimal
    microseconds: int
    pay: Decimal | None = None

    @property
    def seconds(self) -> int:
        """The part's length in whole seconds."""
        return self.microseconds // 1_000_000

    @property
    def hours(self) -> Decimal:
        """The part's length in hours (28 significant digits; exact for whole minutes up to 2)."""
        return Decimal(self.microseconds) / MICROS_PER_HOUR


@dataclass(frozen=True)
class PayLine:
    """One employee on one shift.

    ``parts`` holds the time at each rate, merged by rate in the order the rates first occur; their
    microseconds add up to ``microseconds``, the shift's real length. ``pay`` is ``None`` exactly
    when there is no base rate: missing pay is never shown as zero (FR-004). Shown hours are rounded
    by whoever shows them; totals are summed from lines, so they always match what is shown.
    """

    microseconds: int
    parts: tuple[PayPart, ...]
    pay: Decimal | None
    night: bool

    @property
    def hours(self) -> Decimal:
        """The shift's real length in hours."""
        return Decimal(self.microseconds) / MICROS_PER_HOUR

    @property
    def rate_missing(self) -> bool:
        """Whether the shift had no base rate, so no pay."""
        return self.pay is None


def price(
    starts_at: datetime,
    ends_at: datetime,
    timezone: str,
    base_rate: Decimal | None,
    settings: PaySettings,
    special_days: Iterable[SpecialDay],
) -> PayLine:
    """Price a shift for one employee.

    Args:
        starts_at: The shift's start, timezone-aware.
        ends_at: The shift's end, timezone-aware and after the start.
        timezone: The IANA zone of the shift's site; rest windows, nights and special days are read
            at local wall time there.
        base_rate: The hourly rate: the shift's own rate if set, otherwise the employee's default
            (the caller chooses, FR-004); ``None`` when neither is set.
        settings: The effective premium settings of the shift's project.
        special_days: The special days in effect for the shift's project, at most one per date: the
            caller has already let a project's day replace the organisation's (FR-008a).

    Returns:
        The hours, the time at each rate and the pay.

    Raises:
        ValueError: A naive datetime, an end not after the start, an unknown time zone, or two
            special days on one date.
    """
    if starts_at.tzinfo is None or ends_at.tzinfo is None:
        raise ValueError("starts_at and ends_at must be timezone-aware")
    start, end = starts_at.astimezone(UTC), ends_at.astimezone(UTC)
    if end <= start:
        raise ValueError("a shift must end after it starts")
    try:
        zone = ZoneInfo(timezone)
    except (ZoneInfoNotFoundError, ValueError) as error:
        raise ValueError(f"unknown time zone {timezone!r}") from error
    rates: dict[date, Decimal] = {}
    for special in special_days:
        if special.day in rates:
            raise ValueError(f"two special days on {special.day}")
        rates[special.day] = special.rate_pct
    first = start.astimezone(zone).date()
    last = end.astimezone(zone).date()

    night = _night_time(start, end, zone, first, last) >= NIGHT_MINIMUM
    normal = timedelta(
        minutes=settings.night_normal_minutes if night else settings.day_normal_minutes
    )
    overtime1 = start + normal
    overtime2 = overtime1 + timedelta(minutes=settings.tier1_minutes)
    rests = _rest_windows(settings.rest, zone, first, last)

    cuts = {overtime1, overtime2}
    cuts.update(instant for window in rests for instant in window)
    cuts.update(_at(d, time(0), zone) for d in _days(first, last + timedelta(days=1)))
    points = [start, *sorted(c for c in cuts if start < c < end), end]

    merged: dict[Decimal, int] = {}
    for a, b in pairwise(points):
        day_rate = HUNDRED
        local_day = a.astimezone(zone).date()
        if local_day in rates:
            day_rate = max(day_rate, rates[local_day])
        if any(s <= a < e for s, e in rests):
            day_rate = max(day_rate, settings.rest_pct)
        if a < overtime1:
            rate = day_rate
        elif day_rate == HUNDRED:
            rate = settings.overtime1_pct if a < overtime2 else settings.overtime2_pct
        else:
            increase = settings.increase1_points if a < overtime2 else settings.increase2_points
            rate = day_rate + increase
        merged[rate] = merged.get(rate, 0) + (b - a) // _MICRO

    parts = tuple(PayPart(rate, micros) for rate, micros in merged.items())
    pay: Decimal | None = None
    if base_rate is not None:
        with localcontext() as ctx:
            ctx.prec = 60
            exacts = [
                Decimal(p.microseconds) * p.rate_pct * base_rate / (MICROS_PER_HOUR * HUNDRED)
                for p in parts
            ]
            pay = sum(exacts, Decimal(0)).quantize(CENT, rounding=ROUND_HALF_UP)
            shares = split_pay(exacts, pay)
        parts = tuple(
            PayPart(p.rate_pct, p.microseconds, share)
            for p, share in zip(parts, shares, strict=True)
        )
    return PayLine(
        microseconds=sum(p.microseconds for p in parts), parts=parts, pay=pay, night=night
    )


def split_pay(exacts: list[Decimal], pay: Decimal) -> list[Decimal]:
    """Each part's share of ``pay``, to the agora, adding up to ``pay`` exactly (FR-014, FR-024).

    ``pay`` is the line's pay: the exact sum of ``exacts`` rounded half up to the agora, once, as
    before. Each part is cut down to the agora and the agorot left over go, one each, to the parts
    that lost the most, the first of them on a tie (largest remainder). So no part moves by more
    than an agora from its exact amount, and the rows a screen or an export shows always add up to
    the line, with no pay arithmetic of its own.

    Args:
        exacts: Each part's exact pay, not rounded, in the parts' order.
        pay: The line's pay.

    Returns:
        The parts' shares, in the same order.
    """
    floors = [e.quantize(CENT, rounding=ROUND_DOWN) for e in exacts]
    left = int((pay - sum(floors, Decimal(0))) / CENT)
    order = sorted(range(len(exacts)), key=lambda i: floors[i] - exacts[i])
    for i in order[: max(left, 0)]:
        floors[i] += CENT
    return floors


def _days(first: date, last: date) -> list[date]:
    """Every date from ``first`` to ``last``, both included."""
    return [first + timedelta(days=n) for n in range((last - first).days + 1)]


def _wall(instant: datetime, zone: tzinfo) -> datetime:
    """The site's clock at ``instant``, without a zone."""
    return instant.astimezone(zone).replace(tzinfo=None)


def _first_showing(wall: datetime, zone: tzinfo) -> datetime:
    """The first instant at which the site's clock shows ``wall`` or later, in UTC.

    A wall time that exists maps to its earlier instant (the first of two when the clocks go back).
    One the clocks skip maps to the moment of the jump, found by bisection to the microsecond, so
    exactly (a datetime holds nothing finer).
    """
    candidates = sorted(wall.replace(tzinfo=zone, fold=f).astimezone(UTC) for f in (0, 1))
    if _wall(candidates[0], zone) == wall:
        return candidates[0]
    low, high = candidates  # the clock reads before ``wall`` at ``low``, after it at ``high``
    while high - low > _MICRO:
        middle = low + (high - low) / 2
        if _wall(middle, zone) >= wall:
            high = middle
        else:
            low = middle
    return high


def _at(day: date, at: time, zone: tzinfo) -> datetime:
    """The first instant the site's clock shows ``at`` on ``day``, in UTC."""
    return _first_showing(datetime.combine(day, at), zone)


def _sunday_index(day: date) -> int:
    """0 for Sunday to 6 for Saturday."""
    return (day.weekday() + 1) % 7


def _night_time(start: datetime, end: datetime, zone: tzinfo, first: date, last: date) -> timedelta:
    """How much of the shift falls between 22:00 and 06:00 local time."""
    total = timedelta(0)
    for day in _days(first - timedelta(days=1), last):
        night_start = _at(day, NIGHT_START, zone)
        night_end = _at(day + timedelta(days=1), NIGHT_END, zone)
        overlap = min(end, night_end) - max(start, night_start)
        if overlap > timedelta(0):
            total += overlap
    return total


def _in_week(weekday: int, at: time) -> timedelta:
    """How far ``at`` on ``weekday`` is from Sunday 00:00, to the microsecond."""
    return timedelta(
        days=weekday,
        hours=at.hour,
        minutes=at.minute,
        seconds=at.second,
        microseconds=at.microsecond,
    )


def _rest_windows(
    rest: RestWindow, zone: tzinfo, first: date, last: date
) -> list[tuple[datetime, datetime]]:
    """Every weekly rest window that can touch the dates ``first`` to ``last``, as UTC instants."""
    length = (
        _in_week(rest.end_weekday, rest.end_time) - _in_week(rest.start_weekday, rest.start_time)
    ) % _WEEK
    if not length:
        return []
    windows = []
    for day in _days(first - timedelta(days=7), last):
        if _sunday_index(day) == rest.start_weekday:
            wall_start = datetime.combine(day, rest.start_time)
            # Wall-clock arithmetic: the window ends when the clock shows its end time.
            windows.append(
                (_first_showing(wall_start, zone), _first_showing(wall_start + length, zone))
            )
    return windows
