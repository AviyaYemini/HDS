"""Property tests: pricing a shift agrees with a minute-by-minute reading of the rules (005 T003).

``price`` cuts a shift at instants. The oracle here walks the shift one real minute at a time and
asks, for each minute, which local date and wall time it falls on, whether that is inside the weekly
rest, and how many minutes of the shift came before it; it shares no code with ``price``. Shifts
start on a whole minute and last 1 minute to 24 hours, in zones with and without daylight saving;
settings, the rest window and special days are random. Rest windows last at least 3 hours and may
start or end at any time, including times the clocks skip or show twice; zones include ones whose
clocks change at midnight. The oracle follows the site's clock forward and notes each time it
reaches a window's start or end, so a skipped time counts from the jump and a repeated time from its
first showing.

Covers: 005 FR-009 to FR-015, SC-002. Examples: ``HDS_GENERATE_EXAMPLES`` (200 by default, 1,000 in
CI).
"""

import os
from collections import defaultdict
from datetime import UTC, date, datetime, time, timedelta
from decimal import ROUND_HALF_UP, Decimal
from zoneinfo import ZoneInfo

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from hds.payroll import PayLine, PaySettings, RestWindow, SpecialDay, price

EXAMPLES = int(os.environ.get("HDS_GENERATE_EXAMPLES", "200"))
ZONES = [
    "Asia/Jerusalem",
    "Europe/London",
    "America/New_York",
    "UTC",
    "Australia/Lord_Howe",
    "America/Havana",
    "America/Santiago",
]
MINUTE = timedelta(minutes=1)
WEEK = 7 * 24 * 60
CENT = Decimal("0.01")


def percent(low: int, high: int) -> st.SearchStrategy[Decimal]:
    """A percentage with two decimals between ``low`` and ``high``."""
    return st.integers(low * 100, high * 100).map(lambda n: Decimal(n) / 100)


@st.composite
def rest_windows(draw: st.DrawFn) -> RestWindow:
    """A weekly window of at least 3 hours, starting and ending at any minute of the week."""
    start = draw(st.integers(0, WEEK - 1))
    end = (start + draw(st.integers(180, WEEK - 1))) % WEEK
    return RestWindow(
        start // 1440,
        time(start % 1440 // 60, start % 60),
        end // 1440,
        time(end % 1440 // 60, end % 60),
    )


@st.composite
def pay_settings(draw: st.DrawFn) -> PaySettings:
    """Any valid settings."""
    return PaySettings(
        day_normal_minutes=draw(st.integers(1, 1440)),
        night_normal_minutes=draw(st.integers(1, 1440)),
        tier1_minutes=draw(st.integers(1, 1440)),
        overtime1_pct=draw(percent(100, 300)),
        overtime2_pct=draw(percent(100, 300)),
        rest=draw(rest_windows()),
        rest_pct=draw(percent(100, 300)),
        increase1_points=draw(percent(1, 200)),
        increase2_points=draw(percent(1, 200)),
    )


@st.composite
def shifts(draw: st.DrawFn) -> tuple[datetime, datetime, str]:
    """A shift of 1 minute to 24 hours, starting on a whole minute, and its site's zone."""
    minutes = draw(st.integers(0, 3 * 366 * 24 * 60))
    start = datetime(2026, 1, 1, tzinfo=UTC) + minutes * MINUTE
    length = draw(st.integers(1, 24 * 60))
    return start, start + length * MINUTE, draw(st.sampled_from(ZONES))


def sunday_index(day: date) -> int:
    """0 for Sunday to 6 for Saturday."""
    return (day.weekday() + 1) % 7


def position(local: datetime) -> int:
    """A local wall time as a minute of the week, 0 at Sunday 00:00."""
    return sunday_index(local.date()) * 1440 + local.hour * 60 + local.minute


def crossed(previous: int, current: int, target: int) -> bool:
    """Whether the clock reached ``target`` going forward from ``previous`` to ``current``."""
    step = (current - previous) % WEEK
    return step < WEEK // 2 and 0 < (target - previous) % WEEK <= step


class RestClock:
    """Follows the site's clock and says whether it is inside the weekly rest."""

    def __init__(self, rest: RestWindow, tz: ZoneInfo, start: datetime) -> None:
        self.start = rest.start_weekday * 1440 + rest.start_time.hour * 60 + rest.start_time.minute
        self.end = rest.end_weekday * 1440 + rest.end_time.hour * 60 + rest.end_time.minute
        self.tz, self.inside = tz, False
        self.previous = position((start - timedelta(days=8)).astimezone(tz))
        moment = start - timedelta(days=8)
        while moment < start:  # long enough to have passed both ends at least once
            moment += timedelta(minutes=15)
            self.move(moment)

    def move(self, moment: datetime) -> bool:
        """Advance the clock to ``moment``; return whether that moment is inside the rest."""
        current = position(moment.astimezone(self.tz))
        # Both ends can fall inside one step; apply them in the order the clock reaches them.
        reached = [
            ((target - self.previous) % WEEK, inside)
            for target, inside in ((self.start, True), (self.end, False))
            if crossed(self.previous, current, target)
        ]
        for _, inside in sorted(reached):
            self.inside = inside
        self.previous = current
        return self.inside


def oracle(
    start: datetime, end: datetime, zone: str, cfg: PaySettings, days: dict[date, Decimal]
) -> dict[Decimal, int]:
    """Minutes at each rate, one real minute at a time."""
    tz = ZoneInfo(zone)
    total = int((end - start) / MINUTE)
    night = sum(1 for i in range(total) if not (6 <= (start + i * MINUTE).astimezone(tz).hour < 22))
    normal = cfg.night_normal_minutes if night >= 120 else cfg.day_normal_minutes
    minutes: dict[Decimal, int] = defaultdict(int)
    clock = RestClock(cfg.rest, tz, start)
    for i in range(total):
        local = (start + i * MINUTE).astimezone(tz)
        resting = clock.inside if i == 0 else clock.move(start + i * MINUTE)
        day = Decimal(100)
        if local.date() in days:
            day = max(day, days[local.date()])
        if resting:
            day = max(day, cfg.rest_pct)
        tier = 0 if i < normal else 1 if i < normal + cfg.tier1_minutes else 2
        if tier == 0:
            rate = day
        elif day == 100:
            rate = cfg.overtime1_pct if tier == 1 else cfg.overtime2_pct
        else:
            rate = day + (cfg.increase1_points if tier == 1 else cfg.increase2_points)
        minutes[rate] += 1
    return dict(minutes)


def minutes_of(line: PayLine) -> dict[Decimal, int]:
    """The line's parts as whole minutes per rate."""
    out: dict[Decimal, int] = defaultdict(int)
    for part in line.parts:
        out[part.rate_pct] += part.seconds // 60
        assert part.seconds % 60 == 0
    return dict(out)


@settings(
    max_examples=EXAMPLES,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)
@given(
    shift=shifts(),
    cfg=pay_settings(),
    special=st.lists(st.tuples(st.integers(-1, 2), percent(100, 300)), max_size=3),
    rate=st.one_of(st.none(), st.integers(1, 99_999_999).map(lambda n: Decimal(n) / 10_000)),
)
def test_price_agrees_with_the_minute_by_minute_reading(
    shift: tuple[datetime, datetime, str],
    cfg: PaySettings,
    special: list[tuple[int, Decimal]],
    rate: Decimal | None,
) -> None:
    """The same minutes at the same rates, hours that add up, and pay rounded once (SC-002)."""
    start, end, zone = shift
    first = start.astimezone(ZoneInfo(zone)).date()
    days = {first + timedelta(days=offset): pct for offset, pct in special}

    line = price(start, end, zone, rate, cfg, [SpecialDay(d, p) for d, p in days.items()])

    expected = oracle(start, end, zone, cfg, days)
    assert minutes_of(line) == expected
    total_minutes = int((end - start) / MINUTE)
    assert sum(p.seconds for p in line.parts) == total_minutes * 60
    assert line.hours == Decimal(total_minutes) / 60
    assert all(p.rate_pct >= 100 for p in line.parts)
    if rate is None:
        assert line.pay is None and line.rate_missing
    else:
        exact = sum(Decimal(m) * r for r, m in expected.items()) * rate / Decimal(6000)
        assert line.pay == exact.quantize(CENT, rounding=ROUND_HALF_UP)
        assert not line.rate_missing
