"""The premium settings a shift is priced with, and their defaults.

Layer: payroll (pure). Standard library only; MUST NOT import anything from the application, a web
framework, a database driver or the network (005 research R2).

The defaults follow common readings of the Israeli Hours of Work and Rest Law (005 spec,
Assumptions): overtime after 8 hours, or 7 on a night shift; the first 2 overtime hours at 125% and
the rest at 150%; work in the weekly rest at 150%, and overtime there at 175% then 200%. They are
defaults to confirm before launch; every one can be changed per organisation and per project.
"""

from dataclasses import dataclass, field
from datetime import date, time
from decimal import Decimal


@dataclass(frozen=True)
class RestWindow:
    """The weekly rest, from a weekday and time to a weekday and time at the site.

    Weekdays run from 0 (Sunday) to 6 (Saturday). The window is read forward from its start, so
    Friday 18:00 to Saturday 22:00 lasts 28 hours; a start equal to the end is an empty window (the
    services refuse it).
    """

    start_weekday: int
    start_time: time
    end_weekday: int
    end_time: time


@dataclass(frozen=True)
class PaySettings:
    """How long a normal shift is and what each kind of hour is paid, in percent of the rate.

    ``tier1_minutes`` is how many overtime minutes are paid at ``overtime1_pct`` before
    ``overtime2_pct`` applies. On a rest or special day above 100%, overtime is paid at that day's
    rate plus ``increase1_points`` or ``increase2_points`` (005 FR-013).
    """

    day_normal_minutes: int = 480
    night_normal_minutes: int = 420
    tier1_minutes: int = 120
    overtime1_pct: Decimal = Decimal("125.00")
    overtime2_pct: Decimal = Decimal("150.00")
    rest: RestWindow = field(default_factory=lambda: RestWindow(5, time(18, 0), 6, time(22, 0)))
    rest_pct: Decimal = Decimal("150.00")
    increase1_points: Decimal = Decimal("25.00")
    increase2_points: Decimal = Decimal("50.00")


@dataclass(frozen=True)
class SpecialDay:
    """A local date paid at ``rate_pct`` percent, such as a holiday or an election day."""

    day: date
    rate_pct: Decimal


DEFAULT_SETTINGS = PaySettings()
"""The organisation defaults of a new organisation (005 data-model)."""
