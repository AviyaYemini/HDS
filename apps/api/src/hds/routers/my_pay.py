"""My pay router: the gross pay a person has earned so far in a month.

Layer: routers. Delegates to ``PayService``, which ``hds.wiring`` builds; HTTP only. Every
Employee, Manager and Admin reads their own; Clients are refused (005 FR-017 to FR-020).
"""

import uuid
from datetime import date, datetime
from decimal import Decimal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from hds.core.deps import require_staff
from hds.core.types import Amount, HoursOut, PercentOut
from hds.models.user import User
from hds.services.pay import LineView, PayService, PayToDate
from hds.wiring import pay

router = APIRouter(tags=["my pay"])

# YYYY-MM with a year from 0001; no look-ahead, which the query validator does not support.
MONTH = r"^([1-9][0-9]{3}|0[1-9][0-9]{2}|00[1-9][0-9]|000[1-9])-(0[1-9]|1[0-2])$"


class PartRead(BaseModel):
    """Hours paid at one rate, in percent of the base rate."""

    rate_pct: PercentOut
    hours: HoursOut
    pay: Amount | None = Field(
        default=None,
        description=(
            "This rate's share of the line's pay, to the agora; the parts of a line add up to its "
            "`pay` exactly. Left out when the line has no pay."
        ),
    )


class PayLineRead(BaseModel):
    """One ended shift of the caller's, priced.

    ``pay`` is left out when the project has pay switched off (``pay_enabled`` false) or when there
    is no rate (``rate_missing``): missing pay is never shown as zero.
    """

    shift_id: uuid.UUID
    project_id: uuid.UUID
    project_name: str
    date: date
    starts_at: datetime
    ends_at: datetime
    timezone: str = Field(description="The site's IANA time zone, to show the times as there.")
    hours: HoursOut
    base_rate: Decimal | None = Field(description="The hourly rate used, a decimal string.")
    rate_missing: bool
    pay_enabled: bool
    parts: list[PartRead]
    pay: Amount | None = Field(default=None, description="Left out when there is no pay.")


class PayToDateRead(BaseModel):
    """The caller's pay for a month so far: each ended shift, and the totals of the lines shown."""

    month: str = Field(examples=["2026-10"])
    lines: list[PayLineRead]
    hours: HoursOut
    pay: Amount | None = Field(
        default=None,
        description=(
            "The sum of the lines' pay. Left out when no line has pay and some line lacks a rate "
            "(then shifts_missing_rate is above 0): a missing rate is never shown as zero."
        ),
    )
    shifts_missing_rate: int


def read_line(line: LineView) -> PayLineRead:
    """The response form of a line."""
    fields = {
        "shift_id": line.shift_id,
        "project_id": line.project_id,
        "project_name": line.project_name,
        "date": line.date,
        "starts_at": line.starts_at,
        "ends_at": line.ends_at,
        "timezone": line.timezone,
        "hours": line.hours,
        "base_rate": line.base_rate,
        "rate_missing": line.rate_missing,
        "pay_enabled": line.pay_enabled,
        # A part's pay is shown exactly when the line's is: never a zero for missing pay.
        "parts": [
            PartRead(
                rate_pct=p.rate_pct, hours=p.hours, pay=p.pay if line.pay is not None else None
            )
            for p in line.parts
        ],
    }
    if line.pay is not None:
        fields["pay"] = line.pay
    return PayLineRead.model_validate(fields)


def read(answer: PayToDate) -> PayToDateRead:
    """The response form of pay to date."""
    fields = {
        "month": answer.month,
        "lines": [read_line(line) for line in answer.lines],
        "hours": answer.hours,
        "shifts_missing_rate": answer.shifts_missing_rate,
    }
    if answer.pay is not None:
        fields["pay"] = answer.pay
    return PayToDateRead.model_validate(fields)


@router.get(
    "/me/pay",
    response_model=PayToDateRead,
    response_model_exclude_unset=True,
    summary="Read my pay so far this month",
    description=(
        "Employee, Manager or Admin. The caller's gross pay, before tax, for a month (`YYYY-MM`, "
        "the current one by default): each published shift that has ended, with its hours at each "
        "rate and its pay, and the totals. Draft shifts and shifts not yet ended are not counted. "
        "A shift belongs to the month it starts in at its site. 422 `pay.month_invalid`."
    ),
)
async def my_pay(
    month: str | None = Query(default=None, pattern=MONTH),
    current_user: User = Depends(require_staff),
    svc: PayService = Depends(pay),
) -> PayToDateRead:
    """Read my pay so far this month.

    Employee, Manager or Admin; each sees only their own shifts. Gross pay, before tax.
    """
    return read(await svc.my_pay(current_user, month))
