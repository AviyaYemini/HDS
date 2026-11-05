"""Shifts router: the manual roster and its assignments.

Layer: routers. Delegates to ``RosterService``; HTTP only. Only an Admin changes the roster;
Managers and Admins read it. Every change answers with the warnings it causes.
"""

import base64
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Header, Query, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.database import get_unit_of_work
from hds.core.deps import require_admin, require_manager
from hds.core.exceptions import DomainValidationError
from hds.core.types import ChangeRequest, IsoDate, IsoDateTime, Money, StrictDate, omittable
from hds.models.user import User
from hds.repositories.idempotency_key import IdempotencyKeyRepository
from hds.repositories.shift import ShiftView
from hds.routers.roster_wiring import roster_service
from hds.services.idempotency import IdempotencyService

router = APIRouter(prefix="/api/v1", tags=["shifts"])

Headcount = Field(ge=1, le=1000, description="How many employees the shift needs.")


class AssignmentRead(BaseModel):
    """An employee on a shift."""

    id: uuid.UUID
    shift_id: uuid.UUID
    user_id: uuid.UUID


class ShiftRead(BaseModel):
    """A shift: UTC instants, its site's IANA time zone, and who is on it."""

    id: uuid.UUID
    project_id: uuid.UUID
    shift_type_id: uuid.UUID
    starts_at: datetime
    ends_at: datetime
    timezone: str
    headcount: int
    pay_rate: Decimal | None
    status: Literal["draft", "published"]
    published_at: datetime | None
    assignments: list[AssignmentRead]


class WarningRead(BaseModel):
    """A rule a change breaks; the change was stored anyway."""

    code: str
    employee_id: uuid.UUID | None
    shift_ids: list[uuid.UUID]
    detail: str


class ShiftAnswer(BaseModel):
    """A stored shift and the warnings its change caused."""

    shift: ShiftRead
    warnings: list[WarningRead]


class AssignmentAnswer(BaseModel):
    """A stored assignment and the warnings it caused."""

    assignment: AssignmentRead
    warnings: list[WarningRead]


class WarningsAnswer(BaseModel):
    """The warnings a change leaves."""

    warnings: list[WarningRead]


class ShiftPage(BaseModel):
    """A page of the roster; ``next_cursor`` continues it, ``null`` on the last page."""

    items: list[ShiftRead]
    next_cursor: str | None


class ShiftCreateRequest(BaseModel):
    """A new shift of one of the project's shift types on ``date``.

    Its times are the type's local times at the project's site unless ``starts_at`` and
    ``ends_at`` are both given. ``pay_rate`` is the hourly pay for it, otherwise each employee's own
    rate applies.
    """

    project_id: uuid.UUID
    shift_type_id: uuid.UUID
    date: StrictDate
    headcount: int = Headcount
    pay_rate: Money | None = None
    starts_at: IsoDateTime | None = None
    ends_at: IsoDateTime | None = None


class ShiftUpdateRequest(ChangeRequest):
    """Shift fields that may change; null clears ``pay_rate``."""

    starts_at: IsoDateTime = omittable()
    ends_at: IsoDateTime = omittable()
    headcount: Annotated[int, Field(ge=1, le=1000)] = omittable()
    pay_rate: Money | None = None


class AssignRequest(BaseModel):
    """The employee to put on the shift."""

    user_id: uuid.UUID


def _shift(view: ShiftView) -> ShiftRead:
    s = view.shift
    return ShiftRead(
        id=s.id,
        project_id=s.project_id,
        shift_type_id=s.shift_type_id,
        starts_at=s.starts_at,
        ends_at=s.ends_at,
        timezone=view.timezone,
        headcount=s.headcount,
        pay_rate=s.pay_rate if view.rate_shown else None,
        status=s.status,  # type: ignore[arg-type]
        published_at=s.published_at,
        assignments=[
            AssignmentRead(id=a.id, shift_id=a.shift_id, user_id=a.user_id)
            for a in view.assignments
        ],
    )


def _cursor(position: tuple[datetime, uuid.UUID] | None) -> str | None:
    if position is None:
        return None
    text = f"{position[0].isoformat()}|{position[1]}"
    return base64.urlsafe_b64encode(text.encode()).decode().rstrip("=")


def _position(cursor: str | None) -> tuple[datetime, uuid.UUID] | None:
    if cursor is None:
        return None
    try:
        text = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)).decode()
        moment, id = text.split("|")
        return datetime.fromisoformat(moment), uuid.UUID(id)
    except (ValueError, UnicodeDecodeError):
        raise DomainValidationError(
            code="shifts.cursor_invalid", detail="The cursor is not one this API returned."
        ) from None


@router.get(
    "/shifts",
    response_model=ShiftPage,
    summary="List the roster",
    description=(
        "Manager or Admin. Shifts that start on `from` to `to` (dates in the organisation's time "
        "zone), drafts and published, each with who is on it, ordered by start. `limit` (1 to "
        "200, default 50) per page; pass `next_cursor` back as `cursor` for the next."
    ),
)
async def list_shifts(
    first: IsoDate | None = Query(default=None, alias="from"),
    last: IsoDate | None = Query(default=None, alias="to"),
    project_id: uuid.UUID | None = None,
    status: Literal["draft", "published"] | None = None,
    cursor: str | None = Query(default=None, max_length=200),
    limit: int = Query(default=50, ge=1, le=200),
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> ShiftPage:
    """List the roster.

    Manager or Admin. Shifts that start on `from` to `to`, drafts and published, with who is on
    each, ordered by start and paged with `cursor`.
    """
    views, following = await roster_service(db).list_shifts(
        current_user,
        first=first,
        last=last,
        project_id=project_id,
        status=status,
        after=_position(cursor),
        limit=limit,
    )
    return ShiftPage(items=[_shift(v) for v in views], next_cursor=_cursor(following))


class PublishRequest(BaseModel):
    """The dates, at the project's site, whose drafts to publish."""

    first: StrictDate = Field(alias="from")
    last: StrictDate = Field(alias="to")

    model_config = {"populate_by_name": True}


class PublishAnswer(BaseModel):
    """How many drafts were published."""

    published: int


@router.post(
    "/projects/{id}/publish",
    response_model=PublishAnswer,
    summary="Publish drafts",
    description=(
        "Manager or Admin. Publish the project's draft shifts that start on `from` to `to` "
        "(dates at the project's site); published ones are left as they are. A range "
        "that ends before it starts is refused with 422 (`shifts.date_range_invalid`)."
    ),
)
async def publish(
    id: uuid.UUID,
    body: PublishRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> PublishAnswer:
    """Publish drafts.

    Manager or Admin. Publish the project's drafts in the range; answers how many.
    """
    count = await roster_service(db).publish(current_user, id, body.first, body.last)
    return PublishAnswer(published=count)


@router.post(
    "/shifts",
    response_model=ShiftAnswer,
    status_code=201,
    summary="Create a shift",
    description=(
        "Admin only. A draft shift of one of the project's shift types on `date`, at the "
        "type's local times at the project's site unless `starts_at` and `ends_at` are both "
        "given. A type the project does not use is refused with 422 "
        "(`shifts.type_not_in_project`), times that do not end after they start with 422 "
        "(`shifts.times_invalid`). The answer lists the warnings the shift causes."
    ),
)
async def create_shift(
    body: ShiftCreateRequest,
    current_user: User = Depends(require_admin),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> ShiftAnswer:
    """Create a shift.

    Admin only. The answer lists the warnings the shift causes; none refuses it.
    """
    svc = roster_service(db)
    judged = await svc.create_shift(current_user, body.model_dump())
    [view] = await svc.views([judged.record])
    return ShiftAnswer(shift=_shift(view), warnings=judged.warnings)  # type: ignore[arg-type]


@router.patch(
    "/shifts/{id}",
    response_model=ShiftAnswer,
    summary="Change a shift",
    description=(
        "Admin only. Change the times, headcount or pay rate; a published shift stays published. "
        "Times that do not end after they start are refused with 422 (`shifts.times_invalid`). "
        "The answer lists the warnings the change causes."
    ),
)
async def change_shift(
    id: uuid.UUID,
    body: ShiftUpdateRequest,
    current_user: User = Depends(require_admin),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> ShiftAnswer:
    """Change a shift.

    Admin only. The answer lists the warnings the change causes; none refuses it.
    """
    svc = roster_service(db)
    judged = await svc.change_shift(current_user, id, body.model_dump(exclude_unset=True))
    [view] = await svc.views([judged.record])
    return ShiftAnswer(shift=_shift(view), warnings=judged.warnings)  # type: ignore[arg-type]


@router.delete(
    "/shifts/{id}",
    status_code=204,
    summary="Delete a shift",
    description="Admin only. Delete the shift and its assignments.",
)
async def delete_shift(
    id: uuid.UUID,
    current_user: User = Depends(require_admin),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> Response:
    """Delete a shift.

    Admin only. Its assignments go with it.
    """
    await roster_service(db).delete_shift(current_user, id)
    return Response(status_code=204)


@router.post(
    "/shifts/{id}/assignments",
    response_model=AssignmentAnswer,
    status_code=201,
    summary="Assign an employee",
    description=(
        "Admin only. Put an active employee on the shift; the answer lists the warnings it "
        "causes, and none refuses it. An inactive employee is refused with 409 "
        "(`assignments.employee_inactive`), the same employee twice with 409 "
        "(`assignments.duplicate`). With an `Idempotency-Key`, a repeat of the same request in "
        "the next 24 hours gets the first answer and changes nothing; the same key with another "
        "request is refused with 422 (`idempotency.key_reused`)."
    ),
)
async def assign(
    id: uuid.UUID,
    body: AssignRequest,
    idempotency_key: str | None = Header(default=None, min_length=1, max_length=255),
    current_user: User = Depends(require_admin),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> Response:
    """Assign an employee.

    Admin only. The answer lists the warnings the assignment causes. A repeat with the same
    `Idempotency-Key` gets the first answer.
    """

    async def run() -> tuple[int, Any]:
        """Make the assignment and build the answer to store and send."""
        judged = await roster_service(db).assign(current_user, id, body.user_id)
        a = judged.record
        answer = AssignmentAnswer(
            assignment=AssignmentRead(id=a.id, shift_id=a.shift_id, user_id=a.user_id),
            warnings=judged.warnings,  # type: ignore[arg-type]
        )
        return 201, answer.model_dump(mode="json")

    status, content = await IdempotencyService(IdempotencyKeyRepository(db)).run_once(
        current_user.id,
        idempotency_key,
        {"operation": "assign", "shift_id": str(id), "body": body.model_dump(mode="json")},
        run,
    )
    return JSONResponse(status_code=status, content=content)


@router.delete(
    "/assignments/{id}",
    response_model=WarningsAnswer,
    summary="Remove an assignment",
    description=(
        "Admin only. Take the employee off the shift; the answer lists the warnings the shift "
        "has left, for example too few people."
    ),
)
async def unassign(
    id: uuid.UUID,
    current_user: User = Depends(require_admin),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> WarningsAnswer:
    """Remove an assignment.

    Admin only. The answer lists the warnings the shift has left.
    """
    warnings = await roster_service(db).unassign(current_user, id)
    return WarningsAnswer(warnings=warnings)  # type: ignore[arg-type]
