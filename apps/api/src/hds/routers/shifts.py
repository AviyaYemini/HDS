"""Shifts router: the manual roster and its assignments.

Layer: routers. Delegates to ``RosterService``; HTTP only. Managers and Admins read the roster; an
Admin changes any project's, a Manager the projects they manage (007 FR-028, Proposed). Every change
answers with the warnings it causes. Each shift carries an ``etag``; a change may send it back as
``If-Match`` and is refused with 412 (``roster.changed``) when the shift changed since (FR-041).
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
from hds.core.deps import require_manager
from hds.core.exceptions import DomainValidationError
from hds.core.types import ChangeRequest, IsoDate, IsoDateTime, Money, StrictDate, omittable
from hds.models.user import User
from hds.repositories.idempotency_key import IdempotencyKeyRepository
from hds.repositories.shift import ShiftView
from hds.routers.roster_wiring import roster_service, roster_tools
from hds.services.idempotency import IdempotencyService
from hds.services.roster import etag

router = APIRouter(prefix="/api/v1", tags=["shifts"])

Headcount = Field(ge=1, le=1000, description="How many employees the shift needs.")


class OverrideRead(BaseModel):
    """A recorded reason for keeping an assignment despite a warning (007 FR-034)."""

    warning_code: str
    reason: str
    created_by: uuid.UUID
    created_at: datetime


class AssignmentRead(BaseModel):
    """An employee on a shift, and when they work it.

    ``starts_at`` and ``ends_at`` are when this person works the shift: their own times when set,
    otherwise the shift's; ``own_times`` says which (007 FR-026, FR-027).
    """

    id: uuid.UUID
    shift_id: uuid.UUID
    user_id: uuid.UUID
    starts_at: datetime
    ends_at: datetime
    own_times: bool
    overrides: list[OverrideRead] = []


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
    etag: str


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
    """A stored assignment, the warnings it caused, and its shift's new ``etag``."""

    assignment: AssignmentRead
    warnings: list[WarningRead]
    shift_etag: str


class WarningsAnswer(BaseModel):
    """The warnings a change leaves, and the shift's new ``etag``."""

    warnings: list[WarningRead]
    shift_etag: str


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
    """The employee to put on the shift, and optionally their own times in it (007 FR-027)."""

    user_id: uuid.UUID
    starts_at: IsoDateTime | None = None
    ends_at: IsoDateTime | None = None


class AssignmentTimesRequest(BaseModel):
    """A person's own times in a shift; both null gives them the shift's times again."""

    starts_at: IsoDateTime | None
    ends_at: IsoDateTime | None


IfMatch = Header(
    default=None,
    alias="If-Match",
    max_length=64,
    description="The shift's `etag` as read; a stale one is refused with 412 (`roster.changed`).",
)


def _assignment(a: Any, shift: Any, overrides: list[Any] | None = None) -> AssignmentRead:
    own = a.starts_at is not None and a.ends_at is not None
    return AssignmentRead(
        id=a.id,
        shift_id=a.shift_id,
        user_id=a.user_id,
        starts_at=a.starts_at if own else shift.starts_at,
        ends_at=a.ends_at if own else shift.ends_at,
        own_times=own,
        overrides=[
            OverrideRead(
                warning_code=o.warning_code,
                reason=o.reason,
                created_by=o.created_by,
                created_at=o.created_at,
            )
            for o in overrides or []
        ],
    )


def _shift(view: ShiftView, overrides: dict[uuid.UUID, list[Any]] | None = None) -> ShiftRead:
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
        assignments=[_assignment(a, s, (overrides or {}).get(a.id)) for a in view.assignments],
        etag=etag(s),
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
    ids = [a.id for v in views for a in v.assignments]
    by_assignment: dict[uuid.UUID, list[Any]] = {}
    for o in await roster_tools(db).overrides_for(current_user, ids):
        by_assignment.setdefault(o.assignment_id, []).append(o)
    return ShiftPage(
        items=[_shift(v, by_assignment) for v in views], next_cursor=_cursor(following)
    )


class PublishRequest(BaseModel):
    """The dates, at the project's site, whose drafts to publish.

    ``notify`` is the publish review's choice between Notify everyone (the default) and Publish
    silently; one event per affected person is recorded either way, for feature 010 to deliver
    (007 FR-036, research R12).
    """

    first: StrictDate = Field(alias="from")
    last: StrictDate = Field(alias="to")
    notify: bool = True

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
        "that ends before it starts is refused with 422 (`shifts.date_range_invalid`). One "
        "event per person on the published shifts is recorded for delivery, with `notify` "
        "(default true) as the publisher's choice."
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
    count = await roster_tools(db).publish(current_user, id, body.first, body.last, body.notify)
    return PublishAnswer(published=count)


@router.post(
    "/shifts",
    response_model=ShiftAnswer,
    status_code=201,
    summary="Create a shift",
    description=(
        "A Manager of the project, or an Admin; only an Admin sets `pay_rate` (403 "
        "`shift.pay_rate_admin_only`), and a Manager of other projects is refused with 403 "
        "(`roster.project_not_managed`). "
        "A draft shift of one of the project's shift types on `date`, at the "
        "type's local times at the project's site unless `starts_at` and `ends_at` are both "
        "given. A type the project does not use is refused with 422 "
        "(`shifts.type_not_in_project`), times that do not end after they start with 422 "
        "(`shifts.times_invalid`). The answer lists the warnings the shift causes."
    ),
)
async def create_shift(
    body: ShiftCreateRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> ShiftAnswer:
    """Create a shift.

    A Manager of the project, or an Admin. The answer lists the warnings the shift causes.
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
        "A Manager of the project, or an Admin; only an Admin changes `pay_rate`. Send the "
        "shift's `etag` as `If-Match` to be refused with 412 (`roster.changed`) if it changed "
        "since. Change the times, headcount or pay rate; a published shift stays published. "
        "Times that do not end after they start are refused with 422 (`shifts.times_invalid`). "
        "The answer lists the warnings the change causes."
    ),
)
async def change_shift(
    id: uuid.UUID,
    body: ShiftUpdateRequest,
    if_match: str | None = IfMatch,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> ShiftAnswer:
    """Change a shift.

    A Manager of the project, or an Admin. The answer lists the warnings the change causes.
    """
    svc = roster_service(db)
    judged = await svc.change_shift(current_user, id, body.model_dump(exclude_unset=True), if_match)
    [view] = await svc.views([judged.record])
    return ShiftAnswer(shift=_shift(view), warnings=judged.warnings)  # type: ignore[arg-type]


@router.delete(
    "/shifts/{id}",
    status_code=204,
    summary="Delete a shift",
    description=(
        "A Manager of the project, or an Admin. Delete the shift and its assignments; a stale "
        "`If-Match` is refused with 412 (`roster.changed`)."
    ),
)
async def delete_shift(
    id: uuid.UUID,
    if_match: str | None = IfMatch,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> Response:
    """Delete a shift.

    A Manager of the project, or an Admin. Its assignments go with it.
    """
    await roster_service(db).delete_shift(current_user, id, if_match)
    return Response(status_code=204)


@router.post(
    "/shifts/{id}/assignments",
    response_model=AssignmentAnswer,
    status_code=201,
    summary="Assign an employee",
    description=(
        "A Manager of the project, or an Admin. `starts_at` and `ends_at`, both or neither, are "
        "the person's own times, at most 4 hours before the shift starts and after it ends (422 "
        "`assignment.times_out_of_range`). A stale `If-Match` is refused with 412. "
        "Put an active employee on the shift; the answer lists the warnings it "
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
    if_match: str | None = IfMatch,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> Response:
    """Assign an employee.

    A Manager of the project, or an Admin. The answer lists the warnings the assignment causes.
    A repeat with the same `Idempotency-Key` gets the first answer.
    """

    async def run() -> tuple[int, Any]:
        """Make the assignment and build the answer to store and send."""
        svc = roster_service(db)
        judged = await svc.assign(
            current_user,
            id,
            body.user_id,
            starts_at=body.starts_at,
            ends_at=body.ends_at,
            expected=if_match,
        )
        shift = await svc.fresh_shift(current_user, id)
        answer = AssignmentAnswer(
            assignment=_assignment(judged.record, shift),
            warnings=judged.warnings,  # type: ignore[arg-type]
            shift_etag=etag(shift),
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
        "A Manager of the project, or an Admin. Take the employee off the shift; the answer "
        "lists the warnings the shift has left, for example too few people. A stale `If-Match` "
        "is refused with 412 (`roster.changed`)."
    ),
)
async def unassign(
    id: uuid.UUID,
    if_match: str | None = IfMatch,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> WarningsAnswer:
    """Remove an assignment.

    A Manager of the project, or an Admin. The answer lists the warnings the shift has left.
    """
    svc = roster_service(db)
    shift_id, warnings = await svc.unassign_returning_shift(current_user, id, if_match)
    shift = await svc.fresh_shift(current_user, shift_id)
    return WarningsAnswer(warnings=warnings, shift_etag=etag(shift))  # type: ignore[arg-type]


@router.patch(
    "/assignments/{id}",
    response_model=AssignmentAnswer,
    summary="Change a person's own times in a shift",
    description=(
        "A Manager of the project, or an Admin. Set the person's own `starts_at` and `ends_at` "
        "in the shift, at most 4 hours before it starts and after it ends (422 "
        "`assignment.times_out_of_range`), or both null for the shift's times (007 FR-027). Pay, "
        "the rule checks and reports use the person's times. A stale `If-Match` is refused with "
        "412 (`roster.changed`)."
    ),
)
async def change_assignment_times(
    id: uuid.UUID,
    body: AssignmentTimesRequest,
    if_match: str | None = IfMatch,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> AssignmentAnswer:
    """Change a person's own times in a shift.

    A Manager of the project, or an Admin; both null gives them the shift's times again.
    """
    svc = roster_service(db)
    judged = await svc.change_assignment_times(
        current_user, id, body.starts_at, body.ends_at, if_match
    )
    shift = await svc.fresh_shift(current_user, judged.record.shift_id)
    return AssignmentAnswer(
        assignment=_assignment(judged.record, shift),
        warnings=judged.warnings,  # type: ignore[arg-type]
        shift_etag=etag(shift),
    )
