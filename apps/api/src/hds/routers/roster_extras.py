"""Roster tools router: overrides, the publish review, copy week, templates, bulk edit, tracker.

Layer: routers. Delegates to ``RosterToolsService``; HTTP only (007 FR-034 to FR-040). Managers and
Admins; changes to a project's roster need ``roster.edit:project`` (its Managers, or an Admin).
"""

import uuid
from datetime import datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Query, Response
from pydantic import BaseModel, Field, StrictBool, StrictInt

from hds.core.deps import require_manager
from hds.core.types import IsoDate, StrictDate, text
from hds.models.user import User
from hds.services.roster_tools import PlanResult, RosterToolsService
from hds.wiring import roster_tools_service

router = APIRouter(prefix="/api/v1", tags=["roster tools"])

Reason = Annotated[str, text(500, min_length=1)]
OnConflict = Literal["allow", "overwrite", "unfilled"]


class OverrideRequest(BaseModel):
    """The warning to keep the assignment despite, and why."""

    warning_code: Annotated[str, text(64, min_length=1)]
    reason: Reason


class OverrideCreated(BaseModel):
    """A recorded override."""

    id: uuid.UUID
    assignment_id: uuid.UUID
    warning_code: str
    reason: str
    created_by: uuid.UUID
    created_at: datetime


class PublishPreviewRead(BaseModel):
    """What publishing the project's drafts on those dates would make live (FR-036)."""

    shifts: int
    people: int
    filled: int
    required: int
    unfilled: int
    open_warnings: int


class PlannedChangeRead(BaseModel):
    """One line of a plan: what happens to one shift."""

    action: Literal["create", "replace", "skip", "change", "remove"]
    starts_at: datetime
    ends_at: datetime
    shift_type_id: uuid.UUID
    headcount: int
    people: int
    shift_id: uuid.UUID | None


class WarningLine(BaseModel):
    """A warning applying a plan caused; nothing was refused."""

    code: str
    employee_id: uuid.UUID | None
    shift_ids: list[uuid.UUID]
    detail: str
    values: dict[str, str] = Field(default_factory=dict, description="The rule's numbers.")


class PlanAnswer(BaseModel):
    """A plan's changes; on a dry run nothing was changed and there are no warnings."""

    dry_run: bool
    changes: list[PlannedChangeRead]
    warnings: list[WarningLine]


class CopyWeekRequest(BaseModel):
    """Copy one week of the project's roster to another; both weeks start on a Sunday."""

    from_week: StrictDate
    to_week: StrictDate
    on_conflict: OnConflict = "allow"
    dry_run: StrictBool = False


class TemplateCreate(BaseModel):
    """Save the project's week starting on Sunday ``week`` under ``name``."""

    name: Annotated[str, text(80, min_length=1)]
    week: StrictDate


class TemplateRead(BaseModel):
    """A saved week: its name and how many shifts it holds."""

    id: uuid.UUID
    name: str
    shifts: int
    created_at: datetime


class TemplateList(BaseModel):
    """The project's saved weeks, by name."""

    items: list[TemplateRead]


class TemplateApply(BaseModel):
    """Apply a saved week to the week starting on Sunday ``week``."""

    week: StrictDate
    on_conflict: OnConflict = "allow"
    dry_run: StrictBool = False


class BulkRequest(BaseModel):
    """Change every draft of the project on ``from`` to ``to``: a new headcount, or remove."""

    first: StrictDate = Field(alias="from")
    last: StrictDate = Field(alias="to")
    headcount: Annotated[StrictInt, Field(ge=1, le=1000)] | None = None
    remove: StrictBool = False
    dry_run: StrictBool = False

    model_config = {"populate_by_name": True}


class TrackedRead(BaseModel):
    """One employee's availability for the window."""

    user_id: uuid.UUID
    full_name: str
    status: Literal["not_submitted", "draft", "submitted", "locked"]
    submitted_at: datetime | None
    counted_unavailable: bool
    changed_by: uuid.UUID | None = Field(
        default=None,
        description="The Admin who last changed the submitted availability by hand, if any.",
    )
    changed_at: datetime | None = Field(default=None, description="When that change was made.")


class TrackerRead(BaseModel):
    """Every active staff member's availability for the window, by name (FR-040)."""

    items: list[TrackedRead]


def _plan(result: PlanResult, dry_run: bool) -> PlanAnswer:
    return PlanAnswer(
        dry_run=dry_run,
        changes=[PlannedChangeRead(**vars(c)) for c in result.changes],
        warnings=[WarningLine(**w) for w in result.warnings],
    )


@router.post(
    "/assignments/{id}/overrides",
    response_model=OverrideCreated,
    status_code=201,
    summary="Keep an assignment despite a warning",
    description=(
        "A Manager of the project, or an Admin. Record why the assignment stays despite one of "
        "its current warnings; the publish review then no longer counts that warning as open. "
        "A code the assignment does not have now is refused with 422 "
        "(`override.warning_not_found`), a second override of the same warning with 409 "
        "(`override.duplicate`). Hard stops have no override."
    ),
)
async def override(
    id: uuid.UUID,
    body: OverrideRequest,
    current_user: User = Depends(require_manager),
    tools: RosterToolsService = Depends(roster_tools_service),
) -> OverrideCreated:
    """Keep an assignment despite a warning.

    A Manager of the project, or an Admin; the reason is recorded with who and when.
    """
    row = await tools.override(current_user, id, body.warning_code, body.reason)
    return OverrideCreated(
        id=row.id,
        assignment_id=row.assignment_id,
        warning_code=row.warning_code,
        reason=row.reason,
        created_by=row.created_by,
        created_at=row.created_at,
    )


@router.get(
    "/projects/{id}/publish-preview",
    response_model=PublishPreviewRead,
    summary="Review a publish before making it",
    description=(
        "Manager or Admin. What publishing the project's drafts that start on `from` to `to` "
        "(dates at the site) would make live: the shifts, the people on them, filled against "
        "required, unfilled seats, and warnings without an override."
    ),
)
async def publish_preview(
    id: uuid.UUID,
    first: IsoDate = Query(alias="from"),
    last: IsoDate = Query(alias="to"),
    current_user: User = Depends(require_manager),
    tools: RosterToolsService = Depends(roster_tools_service),
) -> PublishPreviewRead:
    """Review a publish before making it.

    Manager or Admin. Changes nothing.
    """
    found = await tools.publish_preview(current_user, id, first, last)
    return PublishPreviewRead(**vars(found))


@router.post(
    "/projects/{id}/copy-week",
    response_model=PlanAnswer,
    summary="Copy a week of the roster",
    description=(
        "A Manager of the project, or an Admin. Copy the week starting on Sunday `from_week` to "
        "the one starting on `to_week`, at the same local times, with the same people and their "
        "own times. Where a shift of the same type already starts at that time, `on_conflict` "
        "decides: `allow` adds it anyway, `overwrite` replaces a draft (never a published "
        "shift), `unfilled` adds it without people. `dry_run` answers the plan and changes "
        "nothing. A week that does not start on a Sunday is refused with 422 "
        "(`roster.week_not_sunday`)."
    ),
)
async def copy_week(
    id: uuid.UUID,
    body: CopyWeekRequest,
    current_user: User = Depends(require_manager),
    tools: RosterToolsService = Depends(roster_tools_service),
) -> PlanAnswer:
    """Copy a week of the roster.

    A Manager of the project, or an Admin; `dry_run` changes nothing.
    """
    result = await tools.copy_week(
        current_user, id, body.from_week, body.to_week, body.on_conflict, body.dry_run
    )
    return _plan(result, body.dry_run)


@router.get(
    "/projects/{id}/week-templates",
    response_model=TemplateList,
    summary="List the project's saved weeks",
    description="Manager or Admin. The project's week templates, by name.",
)
async def list_templates(
    id: uuid.UUID,
    current_user: User = Depends(require_manager),
    tools: RosterToolsService = Depends(roster_tools_service),
) -> TemplateList:
    """List the project's saved weeks.

    Manager or Admin.
    """
    rows = await tools.templates(current_user, id)
    return TemplateList(
        items=[
            TemplateRead(id=t.id, name=t.name, shifts=len(t.body), created_at=t.created_at)
            for t in rows
        ]
    )


@router.post(
    "/projects/{id}/week-templates",
    response_model=TemplateRead,
    status_code=201,
    summary="Save a week as a template",
    description=(
        "A Manager of the project, or an Admin. Save the project's week starting on Sunday "
        "`week` under `name`, unique per project (409 `templates.duplicate_name`)."
    ),
)
async def save_template(
    id: uuid.UUID,
    body: TemplateCreate,
    current_user: User = Depends(require_manager),
    tools: RosterToolsService = Depends(roster_tools_service),
) -> TemplateRead:
    """Save a week as a template.

    A Manager of the project, or an Admin.
    """
    t = await tools.save_template(current_user, id, body.name, body.week)
    return TemplateRead(id=t.id, name=t.name, shifts=len(t.body), created_at=t.created_at)


@router.delete(
    "/projects/{id}/week-templates/{template_id}",
    status_code=204,
    summary="Remove a saved week",
    description="A Manager of the project, or an Admin. Remove one of the project's templates.",
)
async def delete_template(
    id: uuid.UUID,
    template_id: uuid.UUID,
    current_user: User = Depends(require_manager),
    tools: RosterToolsService = Depends(roster_tools_service),
) -> Response:
    """Remove a saved week.

    A Manager of the project, or an Admin.
    """
    await tools.delete_template(current_user, id, template_id)
    return Response(status_code=204)


@router.post(
    "/projects/{id}/week-templates/{template_id}/apply",
    response_model=PlanAnswer,
    summary="Apply a saved week",
    description=(
        "A Manager of the project, or an Admin. Create the template's shifts in the week "
        "starting on Sunday `week`; `on_conflict` and `dry_run` work as for copy week."
    ),
)
async def apply_template(
    id: uuid.UUID,
    template_id: uuid.UUID,
    body: TemplateApply,
    current_user: User = Depends(require_manager),
    tools: RosterToolsService = Depends(roster_tools_service),
) -> PlanAnswer:
    """Apply a saved week.

    A Manager of the project, or an Admin; `dry_run` changes nothing.
    """
    result = await tools.apply_template(
        current_user, id, template_id, body.week, body.on_conflict, body.dry_run
    )
    return _plan(result, body.dry_run)


@router.post(
    "/projects/{id}/roster/bulk",
    response_model=PlanAnswer,
    summary="Change many drafts at once",
    description=(
        "A Manager of the project, or an Admin. Give every draft of the project that starts on "
        "`from` to `to` a new `headcount`, or `remove` them; published shifts are never touched. "
        "Exactly one of the two (422 `roster.bulk_change_invalid`). `dry_run` lists exactly the "
        "drafts it would hit and changes nothing."
    ),
)
async def bulk(
    id: uuid.UUID,
    body: BulkRequest,
    current_user: User = Depends(require_manager),
    tools: RosterToolsService = Depends(roster_tools_service),
) -> PlanAnswer:
    """Change many drafts at once.

    A Manager of the project, or an Admin; `dry_run` changes nothing.
    """
    result = await tools.bulk(
        current_user,
        id,
        body.first,
        body.last,
        headcount=body.headcount,
        remove=body.remove,
        dry_run=body.dry_run,
    )
    return _plan(result, body.dry_run)


@router.get(
    "/availability-windows/{id}/submissions",
    response_model=TrackerRead,
    summary="Track who has submitted availability",
    description=(
        "Manager or Admin. Every active employee, manager and admin with their availability "
        "status for the window, and after the deadline whether they count as unavailable for not "
        "submitting."
    ),
)
async def tracker(
    id: uuid.UUID,
    current_user: User = Depends(require_manager),
    tools: RosterToolsService = Depends(roster_tools_service),
) -> TrackerRead:
    """Track who has submitted availability.

    Manager or Admin.
    """
    rows = await tools.tracker(current_user, id)
    return TrackerRead(items=[TrackedRead(**vars(r)) for r in rows])
