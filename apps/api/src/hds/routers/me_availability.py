"""My availability submissions: Submit and the week's status (007 FR-014, FR-015).

Layer: routers. Delegates to ``AvailabilitySubmissionService``, which ``hds.wiring`` builds; HTTP
only. Employee, Manager or Admin, for their own availability only.
"""

import uuid
from datetime import datetime
from typing import Literal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from hds.core.deps import require_admin, require_staff
from hds.models.user import User
from hds.services.availability_submission import AvailabilitySubmissionService, SubmissionStatus
from hds.wiring import availability_submissions

router = APIRouter(prefix="/me", tags=["my availability"])


class SubmissionRead(BaseModel):
    """Where the caller's availability for one window stands.

    ``status`` is derived: ``locked`` once the deadline has passed; otherwise ``submitted`` while
    the entries equal what was submitted, ``draft`` after any change or entry, ``not_submitted``
    with neither. ``counted_unavailable`` is true after the deadline when nothing was submitted:
    the employee is then not scheduled anywhere in the window. ``submitted_by`` is who made the
    latest submit: the employee, or an Admin who submitted on their behalf.
    """

    window_id: uuid.UUID
    status: Literal["not_submitted", "draft", "submitted", "locked"]
    submitted_at: datetime | None
    submitted_by: uuid.UUID | None
    deadline: datetime
    counted_unavailable: bool


class SubmitRequest(BaseModel):
    """The window to submit for."""

    window_id: uuid.UUID


def _read(found: SubmissionStatus) -> SubmissionRead:
    return SubmissionRead(
        window_id=found.window.id,
        status=found.status,
        submitted_at=found.submitted_at,
        submitted_by=found.submitted_by,
        deadline=found.window.deadline,
        counted_unavailable=found.counted_unavailable,
    )


@router.get(
    "/availability-submissions",
    response_model=SubmissionRead,
    summary="Read my availability status for a window",
    description=(
        "Employee, Manager or Admin. Where the caller's own availability for `window_id` stands: "
        "not submitted, draft, submitted (with the time) or locked after the deadline, and "
        "whether a missed deadline counts them as unavailable for the whole window."
    ),
)
async def read_submission(
    window_id: uuid.UUID = Query(),
    current_user: User = Depends(require_staff),
    svc: AvailabilitySubmissionService = Depends(availability_submissions),
) -> SubmissionRead:
    """Read my availability status for a window.

    Employee, Manager or Admin, for their own availability only.
    """
    return _read(await svc.status(current_user, window_id))


@router.post(
    "/availability-submissions",
    response_model=SubmissionRead,
    summary="Submit my availability for a window",
    description=(
        "Employee, Manager or Admin. Submit the caller's blocked, open and close entries for an "
        "open window; submitting again replaces the earlier submission. Answers 409 "
        "(`availability.window_closed`) once the deadline has passed. After the deadline, "
        "scheduling uses what was submitted; with nothing submitted the caller counts as "
        "unavailable for the whole window."
    ),
)
async def submit(
    body: SubmitRequest,
    current_user: User = Depends(require_staff),
    svc: AvailabilitySubmissionService = Depends(availability_submissions),
) -> SubmissionRead:
    """Submit my availability for a window.

    Employee, Manager or Admin. Answers 409 once the deadline has passed.
    """
    return _read(await svc.submit(current_user, body.window_id))


on_behalf = APIRouter(prefix="/api/v1", tags=["availability"])


@on_behalf.post(
    "/employees/{id}/availability-submissions",
    response_model=SubmissionRead,
    summary="Submit an employee's availability on their behalf",
    description=(
        "Admin. Submit the employee's current blocked, open and close entries for `window_id`, "
        "at any time, the deadline included. The submission records the Admin as `submitted_by`. "
        "This is the explicit way to count an employee who did not submit; an Admin's edit of "
        "their entries alone never creates a submission."
    ),
)
async def submit_on_behalf(
    id: uuid.UUID,
    body: SubmitRequest,
    current_user: User = Depends(require_admin),
    svc: AvailabilitySubmissionService = Depends(availability_submissions),
) -> SubmissionRead:
    """Submit an employee's availability on their behalf.

    Admin, at any time. Records the Admin as the submitter.
    """
    return _read(await svc.submit_on_behalf(current_user, id, body.window_id))
