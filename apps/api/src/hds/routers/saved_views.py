"""Saved views router: the caller's named filters on a table (007 FR-043, FR-049).

Layer: routers. Delegates to ``SavedViewService``, which ``hds.wiring`` builds; HTTP only.
Employee, Manager or Admin, for their own views only.
"""

import uuid
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Response
from pydantic import BaseModel, Field

from hds.core.deps import require_staff
from hds.models.user import User
from hds.services.saved_view import SavedViewService
from hds.wiring import saved_views

router = APIRouter(prefix="/me", tags=["saved views"])

Screen = Annotated[str, Field(min_length=1, max_length=48, pattern=r"^[a-z][a-z0-9_]*$")]


class SavedViewCreate(BaseModel):
    """A view to save: the screen, a name, and the web address's query it restores."""

    screen: Screen
    name: Annotated[str, Field(min_length=1, max_length=60)]
    query: Annotated[str, Field(max_length=2000)]


class SavedViewRead(BaseModel):
    """One of the caller's saved views."""

    id: uuid.UUID
    screen: str
    name: str
    query: str
    created_at: datetime

    model_config = {"from_attributes": True}


class SavedViewList(BaseModel):
    """The caller's views of one screen, by name."""

    items: list[SavedViewRead]


@router.get(
    "/saved-views",
    response_model=SavedViewList,
    summary="List my saved views of a table",
    description="Employee, Manager or Admin. The caller's own views of `screen`, by name.",
)
async def list_views(
    screen: str = Query(min_length=1, max_length=48, pattern=r"^[a-z][a-z0-9_]*$"),
    current_user: User = Depends(require_staff),
    svc: SavedViewService = Depends(saved_views),
) -> SavedViewList:
    """List my saved views of a table.

    Employee, Manager or Admin, their own only.
    """
    rows = await svc.list(current_user, screen)
    return SavedViewList(items=[SavedViewRead.model_validate(r) for r in rows])


@router.post(
    "/saved-views",
    response_model=SavedViewRead,
    status_code=201,
    summary="Save a view of a table",
    description=(
        "Employee, Manager or Admin. Save the filters and sorting of a table, as the web "
        "address's query, under a name unique per screen (409 `views.duplicate_name`)."
    ),
)
async def save_view(
    body: SavedViewCreate,
    current_user: User = Depends(require_staff),
    svc: SavedViewService = Depends(saved_views),
) -> SavedViewRead:
    """Save a view of a table.

    Employee, Manager or Admin.
    """
    return SavedViewRead.model_validate(
        await svc.save(current_user, body.screen, body.name, body.query)
    )


@router.delete(
    "/saved-views/{id}",
    status_code=204,
    summary="Remove one of my saved views",
    description="Employee, Manager or Admin. Another user's view answers 404.",
)
async def remove_view(
    id: uuid.UUID,
    current_user: User = Depends(require_staff),
    svc: SavedViewService = Depends(saved_views),
) -> Response:
    """Remove one of my saved views.

    Employee, Manager or Admin, their own only.
    """
    await svc.remove(current_user, id)
    return Response(status_code=204)
