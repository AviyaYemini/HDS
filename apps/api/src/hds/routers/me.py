"""Me router: the signed-in user's own profile, capabilities and sessions.

Layer: routers. Open to every authenticated role; it only ever touches the caller's own rows.
``GET /me`` also serves the caller's capabilities, the one list the client gates its navigation by
(007 FR-001, research R4).
"""

import uuid
from datetime import datetime
from typing import Literal

from fastapi import APIRouter, Depends, Response
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.capabilities import Capability
from hds.core.config import get_settings
from hds.core.database import get_unit_of_work
from hds.core.deps import current_session_id, get_current_user
from hds.core.types import Name, Phone, omittable
from hds.models.user import User
from hds.repositories.client import ClientRepository
from hds.repositories.group import GroupRepository
from hds.repositories.invitation_token import InvitationTokenRepository
from hds.repositories.login_throttle import LoginThrottleRepository
from hds.repositories.project_manager import ProjectManagerRepository
from hds.repositories.qualification import QualificationRepository
from hds.repositories.refresh_token import RefreshTokenRepository
from hds.repositories.user import UserRepository
from hds.routers.user_schemas import UserRead
from hds.services.auth import AuthService
from hds.services.capabilities import CapabilityService
from hds.services.employee import EmployeeService

router = APIRouter(prefix="/me", tags=["me"])


class ProfileUpdate(BaseModel):
    """Self-service profile changes.

    Unknown fields are accepted by the schema and refused with 403 by the service, so ``role`` is
    never silently dropped.
    """

    # Extra keys are kept so the service can reject them with 403 (profile.field_not_allowed)
    # instead of the schema silently dropping e.g. ``role``.
    model_config = {"extra": "allow"}

    full_name: Name = omittable()
    phone: Phone | None = None


class MeRead(UserRead):
    """The signed-in user's profile, with what they may do.

    ``capabilities`` are the names in ``hds.core.capabilities``, sorted. ``managed_project_ids``
    are the projects a Manager manages, where their ``:project`` capabilities apply; it is empty
    for every other role (an Admin's scoped capabilities cover every project).
    """

    capabilities: list[Capability]
    managed_project_ids: list[uuid.UUID]


class SessionRead(BaseModel):
    """One signed-in device: its live refresh token, never the token itself.

    ``last_refreshed_at`` is when the device last signed in or refreshed; ``expires_at`` when the
    session ends unless it refreshes again. ``current`` marks the session of the request itself,
    so "sign out of other devices" can end every other one.
    """

    id: uuid.UUID
    client_kind: Literal["web", "native"]
    last_refreshed_at: datetime
    expires_at: datetime
    current: bool


@router.get(
    "",
    response_model=MeRead,
    summary="Get my profile",
    description=(
        "Return the signed-in user's own profile, including their default hourly rate "
        "(view only), the capabilities they hold now and, for a Manager, the projects they "
        "manage. The client draws its navigation from `capabilities` only. Available to every "
        "role."
    ),
)
async def get_me(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> MeRead:
    """Get my profile.

    Return the signed-in user's own profile, including their default hourly rate (view only), the
    capabilities they hold now and, for a Manager, the projects they manage. Available to every
    role.
    """
    caps = await CapabilityService(ProjectManagerRepository(db)).for_user(current_user)
    profile = UserRead.model_validate(current_user).model_dump()
    return MeRead(
        **profile,
        capabilities=caps.capabilities,
        managed_project_ids=caps.managed_project_ids,
    )


def _auth(db: AsyncSession) -> AuthService:
    return AuthService(
        user_repo=UserRepository(db),
        token_repo=RefreshTokenRepository(db),
        config=get_settings(),
        link_repo=InvitationTokenRepository(db),
        throttle_repo=LoginThrottleRepository(db),
    )


@router.get(
    "/sessions",
    response_model=list[SessionRead],
    summary="List my sessions",
    description=(
        "The signed-in user's live sessions, one per device, newest first, with `current` set on "
        "the one this request was made in. Available to every role. `POST /auth/logout-all` ends "
        "them all."
    ),
)
async def list_sessions(
    current_user: User = Depends(get_current_user),
    session_id: uuid.UUID | None = Depends(current_session_id),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> list[SessionRead]:
    """List my sessions.

    The signed-in user's live sessions, one per device, newest first. Available to every role.
    """
    tokens = await _auth(db).sessions(current_user.id)
    return [
        SessionRead(
            id=t.id,
            client_kind="native" if t.client_kind == "native" else "web",
            last_refreshed_at=t.created_at,
            expires_at=t.expires_at,
            current=t.id == session_id,
        )
        for t in tokens
    ]


@router.delete(
    "/sessions/{id}",
    status_code=204,
    summary="End one of my sessions",
    description=(
        "Sign one device out by revoking its refresh token; its access token lapses within 15 "
        "minutes. Answers 404 (`resource.not_found`) for an id that is not one of the caller's "
        "live sessions. Available to every role."
    ),
)
async def end_session(
    id: uuid.UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> Response:
    """End one of my sessions.

    Sign one device out by revoking its refresh token. Answers 404 for an id that is not one of the
    caller's live sessions. Available to every role.
    """
    await _auth(db).end_session(current_user.id, id)
    return Response(status_code=204)


@router.patch(
    "",
    response_model=UserRead,
    summary="Update my profile",
    description=(
        "Change the signed-in user's own full name or phone. Any other field is refused "
        "with 403 (`profile.field_not_allowed`), so role, rate, email and activity "
        "cannot be changed here. "
        "An explicit null clears a field that allows it and is refused with 422 on a "
        "required one; an omitted field is left alone."
    ),
)
async def patch_me(
    body: ProfileUpdate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> User:
    """Update my profile.

    Change the signed-in user's own full name or phone. Any other field is refused with 403
    (`profile.field_not_allowed`), so role, rate, email and activity cannot be changed here.
    An explicit null clears a field that allows it and is refused with 422 on a required one; an
    omitted field is left alone.
    """
    data = body.model_dump(exclude_unset=True)
    svc = EmployeeService(
        user_repo=UserRepository(db),
        token_repo=RefreshTokenRepository(db),
        link_repo=InvitationTokenRepository(db),
        group_repo=GroupRepository(db),
        qualification_repo=QualificationRepository(db),
        client_repo=ClientRepository(db),
        project_manager_repo=ProjectManagerRepository(db),
    )
    return await svc.update_own_profile(current_user, data)
