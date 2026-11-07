"""Auth router: login, refresh, logout, logout-others, logout-all.

Layer: routers. Delegates to ``AuthService``; the only public endpoints are login and refresh.
Paths are served under ``/auth`` without the ``/api/v1`` prefix used by the other routers.
"""

import uuid
from typing import Literal

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.client_address import client_address
from hds.core.config import get_settings
from hds.core.database import get_unit_of_work
from hds.core.deps import current_session_id, get_current_user
from hds.core.types import Email, Password, Token
from hds.models.user import User
from hds.repositories.invitation_token import InvitationTokenRepository
from hds.repositories.login_throttle import LoginThrottleRepository
from hds.repositories.refresh_token import RefreshTokenRepository
from hds.repositories.user import UserRepository
from hds.services.auth import AuthService

router = APIRouter(prefix="/auth", tags=["auth"])


class LoginRequest(BaseModel):
    """Credentials for ``POST /auth/login``.

    ``client_kind`` says what is signing in: ``web`` (the default) or ``native`` (the phone
    app), whose refresh token lasts longer (007 FR-010, ADR 016).
    """

    email: Email
    password: Password
    client_kind: Literal["web", "native"] = "web"


class TokenResponse(BaseModel):
    """Token pair returned by sign-in and refresh.

    ``expires_in`` is the access-token lifetime in seconds.
    """

    access_token: str
    token_type: str = "bearer"
    expires_in: int
    refresh_token: str


class RefreshRequest(BaseModel):
    """A refresh token to exchange or revoke."""

    refresh_token: Token


def _make_service(db: AsyncSession) -> AuthService:
    settings = get_settings()
    return AuthService(
        user_repo=UserRepository(db),
        token_repo=RefreshTokenRepository(db),
        config=settings,
        link_repo=InvitationTokenRepository(db),
        throttle_repo=LoginThrottleRepository(db),
    )


@router.post(
    "/login",
    response_model=TokenResponse,
    summary="Sign in",
    description=(
        "Exchange an email and password for an access token and a refresh token. The "
        "email is matched without regard to letter case. Unknown email, wrong password "
        "and inactive account all return the same 401 (`auth.invalid_credentials`), "
        "after the same amount of work. Any address, registered or not, locks after "
        "exactly the configured number of consecutive failures (default 5) and the call "
        "then returns 423 (`auth.account_locked`) until the cooldown ends, even with the "
        "right password; the count restarts at zero when it does, and a successful "
        "sign-in also clears it. Too many failures from one network address inside the "
        "window return 429 (`auth.rate_limited`) with `Retry-After`, for every address; "
        "a successful sign-in does not clear that count. "
        "Answers 503 (`service.busy`) with `Retry-After` when the password-hashing queue "
        "is full; nothing is changed. "
        "`client_kind` `native` (the phone app) gets a refresh token that lasts "
        "`REFRESH_TOKEN_TTL_DAYS_NATIVE` days (30) and renews on every refresh; `web`, the "
        "default, keeps `REFRESH_TOKEN_TTL_DAYS` (7)."
    ),
)
async def login(
    request: Request,
    body: LoginRequest,
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> TokenResponse:
    """Sign in.

    Exchange an email and password for an access token and a refresh token. The email is matched
    without regard to letter case. Unknown email, wrong password and inactive account all return
    the same 401 (`auth.invalid_credentials`), after the same amount of work. Any address,
    registered or not, locks after exactly the configured number of consecutive failures (default
    5) and the call then returns 423 (`auth.account_locked`) until the cooldown ends, even with the
    right password; the count restarts at zero when it does, and a successful sign-in also clears
    it. Too many failures from one network address inside the window return 429
    (`auth.rate_limited`) with `Retry-After`, for every address; a successful sign-in does not
    clear that count.
    Answers 503 (`service.busy`) with `Retry-After` when the password-hashing queue is full;
    nothing is changed. `client_kind` `native` gets a 30-day refresh token, `web` keeps 7 days.
    """
    settings = get_settings()
    svc = _make_service(db)
    network = client_address(request, settings.TRUSTED_PROXY_COUNT)
    result = await svc.login(str(body.email), body.password, network, body.client_kind)
    return TokenResponse(
        access_token=result.access_token,
        refresh_token=result.refresh_token,
        expires_in=settings.ACCESS_TOKEN_TTL_SECONDS,
    )


@router.post(
    "/refresh",
    response_model=TokenResponse,
    summary="Refresh tokens",
    description=(
        "Exchange a refresh token for a new access and refresh token. The presented "
        "refresh token is spent and works exactly once, even if sent twice at the same "
        "moment: using it again returns 401 (`auth.token_revoked`). If it was replaced "
        "more than the grace window ago (default 10 seconds), every session of the user "
        "also ends, because it may be a stolen copy. An expired token returns "
        "`auth.token_expired` and ends no session, even if it was already used."
    ),
)
async def refresh_token(
    body: RefreshRequest,
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> TokenResponse:
    """Refresh tokens.

    Exchange a refresh token for a new access and refresh token. The presented refresh token is
    spent and works exactly once, even if sent twice at the same moment: using it again returns
    401 (`auth.token_revoked`). If it was replaced more than the grace window ago (default 10
    seconds), every session of the user also ends, because it may be a stolen copy. An expired
    token returns `auth.token_expired` and ends no session, even if it was already used.
    """
    settings = get_settings()
    svc = _make_service(db)
    result = await svc.refresh(body.refresh_token)
    return TokenResponse(
        access_token=result.access_token,
        refresh_token=result.refresh_token,
        expires_in=settings.ACCESS_TOKEN_TTL_SECONDS,
    )


@router.post(
    "/logout",
    status_code=204,
    summary="Sign out",
    description=(
        "Revoke one of the caller's refresh tokens, ending that session. Requires a valid "
        "access token. An unknown or already revoked token, or one of another user, changes "
        "nothing and still returns 204."
    ),
)
async def logout(
    body: RefreshRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> None:
    """Sign out.

    Revoke one of the caller's refresh tokens, ending that session. Requires a valid access token.
    An unknown or already revoked token, or one of another user, changes nothing and still returns
    204.
    """
    svc = _make_service(db)
    await svc.logout(body.refresh_token, current_user.id)


@router.post(
    "/logout-others",
    status_code=204,
    summary="Sign out of my other devices",
    description=(
        "Revoke every refresh token of the signed-in user except the one of the session this "
        "request was made in (007 FR-009). Answers 409 (`session.current_unknown`) when that "
        "session is not known, for an access token issued before sessions were marked: nothing "
        "is revoked, and signing in again fixes it."
    ),
)
async def logout_others(
    current_user: User = Depends(get_current_user),
    session_id: uuid.UUID | None = Depends(current_session_id),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> None:
    """Sign out of my other devices.

    Keeps the caller's own session; refuses with 409 when it is not known, revoking nothing.
    """
    await _make_service(db).end_other_sessions(current_user.id, session_id)


@router.post(
    "/logout-all",
    status_code=204,
    summary="Sign out everywhere",
    description=(
        "Revoke every refresh token of the signed-in user. Access tokens already issued "
        "remain valid until they expire."
    ),
)
async def logout_all(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> None:
    """Sign out everywhere.

    Revoke every refresh token of the signed-in user. Access tokens already issued remain valid
    until they expire.
    """
    svc = _make_service(db)
    await svc.logout_all(current_user.id)
