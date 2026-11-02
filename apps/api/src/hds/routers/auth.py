"""Auth router: login, refresh, logout, logout-all.

Layer: routers. Delegates to ``AuthService``; the only public endpoints are login and refresh.
Paths are served under ``/auth`` without the ``/api/v1`` prefix used by the other routers.
"""

from fastapi import APIRouter, Depends
from pydantic import BaseModel, EmailStr
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.config import get_settings
from hds.core.database import get_unit_of_work
from hds.core.deps import get_current_user
from hds.core.types import Password, Token
from hds.models.user import User
from hds.repositories.refresh_token import RefreshTokenRepository
from hds.repositories.user import UserRepository
from hds.services.auth import AuthService

router = APIRouter(prefix="/auth", tags=["auth"])


class LoginRequest(BaseModel):
    """Credentials for ``POST /auth/login``."""

    email: EmailStr
    password: Password


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
    )


@router.post(
    "/login",
    response_model=TokenResponse,
    summary="Sign in",
    description=(
        "Exchange an email and password for an access token and a refresh token. "
        "Unknown email, wrong password and inactive account all return the same 401 "
        "(`auth.invalid_credentials`). After repeated failures the account is locked "
        "and the call returns 423 (`auth.account_locked`)."
    ),
)
async def login(
    body: LoginRequest,
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> TokenResponse:
    """Sign in.

    Exchange an email and password for an access token and a refresh token. Unknown email, wrong
    password and inactive account all return the same 401 (`auth.invalid_credentials`). After
    repeated failures the account is locked and the call returns 423 (`auth.account_locked`).
    """
    settings = get_settings()
    svc = _make_service(db)
    result = await svc.login(str(body.email), body.password)
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
        "refresh token is spent: using it again returns 401 (`auth.token_revoked`). An "
        "expired token returns `auth.token_expired`."
    ),
)
async def refresh_token(
    body: RefreshRequest,
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> TokenResponse:
    """Refresh tokens.

    Exchange a refresh token for a new access and refresh token. The presented refresh token is
    spent: using it again returns 401 (`auth.token_revoked`). An expired token returns
    `auth.token_expired`.
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
        "Revoke one refresh token, ending that session. Requires a valid access token. "
        "Revoking an unknown or already revoked token still returns 204."
    ),
)
async def logout(
    body: RefreshRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> None:
    """Sign out.

    Revoke one refresh token, ending that session. Requires a valid access token. Revoking an
    unknown or already revoked token still returns 204.
    """
    svc = _make_service(db)
    await svc.logout(body.refresh_token)


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
