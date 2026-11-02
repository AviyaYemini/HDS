"""Auth router: login, refresh, logout, logout-all."""

from fastapi import APIRouter, Depends
from pydantic import BaseModel, EmailStr
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.config import get_settings
from hds.core.database import get_db
from hds.core.deps import get_current_user
from hds.core.types import Password, Token
from hds.models.user import User
from hds.repositories.refresh_token import RefreshTokenRepository
from hds.repositories.user import UserRepository
from hds.services.auth import AuthService

router = APIRouter(prefix="/auth", tags=["auth"])


class LoginRequest(BaseModel):
    email: EmailStr
    password: Password


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    refresh_token: str


class RefreshRequest(BaseModel):
    refresh_token: Token


def _make_service(db: AsyncSession) -> AuthService:
    settings = get_settings()
    return AuthService(
        user_repo=UserRepository(db),
        token_repo=RefreshTokenRepository(db),
        config=settings,
    )


@router.post("/login", response_model=TokenResponse)
async def login(
    body: LoginRequest,
    db: AsyncSession = Depends(get_db),
) -> TokenResponse:
    settings = get_settings()
    svc = _make_service(db)
    result = await svc.login(str(body.email), body.password)
    return TokenResponse(
        access_token=result.access_token,
        refresh_token=result.refresh_token,
        expires_in=settings.ACCESS_TOKEN_TTL_SECONDS,
    )


@router.post("/refresh", response_model=TokenResponse)
async def refresh_token(
    body: RefreshRequest,
    db: AsyncSession = Depends(get_db),
) -> TokenResponse:
    settings = get_settings()
    svc = _make_service(db)
    result = await svc.refresh(body.refresh_token)
    return TokenResponse(
        access_token=result.access_token,
        refresh_token=result.refresh_token,
        expires_in=settings.ACCESS_TOKEN_TTL_SECONDS,
    )


@router.post("/logout", status_code=204)
async def logout(
    body: RefreshRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> None:
    svc = _make_service(db)
    await svc.logout(body.refresh_token)


@router.post("/logout-all", status_code=204)
async def logout_all(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> None:
    svc = _make_service(db)
    await svc.logout_all(current_user.id)
