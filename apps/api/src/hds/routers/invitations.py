"""Invitation and password-reset router (US6 — Proposed)."""

from fastapi import APIRouter, Depends, Response
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.database import get_db
from hds.core.deps import require_manager
from hds.core.mail import MailSender, get_mail_sender
from hds.models.user import User, UserRole
from hds.repositories.invitation_token import InvitationTokenRepository
from hds.repositories.user import UserRepository
from hds.services.invitation import InvitationService

router = APIRouter(prefix="/auth", tags=["invitations"])


class InviteRequest(BaseModel):
    email: EmailStr
    role: UserRole = UserRole.employee


class AcceptInviteRequest(BaseModel):
    token: str
    password: str = Field(min_length=12)


class PasswordResetRequest(BaseModel):
    email: EmailStr


class PasswordResetConfirm(BaseModel):
    token: str
    new_password: str = Field(min_length=12)


def _svc(db: AsyncSession, mailer: MailSender) -> InvitationService:
    return InvitationService(
        user_repo=UserRepository(db),
        token_repo=InvitationTokenRepository(db),
        mailer=mailer,
    )


@router.post("/invite", status_code=202)
async def create_invitation(
    body: InviteRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
    mailer: MailSender = Depends(get_mail_sender),
) -> Response:
    await _svc(db, mailer).create_invitation(
        actor=current_user, email=str(body.email), role=body.role
    )
    return Response(status_code=202)


@router.post("/invite/accept", status_code=204)
async def accept_invitation(
    body: AcceptInviteRequest,
    db: AsyncSession = Depends(get_db),
    mailer: MailSender = Depends(get_mail_sender),
) -> None:
    await _svc(db, mailer).accept_invitation(raw_token=body.token, new_password=body.password)


@router.post("/password-reset/request", status_code=202)
async def request_password_reset(
    body: PasswordResetRequest,
    db: AsyncSession = Depends(get_db),
    mailer: MailSender = Depends(get_mail_sender),
) -> Response:
    await _svc(db, mailer).request_password_reset(email=str(body.email))
    return Response(status_code=202)


@router.post("/password-reset/confirm", status_code=204)
async def confirm_password_reset(
    body: PasswordResetConfirm,
    db: AsyncSession = Depends(get_db),
    mailer: MailSender = Depends(get_mail_sender),
) -> None:
    await _svc(db, mailer).reset_password(raw_token=body.token, new_password=body.new_password)
