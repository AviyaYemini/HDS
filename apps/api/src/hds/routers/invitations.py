"""Invitation and password-reset router (US6 — Proposed).

Layer: routers. Delegates to ``InvitationService``. Accepting an invite and both password-reset
endpoints are public because the caller has no session yet; the one-time token is the credential.
"""

import uuid
from collections.abc import Callable
from typing import Literal

from fastapi import APIRouter, Depends, Request, Response
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from hds.core.background import run_reset_job
from hds.core.client_address import client_address
from hds.core.config import get_settings
from hds.core.database import after_commit, get_session_factory, get_unit_of_work, unit_of_work
from hds.core.deps import require_manager
from hds.core.mail import DeferredMailer, MailSender, get_mail_sender
from hds.core.types import Email, Password, StrictDate, Token
from hds.models.user import User, UserRole
from hds.repositories.client import ClientRepository
from hds.repositories.group import GroupRepository
from hds.repositories.invitation_token import InvitationTokenRepository
from hds.repositories.login_throttle import LoginThrottleRepository
from hds.repositories.project_manager import ProjectManagerRepository
from hds.repositories.project_member import ProjectMemberRepository
from hds.repositories.qualification import QualificationRepository
from hds.repositories.refresh_token import RefreshTokenRepository
from hds.repositories.user import UserRepository
from hds.services.employee import EmployeeService
from hds.services.invitation import InvitationService

router = APIRouter(prefix="/auth", tags=["invitations"])


class InviteRequest(BaseModel):
    """Email and role to invite; the role defaults to employee."""

    email: Email
    role: UserRole = UserRole.employee
    employment_type: Literal["permanent", "temporary"] | None = Field(
        default=None, description="Temporary for a worker on given projects for up to 3 months."
    )
    engaged_from: StrictDate | None = None
    engaged_until: StrictDate | None = None
    project_ids: list[uuid.UUID] | None = Field(default=None, max_length=50)


class AcceptInviteRequest(BaseModel):
    """Invitation token and the chosen password (at least 12 characters)."""

    token: Token
    password: Password = Field(min_length=12)


class PasswordResetRequest(BaseModel):
    """Email address that asked for a password reset."""

    email: Email


class PasswordResetConfirm(BaseModel):
    """Reset token and the new password (at least 12 characters)."""

    token: Token
    new_password: Password = Field(min_length=12)


def _svc(db: AsyncSession, mailer: MailSender) -> InvitationService:
    return InvitationService(
        user_repo=UserRepository(db),
        token_repo=InvitationTokenRepository(db),
        mailer=DeferredMailer(db, mailer),  # mail goes out only after the request is saved
        refresh_token_repo=RefreshTokenRepository(db),
        throttle_repo=LoginThrottleRepository(db),
        config=get_settings(),
    )


def _queue_reset_job(
    db: AsyncSession, factory: async_sessionmaker[AsyncSession], mailer: MailSender
) -> Callable[[str], None]:
    """Return what queues a reset job on this request, to run after the response.

    The job needs a session of its own, because the request's has finished by then.
    """

    def queue(email: str) -> None:
        async def reset_job() -> None:
            async def issue() -> None:
                async with factory() as job_db, unit_of_work(job_db):
                    await _svc(job_db, mailer).issue_reset_link(email)

            await run_reset_job(issue)

        after_commit(db, reset_job)

    return queue


@router.post(
    "/invite",
    status_code=202,
    response_class=Response,
    summary="Invite a user",
    description=(
        "Manager or Admin. Invite an email address and email a one-time invitation token "
        "valid for 7 days. A new address gets a pending account (never activated). "
        "Re-inviting a pending account of the same organisation sets the new role and "
        "replaces the earlier link, unless it is a Client account, whose role cannot change "
        "(409 `users.client_role_locked`). Only an Admin may invite an Admin or change a "
        "pending Admin; Client users are created from the client record (`users.role_invalid`). "
        "An email that belongs to an active account, or to another organisation, returns "
        "409 (`users.duplicate_email`); a deactivated account returns 409 "
        "(`invitations.account_deactivated`) and must be reactivated by staff instead. "
        "Answers 503 (`service.busy`) with `Retry-After` when the password-hashing queue "
        "is full; nothing is changed."
    ),
)
async def create_invitation(
    body: InviteRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
    mailer: MailSender = Depends(get_mail_sender),
) -> Response:
    """Invite a user.

    Manager or Admin. Invite an email address and email a one-time invitation token valid for 7
    days. A new address gets a pending account (never activated). Re-inviting a pending account
    of the same organisation sets the new role and replaces the earlier link. Only an Admin may
    invite an Admin or change a pending Admin; Client users are created from the client record
    (`users.role_invalid`). An email that belongs to an active account, or to another
    organisation, returns 409 (`users.duplicate_email`); a deactivated account returns 409
    (`invitations.account_deactivated`) and must be reactivated by staff instead.
    Answers 503 (`service.busy`) with `Retry-After` when the password-hashing queue is full;
    nothing is changed.
    """
    user_id = await _svc(db, mailer).create_invitation(
        actor=current_user, email=str(body.email), role=body.role
    )
    engagement = body.model_dump(
        include={"employment_type", "engaged_from", "engaged_until", "project_ids"},
        exclude_none=True,
    )
    if engagement:
        # The invitee is a temporary worker from the start (CEO demo): the same rules as editing
        # one; a refusal undoes the invitation too, as it is one unit of work.
        await EmployeeService(
            user_repo=UserRepository(db),
            token_repo=RefreshTokenRepository(db),
            link_repo=InvitationTokenRepository(db),
            group_repo=GroupRepository(db),
            qualification_repo=QualificationRepository(db),
            client_repo=ClientRepository(db),
            project_manager_repo=ProjectManagerRepository(db),
            member_repo=ProjectMemberRepository(db),
        ).set_engagement(current_user, user_id, engagement)
    return Response(status_code=202)


@router.post(
    "/invite/accept",
    status_code=204,
    summary="Accept an invitation",
    description=(
        "Public. Redeem an invitation token to set the password and activate a pending "
        "account. A token for an account that is not pending (already active, or "
        "deactivated) returns 401 (`auth.token_used`) and changes nothing, as does a "
        "spent or cancelled token; an unknown or expired token returns 401 "
        "(`auth.token_expired`). The password needs at least 12 characters. "
        "Answers 503 (`service.busy`) with `Retry-After` when the password-hashing queue "
        "is full; nothing is changed."
    ),
)
async def accept_invitation(
    body: AcceptInviteRequest,
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
    mailer: MailSender = Depends(get_mail_sender),
) -> None:
    """Accept an invitation.

    Public. Redeem an invitation token to set the password and activate a pending account. A
    token for an account that is not pending (already active, or deactivated) returns 401
    (`auth.token_used`) and changes nothing, as does a spent or cancelled token; an unknown or
    expired token returns 401 (`auth.token_expired`). The password needs at least 12 characters.
    Answers 503 (`service.busy`) with `Retry-After` when the password-hashing queue is full;
    nothing is changed.
    """
    await _svc(db, mailer).accept_invitation(raw_token=body.token, new_password=body.password)


@router.post(
    "/password-reset/request",
    status_code=202,
    response_class=Response,
    summary="Request a password reset",
    description=(
        "Public. Email a one-time reset token valid for 60 minutes if the address "
        "belongs to an active account (letter case is ignored). The request only counts "
        "and answers: the account is looked at and the email sent after the response, so "
        "a failure there is not reported, and an email can be lost if the service stops. "
        "Earlier tokens keep working until one is redeemed, which cancels the rest. "
        "Requests are limited per address and per network in a 60-minute window; over a "
        "limit nothing is issued or sent. Always answers 202, whether or not the address "
        "is registered and whether or not a limit is reached, so it cannot be used to "
        "find out who has an account."
    ),
)
async def request_password_reset(
    body: PasswordResetRequest,
    request: Request,
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
    mailer: MailSender = Depends(get_mail_sender),
    factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
) -> Response:
    """Request a password reset.

    Public. Email a one-time reset token valid for 60 minutes if the address belongs to an
    active account (letter case is ignored). The request only counts and answers: the account is
    looked at and the email sent after the response, so a failure there is not reported, and an
    email can be lost if the service stops. Earlier tokens keep working until one is redeemed, which
    cancels the rest. Requests are limited per address and per network in a 60-minute window; over a
    limit nothing is issued or sent. Always answers 202, whether or not the address is registered
    and whether or not a limit is reached, so it cannot be used to find out who has an account.
    """
    network = client_address(request, get_settings().TRUSTED_PROXY_COUNT)
    await _svc(db, mailer).request_password_reset(
        str(body.email), network, _queue_reset_job(db, factory, mailer)
    )
    return Response(status_code=202)


@router.post(
    "/password-reset/confirm",
    status_code=204,
    summary="Confirm a password reset",
    description=(
        "Public. Redeem a reset token to set a new password for an active account. All "
        "of the user's refresh tokens are revoked, ending every session, every other "
        "outstanding token of the account is cancelled, and any sign-in lock is cleared. "
        "An unknown or "
        "expired token returns 401 (`auth.token_expired`); a spent or cancelled token, or "
        "a token for an account that is not active, returns 401 (`auth.token_used`) and "
        "changes nothing. "
        "Answers 503 (`service.busy`) with `Retry-After` when the password-hashing queue "
        "is full; nothing is changed."
    ),
)
async def confirm_password_reset(
    body: PasswordResetConfirm,
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
    mailer: MailSender = Depends(get_mail_sender),
) -> None:
    """Confirm a password reset.

    Public. Redeem a reset token to set a new password for an active account. All of the user's
    refresh tokens are revoked, ending every session, every other outstanding token of the account
    is cancelled, and any sign-in lock is cleared. An unknown or expired token returns 401
    (`auth.token_expired`); a spent or cancelled token, or a token for an account that is not
    active, returns 401 (`auth.token_used`) and changes nothing.
    Answers 503 (`service.busy`) with `Retry-After` when the password-hashing queue is full;
    nothing is changed.
    """
    await _svc(db, mailer).reset_password(raw_token=body.token, new_password=body.new_password)
