"""Development only: issue a fresh invitation or reset token and show it once.

Usage:
    uv run python -m hds.scripts.dev_issue_link <email> <invite|reset>

The development mail outbox deliberately stores no usable token (specs/002-security-hardening,
FR-203), so a developer testing a flow by hand cannot read one from it. This script creates a new
one-time token for an existing account and prints it to the terminal, once. It is not written to
any file or log, and only its hash is stored in the database, exactly as for a real message.

It refuses to run unless ``ENVIRONMENT`` is ``development`` or ``test``.

Exit status: 0 token printed; 1 no eligible account; 2 refused (environment) or bad usage.
"""

import argparse
import asyncio
import sys
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.config import DEVELOPMENT_ENVIRONMENTS, get_settings
from hds.core.database import AsyncSessionLocal, engine
from hds.core.security import generate_refresh_token
from hds.repositories.invitation_token import InvitationTokenRepository
from hds.repositories.user import UserRepository
from hds.services.invitation import INVITE_TTL_DAYS, RESET_TTL_MINUTES


def _parser() -> argparse.ArgumentParser:
    """Build the command-line parser."""
    parser = argparse.ArgumentParser(
        prog="dev_issue_link", description="Issue a development invitation or reset token."
    )
    parser.add_argument("email", help="address of an existing account")
    parser.add_argument("purpose", choices=("invite", "reset"))
    return parser


async def run(
    argv: list[str], environment: str, session_factory: Callable[[], AsyncSession]
) -> int:
    """Issue one token and print it.

    An invitation is issued for a pending account (never activated, waiting to accept) and a reset
    for an active account, the same accounts the real flows serve. A deactivated account gets
    neither: staff reactivate it.

    Side effects:
        Stores the token's hash in the database and prints the token to standard output once.
        Nothing is written to a file or a log.

    Args:
        argv: ``[email, purpose]``.
        environment: The value of ``ENVIRONMENT``.
        session_factory: Opens a database session.

    Returns:
        0 when a token was printed, 1 when there is no eligible account, 2 when refused.

    Raises:
        SystemExit: With status 2 for a bad command line, as ``argparse`` does.
    """
    if environment.lower() not in DEVELOPMENT_ENVIRONMENTS:
        print(
            "dev_issue_link refuses to run: ENVIRONMENT must be development or test",
            file=sys.stderr,
        )
        return 2
    args = _parser().parse_args(argv)
    async with session_factory() as session:
        user = await UserRepository(session).get_by_email(args.email)
        if user is None:
            print("no account has that address", file=sys.stderr)
            return 1
        if args.purpose == "invite" and user.is_active:
            print(
                "that account is already active; an invitation completes a pending account",
                file=sys.stderr,
            )
            return 1
        if args.purpose == "invite" and user.activated_at is not None:
            print("that account is not pending; staff must reactivate it instead", file=sys.stderr)
            return 1
        if args.purpose == "reset" and not user.is_active:
            print("that account is not active; a reset needs an active account", file=sys.stderr)
            return 1
        raw, token_hash = generate_refresh_token()
        if args.purpose == "invite":
            expires_at = datetime.now(UTC) + timedelta(days=INVITE_TTL_DAYS)
            lifetime = f"{INVITE_TTL_DAYS} days"
        else:
            expires_at = datetime.now(UTC) + timedelta(minutes=RESET_TTL_MINUTES)
            lifetime = f"{RESET_TTL_MINUTES} minutes"
        await InvitationTokenRepository(session).create(
            user_id=user.id, token_hash=token_hash, purpose=args.purpose, expires_at=expires_at
        )
        await session.commit()
    print(f"{args.purpose} token for {args.email} (valid {lifetime}): {raw}")
    return 0


async def _main(argv: list[str]) -> int:
    """Run against the configured database, then release its connections."""
    try:
        return await run(argv, get_settings().ENVIRONMENT, AsyncSessionLocal)
    finally:
        await engine.dispose()


def main(argv: list[str] | None = None) -> int:
    """Command-line entry point.

    Args:
        argv: Arguments without the program name; defaults to ``sys.argv[1:]``.

    Returns:
        The exit status described in the module docstring.
    """
    return asyncio.run(_main(sys.argv[1:] if argv is None else argv))


if __name__ == "__main__":
    sys.exit(main())
