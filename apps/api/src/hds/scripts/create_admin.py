"""Create the first Admin of an empty database, with its organisation (specs/006 research R13).

Usage:
    uv run python -m hds.scripts.create_admin --email a@example.com --organisation "Acme"
        --timezone Asia/Jerusalem [--name "Dana Levi"] < password  (one command line)

The shared environment has no seed, so its operator runs this once (``make ops-create-admin``).
The address comes from the arguments and the password from standard input: typed at a hidden
prompt in a terminal, or the first line of a pipe, so it never sits in the shell's history or the
process list. It is refused once any user exists. The password is never printed, and the address
is shown masked.

Layer: entry point, like ``hds.main``; the rules are in ``hds.services.first_admin``.

Exit status: 0 created; 1 refused (users exist, or a bad password, address, name or time zone);
2 bad usage.
"""

import argparse
import asyncio
import getpass
import sys
from typing import TextIO

from pydantic import TypeAdapter, ValidationError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from hds.core.config import configure_email_validation, get_settings
from hds.core.database import unit_of_work
from hds.core.exceptions import ConflictError, DomainValidationError
from hds.core.logging import mask_email
from hds.core.types import Email
from hds.repositories.organisation import OrganisationRepository
from hds.repositories.user import UserRepository
from hds.services.first_admin import FirstAdminService

_EMAIL = TypeAdapter(Email)


def _parser() -> argparse.ArgumentParser:
    """Build the command-line parser."""
    parser = argparse.ArgumentParser(
        prog="create_admin",
        description="Create the first Admin and its organisation; the password is read from stdin.",
    )
    parser.add_argument("--email", required=True, help="the Admin's address")
    parser.add_argument("--organisation", required=True, help="the organisation's name")
    parser.add_argument("--timezone", required=True, help="an IANA zone, such as Asia/Jerusalem")
    parser.add_argument("--name", default="Administrator", help="the Admin's name")
    return parser


def _read_password(stdin: TextIO) -> str:
    """The password: a hidden prompt in a terminal, else the first line of the input."""
    if stdin.isatty():
        return getpass.getpass("Password for the first Admin: ")
    return stdin.readline().rstrip("\r\n")


async def run(
    argv: list[str],
    stdin: TextIO,
    out: TextIO,
    factory: async_sessionmaker[AsyncSession],
) -> int:
    """Run the command: parse, read the password, create, and report; never print the password.

    Args:
        argv: The arguments, without the program name.
        stdin: Where the password is read from.
        out: Where the outcome is written.
        factory: Sessions on the application database.

    Returns:
        The exit status: 0 created, 1 refused, 2 bad usage.
    """
    try:
        args = _parser().parse_args(argv)
    except SystemExit as stop:  # --help (0) or bad usage (2); review R22
        return 2 if stop.code is None else int(stop.code)
    password = _read_password(stdin)
    if not password:
        print("create_admin: no password was given on standard input", file=out)
        return 1
    configure_email_validation(get_settings())
    try:
        email = _EMAIL.validate_python(args.email)
    except ValidationError:
        print("create_admin: the email address is not valid", file=out)
        return 1
    try:
        async with factory() as session, unit_of_work(session):
            service = FirstAdminService(UserRepository(session), OrganisationRepository(session))
            admin = await service.create(
                organisation=args.organisation,
                timezone=args.timezone,
                email=email,
                full_name=args.name,
                password=password,
            )
    except (ConflictError, DomainValidationError) as refused:
        print(f"create_admin: {refused.detail}", file=out)
        return 1
    except SQLAlchemyError as error:
        # The type only: the error's text quotes the statement's parameters, the address and the
        # password's hash among them (review R19).
        print(f"create_admin: the database refused the change ({type(error).__name__})", file=out)
        return 1
    print(
        f"create_admin: created the organisation {args.organisation.strip()!r} and its first "
        f"Admin {mask_email(admin.email)}",
        file=out,
    )
    return 0


async def _main() -> int:
    """Run on the configured database, and close its connections before the loop ends."""
    from hds.core.database import engine, get_session_factory

    try:
        return await run(sys.argv[1:], sys.stdin, sys.stdout, get_session_factory())
    finally:
        await engine.dispose()  # as every script does (review R23)


def main() -> None:
    """Run and exit with the command's status."""
    sys.exit(asyncio.run(_main()))


if __name__ == "__main__":
    main()
