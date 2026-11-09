"""List accounts whose addresses differ only by letter case (read-only).

Usage:
    uv run python -m hds.scripts.find_email_case_duplicates

Run it before applying migration 016 (specs/002-security-hardening, FR-216). It reads the user
table and changes nothing. Each clash is printed as one line of account ids and organisation ids;
addresses are not printed, so the output can be shared. Look an account up by id to see its address.

Exit status: 0 no clashes (nothing is printed); 1 clashes found.
"""

import asyncio
import sys
import uuid
from collections import defaultdict
from collections.abc import Awaitable, Callable, Iterable

from sqlalchemy import select

from hds.core.database import AsyncSessionLocal, engine
from hds.models.user import User

Row = tuple[uuid.UUID, uuid.UUID, str]
"""``(account id, organisation id, address)``."""


def find_clashes(rows: Iterable[Row]) -> list[list[Row]]:
    """Group accounts that hold the same address apart from letter case.

    Args:
        rows: Live (non-deleted) accounts.

    Returns:
        One list per clash, each with two or more accounts; empty when the data is clean.
    """
    by_address: dict[str, list[Row]] = defaultdict(list)
    for row in rows:
        by_address[row[2].strip().lower()].append(row)
    return [group for group in by_address.values() if len(group) > 1]


async def run(fetch: Callable[[], Awaitable[list[Row]]]) -> int:
    """Fetch the accounts, print each clash by id, and return the exit status.

    Args:
        fetch: Returns every non-deleted account as ``(id, organisation id, address)``.

    Returns:
        0 when there is no clash (nothing printed), 1 when there is at least one.
    """
    groups = find_clashes(await fetch())
    for group in groups:
        print("; ".join(f"{user_id} (organisation {org_id})" for user_id, org_id, _ in group))
    return 1 if groups else 0


async def _fetch_live_accounts() -> list[Row]:
    """Read id, organisation and address of every non-deleted account."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(User.id, User.organisation_id, User.email).where(User.is_deleted.is_(False))
        )
        return [(i, o, e) for i, o, e in result.all()]


async def _main() -> int:
    """Run against the configured database, then release its connections."""
    try:
        return await run(_fetch_live_accounts)
    finally:
        await engine.dispose()


def main() -> int:
    """Command-line entry point.

    Returns:
        The exit status described in the module docstring.
    """
    return asyncio.run(_main())


if __name__ == "__main__":
    sys.exit(main())
