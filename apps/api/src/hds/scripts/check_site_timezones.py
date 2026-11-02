"""List sites whose time zone is not a real IANA name (read-only).

Usage:
    uv run python -m hds.scripts.check_site_timezones

Sites created before the check (specs/002-security-hardening, FR-217) may hold a name such as
``Mars/Base``. This reads the sites table and changes nothing: the right zone cannot be guessed,
so staff correct each site themselves. Each bad site is printed as its id and organisation id (no
names or addresses).

Exit status: 0 every zone is valid (nothing is printed); 1 at least one is not.
"""

import asyncio
import sys
import uuid
from collections.abc import Awaitable, Callable, Iterable

from sqlalchemy import select

from hds.core.database import AsyncSessionLocal, engine
from hds.models.site import Site
from hds.services.site import validate_timezone

Row = tuple[uuid.UUID, uuid.UUID, str]
"""``(site id, organisation id, time zone)``."""


def is_valid_zone(name: str) -> bool:
    """Whether ``name`` is a time zone the system can load (the rule the service enforces)."""
    try:
        validate_timezone(name)
    except Exception:
        return False
    return True


def find_invalid(rows: Iterable[Row]) -> list[Row]:
    """Pick out the sites whose time zone cannot be loaded.

    Args:
        rows: Sites as ``(id, organisation id, time zone)``.

    Returns:
        The invalid ones, in the order given.
    """
    return [row for row in rows if not is_valid_zone(row[2])]


async def run(fetch: Callable[[], Awaitable[list[Row]]]) -> int:
    """Fetch the sites, print each invalid one by id, and return the exit status.

    Args:
        fetch: Returns every site as ``(id, organisation id, time zone)``.

    Returns:
        0 when every zone is valid (nothing printed), 1 otherwise.
    """
    bad = find_invalid(await fetch())
    for site_id, org_id, _ in bad:
        print(f"site {site_id} (organisation {org_id})")
    return 1 if bad else 0


async def _fetch_sites() -> list[Row]:
    """Read id, organisation and time zone of every site."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Site.id, Site.organization_id, Site.timezone))
        return [(i, o, z) for i, o, z in result.all()]


async def _main() -> int:
    """Run against the configured database, then release its connections."""
    try:
        return await run(_fetch_sites)
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
