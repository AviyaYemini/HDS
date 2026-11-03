"""Read and set the sign-in failure count of an address, for tests.

Layer: tests. Every address, registered or not, is counted in ``login_throttle`` under kind
``address`` (FR-221); the account's own columns are not used.

Covers: none directly; helpers for FR-221.
"""

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.config import get_settings
from hds.services.throttle import address_key as _address_key


def address_key(email: str) -> str:
    """The stored key of an address."""
    return _address_key(email, get_settings().throttle_key)


async def address_state(db: AsyncSession, email: str) -> tuple[int, bool]:
    """``(consecutive failures, locked)`` as stored; ``(0, False)`` when there is no row."""
    await db.rollback()
    row = (
        await db.execute(
            text(
                "SELECT failures, locked_until IS NOT NULL FROM login_throttle "
                "WHERE kind = 'address' AND key_hash = :k"
            ),
            {"k": address_key(email)},
        )
    ).one_or_none()
    return (0, False) if row is None else (int(row[0]), bool(row[1]))


async def set_address_state(
    db: AsyncSession, email: str, failures: int, locked_until: datetime | None
) -> None:
    """Store a count and a lock end for an address, as earlier failures would have."""
    await db.execute(
        text(
            "INSERT INTO login_throttle (kind, key_hash, failures, locked_until, updated_at) "
            "VALUES ('address', :k, :f, :l, now()) ON CONFLICT (kind, key_hash) DO UPDATE "
            "SET failures = :f, locked_until = :l, updated_at = now()"
        ),
        {"k": address_key(email), "f": failures, "l": locked_until},
    )
    await db.commit()
