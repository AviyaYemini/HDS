"""Accepting a proposal and a keyed assignment never deadlock (PR 6 CI finding).

An assignment with an ``Idempotency-Key`` inserts its key first, which share-locks the Admin's user
row through the key's foreign key, and then locks its shift. Accepting locks the shifts in the
run's surroundings first and then every staff row. With ``FOR UPDATE`` on the staff rows the two
waited on each other; ``FOR NO KEY UPDATE`` does not conflict with a key-share lock.

Covers: 004 FR-023, FR-024; research G9.
"""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.user import User
from hds.repositories.idempotency_key import IdempotencyKeyRepository
from hds.repositories.organisation import OrganisationRepository
from hds.repositories.project import ProjectRepository
from hds.repositories.shift import ShiftRepository
from hds.repositories.shift_type import ShiftTypeRepository
from hds.repositories.user import UserRepository
from tests.roster_support import add_shift, headers_for
from tests.schedule_support import SUNDAY, staffed_project


async def test_accepts_staff_lock_does_not_deadlock_with_a_keyed_assignment(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    session_pair: tuple[AsyncSession, AsyncSession],
) -> None:
    admin_headers = await headers_for(async_client, "admin")
    project = await staffed_project(async_client, admin_headers)
    made = await add_shift(async_client, admin_headers, project, SUNDAY + timedelta(days=1))
    shift_id = uuid.UUID(made["shift"]["id"])
    admin = seed_users["admin"]
    org = admin.organization_id
    assigning, accepting = session_pair

    # A: the keyed assignment claims its key, share-locking the Admin's row through the key.
    held = await IdempotencyKeyRepository(assigning).reserve(
        admin.id, "week-23", "0" * 64, datetime.now(UTC) - timedelta(days=1)
    )
    assert held is None
    # B: accept's locks, up to the shifts of the surroundings.
    await OrganisationRepository(accepting).lock(org)
    await ProjectRepository(accepting).lock_for_shifts(org, None)
    await ShiftTypeRepository(accepting).share_lock_all(org)
    start = datetime.combine(SUNDAY, datetime.min.time(), UTC) - timedelta(days=2)
    await ShiftRepository(accepting).lock_between(org, start, start + timedelta(days=14))

    # A now waits for the shift B holds.
    waiting = asyncio.create_task(ShiftRepository(assigning).get_locked(shift_id, org))
    await asyncio.sleep(0.3)
    assert not waiting.done()

    # B takes the staff rows; with FOR UPDATE this waited on A's key-share lock: a deadlock.
    await asyncio.wait_for(UserRepository(accepting).lock_staff(org), timeout=10)
    await accepting.rollback()

    found = await asyncio.wait_for(waiting, timeout=10)
    assert found is not None and found.id == shift_id
    await assigning.rollback()
