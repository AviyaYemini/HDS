"""The wider dev seed (stabilization S21-S23): it obeys the rules, repeats exactly, and runs once.

Each test fills a scratch database with the base seed and the wide layer, laid around a fixed day,
as `seed_dev --wide` would. Every assignment the scheduler made breaks no rule the roster checks
(only the seed's own hand placements do, on purpose); two runs on the same day give the same data;
and running the layer again adds nothing.

Covers: specs/chore-stabilization/plan.md (PR 3); the roster's rules of 004 and 007 (FR-039).
"""

import datetime
import hashlib
from collections.abc import AsyncGenerator

import pytest_asyncio
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from hds.models.shift import Assignment, Shift
from hds.models.user import User
from hds.scripts.seed_dev import seed
from hds.scripts.seed_wide import seed_wide
from hds.wiring import roster_service_for
from tests.integration.migration_support import Scratch, scratch_database

TODAY = datetime.date(2027, 3, 10)  # a Wednesday; every date is laid around it
HEAD = "head"


def factory_on(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


async def seeded(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """The base seed and the wide layer on ``engine``'s database, around ``TODAY``."""
    factory = factory_on(engine)
    await seed(factory=factory)
    assert await seed_wide(factory, TODAY) is True
    return factory


@pytest_asyncio.fixture
async def scratch() -> AsyncGenerator[Scratch, None]:
    async with scratch_database("hds_seed_wide_test", HEAD) as pair:
        yield pair


@pytest_asyncio.fixture
async def other_scratch() -> AsyncGenerator[Scratch, None]:
    async with scratch_database("hds_seed_wide_again_test", HEAD) as pair:
        yield pair


async def digest(factory: async_sessionmaker[AsyncSession]) -> str:
    """A digest of what the seed decides: the people, and who works which published shift."""
    async with factory() as db:
        people = (
            await db.execute(select(User.email, User.full_name, User.role).order_by(User.email))
        ).all()
        work = (
            await db.execute(
                select(Shift.starts_at, User.email)
                .join(Assignment, Assignment.shift_id == Shift.id)
                .join(User, User.id == Assignment.user_id)
                .where(Shift.status == "published")
                .order_by(Shift.starts_at, User.email)
            )
        ).all()
    return hashlib.sha256(repr((people, work)).encode()).hexdigest()


async def test_the_scheduler_s_assignments_break_no_rule_and_a_rerun_adds_nothing(
    scratch: Scratch,
) -> None:
    _, engine = scratch
    factory = await seeded(engine)
    async with factory() as db:
        admin = (await db.execute(select(User).where(User.email == "admin@hds.test"))).scalar_one()
        shifts = (await db.execute(select(Shift.id).where(Shift.status == "published"))).scalars()
        published = list(shifts)
        # The shifts the seed placed someone on by hand, on purpose against a rule.
        by_hand = set(
            (
                await db.execute(select(Assignment.shift_id).where(Assignment.source == "manual"))
            ).scalars()
        )
        warnings = await roster_service_for(db).judge(admin, published)
        counts = {
            table: (await db.execute(text(f"SELECT count(*) FROM {table}"))).scalar_one()  # noqa: S608
            for table in ("users", "shifts", "assignments", "availability_entries", "schedule_jobs")
        }
    assert len(published) > 300
    # A warning on no shift the seed placed someone on by hand is the scheduler's own: none.
    theirs = [w for w in warnings if not by_hand.intersection(w["shift_ids"])]
    assert theirs == []

    assert await seed_wide(factory, TODAY) is False
    async with factory() as db:
        again = {
            table: (await db.execute(text(f"SELECT count(*) FROM {table}"))).scalar_one()  # noqa: S608
            for table in counts
        }
        open_runs = (
            await db.execute(
                text("SELECT count(*) FROM schedule_jobs WHERE proposal_state = 'open'")
            )
        ).scalar_one()
        hand = (
            await db.execute(
                select(func.count()).select_from(Assignment).where(Assignment.note.is_not(None))
            )
        ).scalar_one()
    assert again == counts
    assert open_runs == 1  # next week's proposal, waiting for review
    assert hand == 3  # the seed's hand placements, each with a note


async def test_the_same_day_gives_the_same_data(scratch: Scratch, other_scratch: Scratch) -> None:
    first = await digest(await seeded(scratch[1]))
    second = await digest(await seeded(other_scratch[1]))

    assert first == second
