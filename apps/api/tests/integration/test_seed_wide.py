"""The wider dev seed (stabilization S21-S23): it obeys the rules, repeats, and finishes once.

Each test fills a scratch database with the base seed and the wide layer, laid around a fixed day,
as `seed_dev --wide` would. Every assignment the scheduler made breaks no rule the roster checks
(only the seed's own hand placements do, on purpose); two runs on the same day give the same
people, availability and filled places; running the layer again adds nothing; and a run that
stopped is finished by the next, which keeps a window a developer made.

Covers: specs/chore-stabilization/plan.md (PR 3); the roster's rules of 004 and 007 (FR-039).
"""

import datetime
import hashlib
from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from hds.models.shift import Assignment, Shift
from hds.models.user import User
from hds.scripts import seed_wide as seed_wide_module
from hds.scripts.seed_dev import SITE_ZONE, _sunday_of, seed
from hds.scripts.seed_wide import seed_wide
from hds.wiring import roster_service_for
from tests.integration.migration_support import Scratch, scratch_database

# Today at the site, as `make up` would lay it: the services judge deadlines by the real clock,
# so a fixed day would turn next week's open window closed once that day had passed.
TODAY = datetime.datetime.now(SITE_ZONE).date()
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
    """A digest of what the seed decides: the people, their availability, and how full each week is.

    Not who works which shift: the scheduler may settle an equal choice differently in another
    database, so that is the scheduler's to repeat, not the seed's.
    """
    async with factory() as db:
        people = (
            await db.execute(select(User.email, User.full_name, User.role).order_by(User.email))
        ).all()
        entries = (
            await db.execute(
                text(
                    "SELECT u.email, e.kind, e.starts_on, e.ends_on FROM availability_entries e "
                    "JOIN users u ON u.id = e.user_id ORDER BY 1, 3, 2"
                )
            )
        ).all()
        weeks = (
            await db.execute(
                text(
                    "SELECT first_day, proposal_state, needed, best_filled FROM schedule_jobs "
                    "ORDER BY first_day"
                )
            )
        ).all()
    return hashlib.sha256(repr((people, entries, weeks)).encode()).hexdigest()


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


async def with_window_over_next_week(factory: async_sessionmaker[AsyncSession]) -> None:
    """The base seed, and a developer's window that covers next week from its Tuesday on."""
    await seed(factory=factory)
    tuesday = _sunday_of(TODAY) + datetime.timedelta(days=9)
    async with factory() as db:
        org = (await db.execute(text("SELECT id FROM organisations"))).scalar_one()
        await db.execute(
            text(
                "INSERT INTO submission_windows (id, organisation_id, starts_on, ends_on, "
                "deadline) VALUES (gen_random_uuid(), :o, :f, :l, now() + interval '30 days')"
            ),
            {"o": org, "f": tuesday, "l": tuesday + datetime.timedelta(days=6)},
        )
        await db.commit()


async def test_a_run_that_stopped_is_finished_and_a_window_already_there_is_kept(
    scratch: Scratch, other_scratch: Scratch, monkeypatch: pytest.MonkeyPatch
) -> None:
    stopped, straight = factory_on(scratch[1]), factory_on(other_scratch[1])
    await with_window_over_next_week(stopped)
    await with_window_over_next_week(straight)

    # The first run stops at its first roster week, as the owner's `make up` did at a window.
    real_week = seed_wide_module._week

    async def stop(*_: object, **__: object) -> None:
        raise RuntimeError("stopped part-way")

    monkeypatch.setattr(seed_wide_module, "_week", stop)
    with pytest.raises(RuntimeError):
        await seed_wide(stopped, TODAY)
    monkeypatch.setattr(seed_wide_module, "_week", real_week)

    assert await seed_wide(stopped, TODAY) is False  # its records were there: it finished them
    assert await seed_wide(straight, TODAY) is True
    assert await digest(stopped) == await digest(straight)
    async with stopped() as db:
        windows = (await db.execute(text("SELECT count(*) FROM submission_windows"))).scalar_one()
        open_runs = (
            await db.execute(
                text("SELECT count(*) FROM schedule_jobs WHERE proposal_state = 'open'")
            )
        ).scalar_one()
    # The base's two, four past weeks and this one; next week keeps the developer's window.
    assert windows == 2 + 4 + 1 + 1 - 1
    assert open_runs == 1
