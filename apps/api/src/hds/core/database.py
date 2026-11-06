"""Async database engine, per-request session and the unit of work.

Layer: core. Depends on ``hds.core.config`` only. Only repositories may issue queries on the
session this module hands out (Constitution II); routers and services must not.

One request is one transaction (specs/002-security-hardening/research.md D1). Repositories only
``flush``; the unit of work commits once when the handler returns, and rolls back on any error.
Work that must happen only if the change was saved (sending email) is queued with
``after_commit``. In a request it runs after the response has been sent (docs/adr/008), and it never
runs if the commit failed.
"""

import logging
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

from fastapi import BackgroundTasks, Depends
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import ORMExecuteState, Session, SessionTransaction
from sqlalchemy.pool import NullPool

from hds.core.config import settings

logger = logging.getLogger("hds.database")

engine = create_async_engine(settings.DATABASE_URL, echo=False)

AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)

# The ready checks' own engine, with no pool: a check opens one connection and closes it. Sharing
# the request pool would make every pod report the database down together whenever that pool is
# busy, turning a slow spell into an outage (specs/006 research R6). The URL is the same database.
_health_engine = create_async_engine(settings.DATABASE_URL, echo=False, poolclass=NullPool)
HealthSessionLocal = async_sessionmaker(_health_engine, expire_on_commit=False)

#: Key under which the queue of after-commit actions is stored in ``session.info``.
_AFTER_COMMIT = "hds.after_commit"
TRANSACTION_WROTE = "hds.transaction_wrote"


@event.listens_for(Session, "do_orm_execute")
def _note_statement(state: ORMExecuteState) -> None:
    """Record a write or a row lock, for ``BaseRepository.release_connection``.

    This sees direct ``UPDATE``, ``INSERT`` and ``DELETE`` statements, which ``session.new`` and
    ``session.dirty`` do not.
    """
    locks = state.is_select and getattr(state.statement, "_for_update_arg", None) is not None
    if state.is_insert or state.is_update or state.is_delete or locks:
        state.session.info[TRANSACTION_WROTE] = True


@event.listens_for(Session, "after_flush")
def _note_flush(session: Session, flush_context: object) -> None:
    """Record ORM changes written by a flush."""
    session.info[TRANSACTION_WROTE] = True


@event.listens_for(Session, "after_transaction_end")
def _forget_writes(session: Session, transaction: SessionTransaction) -> None:
    """Clear the record when the outermost transaction commits or rolls back."""
    if transaction.parent is None:
        session.info.pop(TRANSACTION_WROTE, None)


AfterCommitAction = Callable[[], Awaitable[None]]


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """Return the factory for work that outlives the request's session.

    A dependency, so tests can point background jobs at their own database.

    Returns:
        The shared session factory.
    """
    return AsyncSessionLocal


def get_health_session_factory() -> async_sessionmaker[AsyncSession]:
    """Return the factory the ready checks use: the same database, never the request pool.

    A dependency, so tests can point the checks at their own database.

    Returns:
        The health checks' session factory.
    """
    return HealthSessionLocal


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Yield one database session per request and close it afterwards.

    The session does not commit on its own. Routers do not use this directly: they depend on
    ``get_unit_of_work``, which wraps this session in a transaction. ``expire_on_commit`` is off
    so objects stay readable after the commit when the response is built. Tests override this
    dependency to hand out their own sessions, and ``get_unit_of_work`` keeps working on top.

    Yields:
        An ``AsyncSession`` bound to the shared engine.
    """
    async with AsyncSessionLocal() as session:
        yield session


def after_commit(session: AsyncSession, action: AfterCommitAction) -> None:
    """Queue ``action`` to run after the current unit of work has committed.

    The queue lives on the session, so it is dropped with the transaction if that rolls back.

    Args:
        session: The session whose unit of work the action belongs to.
        action: A coroutine function taking no arguments, for example one that sends an email.
    """
    session.info.setdefault(_AFTER_COMMIT, []).append(action)


async def _run_actions(actions: list[AfterCommitAction]) -> None:
    """Run after-commit actions in order; a failure is logged and stops nothing.

    Only the exception type is logged, never its message or traceback, which could carry a secret.

    Args:
        actions: The actions queued by ``after_commit``.
    """
    for action in actions:
        try:
            await action()
        except Exception as exc:
            logger.error(
                "after-commit action %s failed: %s",
                getattr(action, "__qualname__", "action"),
                type(exc).__name__,
            )


@asynccontextmanager
async def unit_of_work(
    session: AsyncSession, background: BackgroundTasks | None = None
) -> AsyncIterator[AsyncSession]:
    """Run a block as one transaction: commit once on success, roll back on any error.

    After a successful commit the queued ``after_commit`` actions run in order, at once or, given
    ``background``, as one task after the response. A failing action does not undo the saved
    change or stop the remaining actions. If the block or the commit raises, the queue is
    discarded, the transaction is rolled back and the error is re-raised unchanged. A rollback that
    fails too is logged by exception type only and does not replace that error (FR-233). A
    cancellation during the rollback is not a failed rollback: it goes through, because swallowing
    one breaks the caller's cancellation. Cancellation counts as an error and also rolls back.

    Args:
        session: The session to commit or roll back.
        background: The request's background tasks, when there is a request.

    Yields:
        The same session, so callers can write ``async with unit_of_work(db) as session``.

    Raises:
        BaseException: Whatever the block or the commit raised, after the rollback.
    """
    try:
        yield session
        await session.commit()
    except BaseException:
        session.info.pop(_AFTER_COMMIT, None)
        try:
            await session.rollback()
        except Exception as rollback_error:  # the original error is the one that matters
            logger.error("rollback failed: %s", type(rollback_error).__name__)
        raise
    actions = session.info.pop(_AFTER_COMMIT, [])
    if actions and background is not None:
        background.add_task(_run_actions, actions)
    elif actions:
        await _run_actions(actions)


async def get_unit_of_work(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
) -> AsyncGenerator[AsyncSession, None]:
    """Provide the request's session inside a unit of work.

    Declare it as ``Depends(get_unit_of_work, scope="function")`` so the commit happens when the
    handler returns and before the response is sent: a failed commit then becomes an error
    response instead of a success that was not saved. After-commit actions run after the response
    has been sent, so a slow mail provider cannot hold a request open or show which addresses are
    registered (FR-228).

    Args:
        background_tasks: FastAPI's background tasks for this request.
        db: The session from ``get_db`` (or a test override of it).

    Yields:
        The session for the handler to pass to repositories.
    """
    async with unit_of_work(db, background_tasks):
        yield db
