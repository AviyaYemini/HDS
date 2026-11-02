"""Async database engine, per-request session and the unit of work.

Layer: core. Depends on ``hds.core.config`` only. Only repositories may issue queries on the
session this module hands out (Constitution II); routers and services must not.

One request is one transaction (specs/002-security-hardening/research.md D1). Repositories only
``flush``; the unit of work commits once when the handler returns, and rolls back on any error.
Work that must happen only if the change was saved (sending email) is queued with
``after_commit`` and runs after a successful commit.
"""

import logging
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from hds.core.config import settings

logger = logging.getLogger("hds.database")

engine = create_async_engine(settings.DATABASE_URL, echo=False)

AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)

#: Key under which the queue of after-commit actions is stored in ``session.info``.
_AFTER_COMMIT = "hds.after_commit"

AfterCommitAction = Callable[[], Awaitable[None]]


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


@asynccontextmanager
async def unit_of_work(session: AsyncSession) -> AsyncIterator[AsyncSession]:
    """Run a block as one transaction: commit once on success, roll back on any error.

    After a successful commit the queued ``after_commit`` actions run in order. An action that
    fails is logged (its exception type only, never its message or traceback, which could carry a
    secret) and does not undo the saved change or stop the remaining actions. If the block or the
    commit raises, the queue is discarded, the transaction is rolled back and the error is
    re-raised unchanged. Cancellation counts as an error and also rolls back.

    Args:
        session: The session to commit or roll back.

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
        await session.rollback()
        raise
    for action in session.info.pop(_AFTER_COMMIT, []):
        try:
            await action()
        except Exception as exc:
            logger.error(
                "after-commit action %s failed: %s",
                getattr(action, "__qualname__", "action"),
                type(exc).__name__,
            )


async def get_unit_of_work(
    db: AsyncSession = Depends(get_db),
) -> AsyncGenerator[AsyncSession, None]:
    """Provide the request's session inside a unit of work.

    Declare it as ``Depends(get_unit_of_work, scope="function")`` so the commit happens when the
    handler returns and before the response is sent: a failed commit then becomes an error
    response instead of a success that was not saved.

    Args:
        db: The session from ``get_db`` (or a test override of it).

    Yields:
        The session for the handler to pass to repositories.
    """
    async with unit_of_work(db):
        yield db
