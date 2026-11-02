"""Async database engine and per-request session.

Layer: core. Depends on ``hds.core.config`` only. Only repositories may issue queries on the
session this module hands out (Constitution II); routers and services must not.
"""

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from hds.core.config import settings

engine = create_async_engine(settings.DATABASE_URL, echo=False)

AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Yield one database session per request and close it afterwards.

    The session does not commit on its own; the code that owns the unit of work (the
    service or repository method) commits explicitly. ``expire_on_commit`` is off so
    objects stay readable after the commit when the response is built.

    Yields:
        An ``AsyncSession`` bound to the shared engine.
    """
    async with AsyncSessionLocal() as session:
        yield session
