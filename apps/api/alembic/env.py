"""Alembic environment: connects migrations to the application's models and database.

Layer: tooling (outside the request path). The database URL comes from ``DATABASE_URL`` when
set, so CI and scratch databases need no edit to ``alembic.ini``. ``hds.models`` is imported
for its side effect of registering every table on ``Base.metadata``; ``alembic check``
compares that metadata with the live schema (Constitution X).
"""

import asyncio
import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy.ext.asyncio import create_async_engine

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Override sqlalchemy.url from environment variable if set
database_url = os.environ.get("DATABASE_URL")
if database_url:
    config.set_main_option("sqlalchemy.url", database_url)

# Import all models so Base.metadata includes every table
import hds.models  # noqa: F401, E402
from hds.models.base import Base  # noqa: E402

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Emit migration SQL without connecting (``alembic upgrade --sql``).

    Values are rendered as literals because there is no driver to bind parameters; use it to
    review or hand over the SQL, never to apply a migration.
    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection):  # type: ignore[no-untyped-def]
    """Run all pending migrations on a synchronous connection, in one transaction.

    Alembic's API is synchronous, so the async engine hands it a sync view of the connection
    through ``run_sync``.

    Args:
        connection: The synchronous connection provided by ``AsyncConnection.run_sync``.
    """
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """Connect with the async driver, run the migrations and close the engine.

    Side effects:
        Changes the schema of the database named by ``DATABASE_URL``.
    """
    url = config.get_main_option("sqlalchemy.url")
    connectable = create_async_engine(url)
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


def run_migrations_online() -> None:
    """Apply migrations to the live database (the normal ``alembic upgrade`` path)."""
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
