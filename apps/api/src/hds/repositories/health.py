"""Health repository: the two database reads the ready check needs.

Layer: repositories. Depends on SQLAlchemy and ``hds.core`` only; it holds no rule of its own (the
"behind" rule is in ``hds.core.health``).
"""

from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.asyncio import AsyncSession

# PostgreSQL's SQLSTATE for a table that does not exist.
_UNDEFINED_TABLE = "42P01"


class HealthRepository:
    """Reads that tell whether the database answers and which migration it holds."""

    def __init__(self, session: AsyncSession) -> None:
        """Wrap ``session``; the repository never writes through it."""
        self._session = session

    async def ping(self) -> None:
        """Run ``SELECT 1``.

        Raises:
            Exception: Whatever the driver raises when the database cannot be reached.
        """
        await self._session.execute(text("SELECT 1"))

    async def revisions(self) -> list[str]:
        """The rows of ``alembic_version``: empty when the table is empty or does not exist."""
        try:
            found = await self._session.execute(text("SELECT version_num FROM alembic_version"))
        except ProgrammingError as error:
            if getattr(error.orig, "sqlstate", None) != _UNDEFINED_TABLE:
                raise  # a permission or other fault is a database failure, not "behind"
            # No table yet: a database that was never migrated is behind every release.
            await self._session.rollback()
            return []
        return [str(row) for row in found.scalars()]
