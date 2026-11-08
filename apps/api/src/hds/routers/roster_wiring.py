"""Builds the roster service on a request's session, for the routers that use it.

Layer: routers. Wiring only: it constructs ``RosterService`` and ``RosterToolsService`` with their
repositories and holds no logic, so the routers that use them share one place to build them.
"""

from sqlalchemy.ext.asyncio import AsyncSession

from hds.services.roster import RosterService
from hds.services.roster_tools import RosterToolsService
from hds.wiring import roster_service_for, roster_tools_for


def roster_service(db: AsyncSession) -> RosterService:
    """The roster service on the request's session (wired once, in ``hds.wiring``)."""
    return roster_service_for(db)


def roster_tools(db: AsyncSession) -> RosterToolsService:
    """The roster tools (overrides, publish review, copy week, templates, bulk, tracker)."""
    return roster_tools_for(db)
