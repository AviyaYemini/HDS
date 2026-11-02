"""SQLAlchemy models, imported together so ``Base.metadata`` lists every table.

Layer: models (the lowest layer). Models describe tables, constraints and relationships only;
they MUST NOT import repositories, services, routers or schemas, and hold no business logic.
Alembic's autogenerate and ``alembic check`` read ``Base.metadata`` through this package, so a
new model must be imported here or its table is invisible to migrations.
"""

from hds.models.base import Base, TimestampedBase
from hds.models.client import Client
from hds.models.group import Group, user_groups
from hds.models.invitation_token import InvitationToken
from hds.models.login_throttle import LoginThrottle
from hds.models.organisation import Organisation
from hds.models.project import Project, ProjectStatus, ProjectType
from hds.models.qualification import Qualification, user_qualifications
from hds.models.refresh_token import RefreshToken
from hds.models.site import Site
from hds.models.user import User, UserRole

__all__ = [
    "Base",
    "TimestampedBase",
    "Organisation",
    "Client",
    "Site",
    "User",
    "UserRole",
    "RefreshToken",
    "LoginThrottle",
    "Group",
    "user_groups",
    "Qualification",
    "user_qualifications",
    "Project",
    "ProjectType",
    "ProjectStatus",
    "InvitationToken",
]
