# Wire all model imports so Base.metadata contains every table for Alembic
from hds.models.base import Base, TimestampedBase
from hds.models.client import Client
from hds.models.group import Group, user_groups
from hds.models.invitation_token import InvitationToken
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
    "Group",
    "user_groups",
    "Qualification",
    "user_qualifications",
    "Project",
    "ProjectType",
    "ProjectStatus",
    "InvitationToken",
]
