"""The capability map: what each role may do, one table shared by the API and the UI (007 FR-001).

Layer: core. Depends on ``hds.models.user`` only; it MUST NOT import repositories, services or
routers, and it does no I/O. ``GET /me`` serves a user's list from here, the route gates in
``hds.core.deps`` ask it, and services ask it for project-scoped checks, so the navigation the
client draws and the refusals the API gives can never disagree (research R4).

Names follow ``specs/007-ux-redesign/contracts/capabilities.md``; a unit test reads that table and
compares. A name ending in ``:project`` is scoped: an Admin holds it for every project, a Manager
only for the projects they manage (``project_managers``). The roles follow ``docs/hds-spec.md``
section 2: a Manager inherits every Employee capability, and only an Admin deletes, edits pay on a
shift or changes admin-only settings.
"""

import uuid
from collections.abc import Collection
from enum import StrEnum

from hds.models.user import UserRole


class Capability(StrEnum):
    """One thing a signed-in person may do."""

    MY_WORK_READ = "my_work.read"
    MY_WORK_AVAILABILITY = "my_work.availability"
    ROSTER_READ = "roster.read"
    ROSTER_EDIT_PROJECT = "roster.edit:project"
    ROSTER_PUBLISH_PROJECT = "roster.publish:project"
    ROSTER_OVERRIDE_PROJECT = "roster.override:project"
    SCHEDULE_GENERATE = "schedule.generate"
    AVAILABILITY_READ_OTHERS = "availability.read_others"
    AVAILABILITY_EDIT_OTHERS = "availability.edit_others"
    REPORTS_READ = "reports.read"
    PAY_READ_OTHERS = "pay.read_others"
    PAY_EDIT_SHIFT = "pay.edit_shift"
    PAY_SETTINGS_EDIT = "pay_settings.edit"
    STAFF_MANAGE = "staff.manage"
    CLIENTS_MANAGE = "clients.manage"
    SITES_MANAGE = "sites.manage"
    PROJECTS_MANAGE = "projects.manage"
    PROJECTS_READ = "projects.read"
    STAFF_DELETE = "staff.delete"
    CLIENTS_DELETE = "clients.delete"
    PROJECTS_DELETE = "projects.delete"
    SPECIAL_DAYS_EDIT = "special_days.edit"
    ORGANISATION_CONTACT_EDIT = "organisation.contact.edit"
    CLIENT_PORTAL_READ = "client_portal.read"
    CLIENT_PORTAL_REQUESTS = "client_portal.requests"
    CLIENT_REQUESTS_ANSWER = "client_requests.answer"
    ACCOUNT_SELF = "account.self"


C = Capability

_EMPLOYEE = frozenset({C.MY_WORK_READ, C.MY_WORK_AVAILABILITY, C.ACCOUNT_SELF})

_MANAGER = _EMPLOYEE | frozenset(
    {
        C.ROSTER_READ,
        C.ROSTER_EDIT_PROJECT,
        C.ROSTER_PUBLISH_PROJECT,
        C.ROSTER_OVERRIDE_PROJECT,
        C.SCHEDULE_GENERATE,
        C.AVAILABILITY_READ_OTHERS,
        C.REPORTS_READ,
        # Scoped further by 005's pay visibility (``hds.services.pay_visibility``).
        C.PAY_READ_OTHERS,
        C.STAFF_MANAGE,
        C.CLIENTS_MANAGE,
        C.SITES_MANAGE,
        C.PROJECTS_MANAGE,
        C.PROJECTS_READ,
        C.CLIENT_REQUESTS_ANSWER,
    }
)

_ADMIN = _MANAGER | frozenset(
    {
        C.AVAILABILITY_EDIT_OTHERS,
        C.PAY_EDIT_SHIFT,
        C.PAY_SETTINGS_EDIT,
        C.STAFF_DELETE,
        C.CLIENTS_DELETE,
        C.PROJECTS_DELETE,
        C.SPECIAL_DAYS_EDIT,
        C.ORGANISATION_CONTACT_EDIT,
    }
)

# A client reads only its own projects; the project service narrows the rows (CLI-1).
_CLIENT = frozenset(
    {C.CLIENT_PORTAL_READ, C.CLIENT_PORTAL_REQUESTS, C.PROJECTS_READ, C.ACCOUNT_SELF}
)

ROLE_CAPABILITIES: dict[UserRole, frozenset[Capability]] = {
    UserRole.employee: _EMPLOYEE,
    UserRole.manager: _MANAGER,
    UserRole.admin: _ADMIN,
    UserRole.client: _CLIENT,
}


def capabilities_for(role: UserRole) -> list[str]:
    """The capability names a role holds, sorted, as ``GET /me`` serves them.

    Args:
        role: The user's current role, read from their row on this request.

    Returns:
        The names in ``Capability``, sorted alphabetically so the answer is stable.
    """
    return sorted(c.value for c in ROLE_CAPABILITIES[role])


def has(role: UserRole, capability: Capability) -> bool:
    """Whether ``role`` holds ``capability`` at all (for a scoped one, on some project).

    Args:
        role: The user's current role.
        capability: The capability asked about.

    Returns:
        ``True`` when the role's set contains it.
    """
    return capability in ROLE_CAPABILITIES[role]


def has_on_project(
    role: UserRole,
    capability: Capability,
    project_id: uuid.UUID,
    managed_project_ids: Collection[uuid.UUID],
) -> bool:
    """Whether ``role`` holds a ``:project`` capability on one project.

    An Admin holds a scoped capability on every project; a Manager only on the projects they
    manage; any other role on none. An unscoped capability falls back to :func:`has`.

    Args:
        role: The user's current role.
        capability: The capability asked about.
        project_id: The project the action touches.
        managed_project_ids: The projects the user manages (``project_managers``).

    Returns:
        ``True`` when the action is allowed on that project.
    """
    if not has(role, capability):
        return False
    if not capability.value.endswith(":project") or role == UserRole.admin:
        return True
    return project_id in managed_project_ids
