"""The first Admin of an empty database, with its organisation (specs/006 research R13).

Layer: services. Depends on ``hds.core``, ``hds.models``, the repositories it is given, and the
employee and site rules it reuses; it MUST NOT import routers or know about HTTP.

The shared environment has no seed: its operator runs ``hds.scripts.create_admin`` once. That makes
the organisation and one active Admin, and only while the database holds no user at all, so the
command can never add an Admin to a running system. The password follows the same rule as every
other initial password (at least 12 characters), and the time zone the same rule as a site's.
"""

from hds.core import passwords
from hds.core.exceptions import ConflictError, DomainValidationError
from hds.models.user import User, UserRole
from hds.repositories.organisation import OrganisationRepository
from hds.repositories.user import UserRepository
from hds.services.employee import validate_initial_password
from hds.services.site import validate_timezone

#: The longest name ``users.full_name`` holds; checked first, so no insert fails on it (review R19).
MAX_NAME_LENGTH = 255


class FirstAdminService:
    """Creates the organisation and its first Admin, once."""

    def __init__(
        self, user_repo: UserRepository, organisation_repo: OrganisationRepository
    ) -> None:
        """Wire the service.

        Args:
            user_repo: Users: the check that none exists, and the new Admin.
            organisation_repo: Organisations: the new one.
        """
        self._users = user_repo
        self._orgs = organisation_repo

    async def create(
        self, *, organisation: str, timezone: str, email: str, full_name: str, password: str
    ) -> User:
        """Create the organisation and its first Admin, active, in the caller's transaction.

        Args:
            organisation: The organisation's name.
            timezone: Its IANA time zone.
            email: The Admin's address, already validated and lower-cased.
            full_name: The Admin's name.
            password: The Admin's initial password, in clear; only its hash is stored.

        Returns:
            The new Admin.

        Raises:
            DomainValidationError: ``organisations.name_required``, ``users.full_name_invalid``,
                ``sites.timezone_invalid`` or ``users.password_too_short``.
            ConflictError: ``users.first_admin_exists`` when any user exists.
        """
        name = organisation.strip()
        if not name:
            raise DomainValidationError(
                code="organisations.name_required", detail="The organisation needs a name."
            )
        admin_name = full_name.strip()
        if not admin_name or len(admin_name) > MAX_NAME_LENGTH:
            raise DomainValidationError(
                code="users.full_name_invalid",
                detail=f"The name must be 1 to {MAX_NAME_LENGTH} characters.",
            )
        validate_timezone(timezone)
        validate_initial_password(password)
        # Hashed before the lock and the first query, and with the connection released, so none
        # is held while it runs (002 FR-222).
        await self._users.release_connection()
        hashed = await passwords.hash_password(password)
        await self._users.lock_first_admin()
        if await self._users.any_exists():
            raise ConflictError(
                code="users.first_admin_exists",
                detail="Users already exist; the first Admin is created only in an empty database.",
            )
        org = await self._orgs.create(name, timezone)
        return await self._users.create(
            org.id,
            {
                "email": email,
                "full_name": admin_name,
                "role": UserRole.admin,
                "password_hash": hashed,
            },
        )
