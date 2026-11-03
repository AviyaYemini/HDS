"""Shared integration fixtures: a second organisation holding one of each resource.

Covers: none directly; fixtures for the cross-tenant tests (NFR-001).
"""

import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest_asyncio
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.security import hash_password
from hds.models.availability import AvailabilityEntry
from hds.models.client import Client
from hds.models.group import Group
from hds.models.organisation import Organisation
from hds.models.project import Project, ProjectStatus, ProjectType
from hds.models.qualification import Qualification
from hds.models.shift import Assignment, Shift
from hds.models.shift_type import ShiftType, project_shift_types
from hds.models.site import Site
from hds.models.submission_window import SubmissionWindow
from hds.models.user import User, UserRole


@dataclass
class OtherOrg:
    org: Organisation
    user: User
    client: Client
    site: Site
    project: Project
    group: Group
    qualification: Qualification
    window: SubmissionWindow | None = None
    entry: AvailabilityEntry | None = None
    shift_type: ShiftType | None = None
    shift: Shift | None = None
    assignment: Assignment | None = None


@pytest_asyncio.fixture
async def other_org(test_db: AsyncSession, seed_users: dict[str, User]) -> OtherOrg:
    """A second organisation with one of each resource (the caller is in ``seed_org``)."""
    org = Organisation(id=uuid.uuid4(), name="Other Org", timezone="UTC")
    test_db.add(org)
    await test_db.flush()
    user = User(
        id=uuid.uuid4(),
        organization_id=org.id,
        email="employee@other.test",
        password_hash=hash_password("Dev1234!"),
        full_name="Other Employee",
        role=UserRole.employee,
        activated_at=datetime.now(UTC),
    )
    client = Client(
        id=uuid.uuid4(), organization_id=org.id, name="Other Client", contract_start=date.today()
    )
    site = Site(
        id=uuid.uuid4(), organization_id=org.id, name="Other Site", address="x", timezone="UTC"
    )
    group = Group(id=uuid.uuid4(), organization_id=org.id, name="Other Group")
    qualification = Qualification(id=uuid.uuid4(), organization_id=org.id, name="Other Qual")
    test_db.add_all([user, client, site, group, qualification])
    await test_db.flush()
    project = Project(
        id=uuid.uuid4(),
        organization_id=org.id,
        client_id=client.id,
        site_id=site.id,
        name="Other Project",
        type=ProjectType.security,
        billing_rate=Decimal("10.0000"),
        start_date=date.today(),
        status=ProjectStatus.active,
    )
    window = SubmissionWindow(
        id=uuid.uuid4(),
        organization_id=org.id,
        starts_on=date(2027, 6, 6),
        ends_on=date(2027, 6, 12),
        deadline=datetime.now(UTC),
    )
    entry = AvailabilityEntry(
        id=uuid.uuid4(),
        organization_id=org.id,
        user_id=user.id,
        kind="blocked",
        starts_on=date(2027, 6, 6),
        ends_on=date(2027, 6, 6),
    )
    test_db.add_all([project, window, entry])
    await test_db.flush()
    # The organisation's default shift types come from the database trigger. Read before the
    # commit: a query after it would open a transaction whose rollback expires these objects.
    shift_type = (
        (await test_db.execute(select(ShiftType).where(ShiftType.organization_id == org.id)))
        .scalars()
        .first()
    )
    assert shift_type is not None
    await test_db.execute(
        insert(project_shift_types).values(project_id=project.id, shift_type_id=shift_type.id)
    )
    shift = Shift(
        id=uuid.uuid4(),
        organization_id=org.id,
        project_id=project.id,
        shift_type_id=shift_type.id,
        starts_at=datetime(2027, 6, 7, 4, tzinfo=UTC),
        ends_at=datetime(2027, 6, 7, 12, tzinfo=UTC),
        headcount=1,
    )
    test_db.add(shift)
    await test_db.flush()
    assignment = Assignment(
        id=uuid.uuid4(), organization_id=org.id, shift_id=shift.id, user_id=user.id
    )
    test_db.add(assignment)
    await test_db.commit()
    return OtherOrg(
        org,
        user,
        client,
        site,
        project,
        group,
        qualification,
        window,
        entry,
        shift_type,
        shift,
        assignment,
    )
