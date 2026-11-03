"""Shared integration fixtures: a second organisation holding one of each resource.

Covers: none directly; fixtures for the cross-tenant tests (NFR-001).
"""

import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.security import hash_password
from hds.models.client import Client
from hds.models.group import Group
from hds.models.organisation import Organisation
from hds.models.project import Project, ProjectStatus, ProjectType
from hds.models.qualification import Qualification
from hds.models.site import Site
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
    test_db.add(project)
    await test_db.commit()
    return OtherOrg(org, user, client, site, project, group, qualification)
