"""Idempotent full development seed (T115).

Run with:
    uv run python -m hds.scripts.seed_dev

Seeded accounts (password Dev1234! for all):
    admin@hds.test      role=admin
    manager@hds.test    role=manager
    employee@hds.test   role=employee
    extra@hds.test      role=employee  (extra employee pre-assigned to seeded Group)
    client@hds.test     role=client    (linked to seeded Client record)

Seeded entities:
    Organisation:  HDS Dev (Asia/Jerusalem)
    Client:        Seed Client → client@hds.test
    Site:          HQ Site (Tel Aviv)
    Group:         Night Shift
    Qualification: First Aid
    Project:       Night Watch (security, billing_rate=75.0000, client+site linked)

Requires migrations 001–010 to be applied before running against a real DB.
"""

import asyncio
import datetime
import decimal
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from hds.core.config import get_settings
from hds.core.security import hash_password
from hds.models.client import Client
from hds.models.group import Group, user_groups
from hds.models.organisation import Organisation
from hds.models.project import Project, ProjectType
from hds.models.qualification import Qualification
from hds.models.site import Site
from hds.models.user import User, UserRole


async def _get_or_create(
    session: AsyncSession,
    model: Any,
    filters: dict[str, Any],
    defaults: dict[str, Any],
) -> tuple[Any, bool]:
    """Return (obj, created). filters used for lookup; defaults merged on create."""
    stmt = select(model)
    for attr, val in filters.items():
        stmt = stmt.where(getattr(model, attr) == val)
    result = await session.execute(stmt)
    obj = result.scalar_one_or_none()
    if obj is not None:
        return obj, False
    obj = model(id=uuid.uuid4(), **{**filters, **defaults})
    session.add(obj)
    await session.flush()
    return obj, True


async def seed() -> None:
    """Create the development data set, skipping anything that already exists.

    Safe to run repeatedly: each record is looked up first. Writes to the database named by
    ``DATABASE_URL`` and commits once at the end, so a failure part-way leaves nothing behind.
    Never run it against production: the seeded accounts share a published password.

    Side effects:
        Inserts rows and prints one line per record to standard output.
    """
    settings = get_settings()
    engine = create_async_engine(settings.DATABASE_URL, echo=False)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async with session_factory() as db:
        # ── Organisation ─────────────────────────────────────────────────────
        org, created = await _get_or_create(
            db,
            Organisation,
            {"name": "HDS Dev"},
            {"timezone": "Asia/Jerusalem"},
        )
        print(f"Organisation {'created' if created else 'exists'}: {org.id}")

        # ── Client record ─────────────────────────────────────────────────────
        client_rec, created = await _get_or_create(
            db,
            Client,
            {"organization_id": org.id, "name": "Seed Client"},
            {"contract_start": datetime.date.today(), "is_active": True, "is_deleted": False},
        )
        print(f"Client {'created' if created else 'exists'}: {client_rec.id}")

        # ── Site ──────────────────────────────────────────────────────────────
        site, created = await _get_or_create(
            db,
            Site,
            {"organization_id": org.id, "name": "HQ Site"},
            {"address": "1 Rothschild Blvd, Tel Aviv", "timezone": "Asia/Jerusalem"},
        )
        print(f"Site {'created' if created else 'exists'}: {site.id}")

        # ── Internal users ────────────────────────────────────────────────────
        internal_users = [
            ("admin@hds.test", "Admin User", UserRole.admin, None),
            ("manager@hds.test", "Manager User", UserRole.manager, None),
            ("employee@hds.test", "Employee User", UserRole.employee, None),
            ("extra@hds.test", "Extra Employee", UserRole.employee, None),
        ]
        user_map: dict[str, User] = {}
        for email, full_name, role, client_id in internal_users:
            u, created = await _get_or_create(
                db,
                User,
                {"email": email, "organization_id": org.id},
                {
                    "password_hash": hash_password("Dev1234!"),
                    "full_name": full_name,
                    "role": role,
                    "client_id": client_id,
                    "is_active": True,
                    "is_deleted": False,
                    "activated_at": datetime.datetime.now(datetime.UTC),
                },
            )
            user_map[email] = u
            print(f"User {'created' if created else 'exists'}: {email}")

        # ── Client user ───────────────────────────────────────────────────────
        client_user, created = await _get_or_create(
            db,
            User,
            {"email": "client@hds.test", "organization_id": org.id},
            {
                "password_hash": hash_password("Dev1234!"),
                "full_name": "Client User",
                "role": UserRole.client,
                "client_id": client_rec.id,
                "is_active": True,
                "is_deleted": False,
                "activated_at": datetime.datetime.now(datetime.UTC),
            },
        )
        print(f"Client user {'created' if created else 'exists'}: {client_user.id}")

        # ── Group ─────────────────────────────────────────────────────────────
        group, created = await _get_or_create(
            db,
            Group,
            {"organization_id": org.id, "name": "Night Shift"},
            {},
        )
        print(f"Group {'created' if created else 'exists'}: {group.id}")

        # Assign extra@hds.test to group
        extra_user = user_map.get("extra@hds.test")
        if extra_user:
            existing_membership = await db.execute(
                select(user_groups).where(
                    user_groups.c.user_id == extra_user.id,
                    user_groups.c.group_id == group.id,
                )
            )
            if existing_membership.first() is None:
                await db.execute(
                    user_groups.insert().values(user_id=extra_user.id, group_id=group.id)
                )
                print(f"Assigned {extra_user.email} to group {group.name}")

        # ── Qualification ─────────────────────────────────────────────────────
        qual, created = await _get_or_create(
            db,
            Qualification,
            {"organization_id": org.id, "name": "First Aid"},
            {},
        )
        print(f"Qualification {'created' if created else 'exists'}: {qual.id}")

        # ── Project ───────────────────────────────────────────────────────────
        result = await db.execute(
            select(Project).where(
                Project.organization_id == org.id,
                Project.name == "Night Watch",
                Project.is_deleted.is_(False),
            )
        )
        project = result.scalar_one_or_none()
        if project is None:
            project = Project(
                id=uuid.uuid4(),
                organization_id=org.id,
                client_id=client_rec.id,
                site_id=site.id,
                name="Night Watch",
                type=ProjectType.security,
                billing_rate=decimal.Decimal("75.0000"),
                # A month back, so shifts that ended earlier today or yesterday fall inside it.
                start_date=datetime.date.today() - datetime.timedelta(days=30),
                is_deleted=False,
            )
            db.add(project)
            await db.flush()
            print(f"Project created: {project.id}")
        else:
            print(f"Project exists: {project.id}")

        await db.commit()
        print("\nSeed complete.")

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(seed())
