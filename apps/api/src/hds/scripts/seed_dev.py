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
    Project:       Night Watch (security, billing_rate=75.0000, client+site linked),
                   managed by manager@hds.test
    Project:       Day Patrol (security, same client and site), managed by no one, so the
                   Manager manages one project only (007 T005)
    Windows:       last week's availability window (closed) and one four weeks ahead (open)
    Shifts:        this week published on Night Watch (a morning for two, an overnight shift)
                   and on Day Patrol; a draft week four weeks ahead on Night Watch

Weeks run Sunday to Saturday at the site (Asia/Jerusalem); the windows and shifts sit on whole
weeks, so a run later in the same week finds them and a run in a later week adds that week's own.
Nobody has submitted availability. Requires every migration to be applied first.
"""

import asyncio
import datetime
import decimal
import uuid
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from hds.core.config import get_settings
from hds.core.security import hash_password
from hds.models.client import Client
from hds.models.group import Group, user_groups
from hds.models.organisation import Organisation
from hds.models.project import Project, ProjectType
from hds.models.project_manager import ProjectManager
from hds.models.qualification import Qualification
from hds.models.shift import Assignment, Shift
from hds.models.shift_type import ShiftType, project_shift_types
from hds.models.site import Site
from hds.models.submission_window import SubmissionWindow
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


SITE_ZONE = ZoneInfo("Asia/Jerusalem")


def _sunday_of(day: datetime.date) -> datetime.date:
    """The Sunday that starts ``day``'s week (weeks run Sunday to Saturday)."""
    return day - datetime.timedelta(days=(day.weekday() + 1) % 7)


def _instants(day: datetime.date, kind: ShiftType) -> tuple[datetime.datetime, datetime.datetime]:
    """The UTC start and end of a ``kind`` shift on the site date ``day``."""
    starts = datetime.datetime.combine(day, kind.starts_at_local, SITE_ZONE)
    end_day = day + datetime.timedelta(days=1) if kind.crosses_midnight else day
    ends = datetime.datetime.combine(end_day, kind.ends_at_local, SITE_ZONE)
    return starts.astimezone(datetime.UTC), ends.astimezone(datetime.UTC)


async def _project(
    db: AsyncSession, org: Organisation, client: Client, site: Site, name: str
) -> Project:
    """The live project ``name``, created when missing (a month back, so recent shifts fit)."""
    found = await db.execute(
        select(Project).where(
            Project.organization_id == org.id,
            Project.name == name,
            Project.is_deleted.is_(False),
        )
    )
    project = found.scalar_one_or_none()
    if project is not None:
        print(f"Project exists: {name}")
        return project
    project = Project(
        id=uuid.uuid4(),
        organization_id=org.id,
        client_id=client.id,
        site_id=site.id,
        name=name,
        type=ProjectType.security,
        billing_rate=decimal.Decimal("75.0000"),
        start_date=datetime.date.today() - datetime.timedelta(days=30),
        is_deleted=False,
    )
    db.add(project)
    await db.flush()
    print(f"Project created: {name}")
    return project


async def _shift(
    db: AsyncSession,
    project: Project,
    kind: ShiftType,
    day: datetime.date,
    headcount: int,
    people: list[User],
    *,
    published: bool,
) -> None:
    """One shift of ``kind`` on ``day`` with ``people`` on it, unless it already exists."""
    starts_at, ends_at = _instants(day, kind)
    found = await db.execute(
        select(Shift.id).where(
            Shift.project_id == project.id,
            Shift.shift_type_id == kind.id,
            Shift.starts_at == starts_at,
        )
    )
    if found.first() is not None:
        return
    shift = Shift(
        id=uuid.uuid4(),
        organization_id=project.organization_id,
        project_id=project.id,
        shift_type_id=kind.id,
        starts_at=starts_at,
        ends_at=ends_at,
        headcount=headcount,
        status="published" if published else "draft",
        published_at=datetime.datetime.now(datetime.UTC) if published else None,
    )
    db.add(shift)
    await db.flush()
    for person in people:
        db.add(
            Assignment(
                id=uuid.uuid4(),
                organization_id=project.organization_id,
                shift_id=shift.id,
                user_id=person.id,
                source="manual",
            )
        )
    await db.flush()
    state = "published" if published else "draft"
    print(f"Shift created: {project.name} {kind.name} {day} ({state})")


async def _window(
    db: AsyncSession, org: Organisation, first: datetime.date, deadline: datetime.datetime
) -> None:
    """The availability window for the week starting on Sunday ``first``, created when missing."""
    window, created = await _get_or_create(
        db,
        SubmissionWindow,
        {"organization_id": org.id, "starts_on": first, "ends_on": first + datetime.timedelta(6)},
        {"deadline": deadline},
    )
    print(f"Window {'created' if created else 'exists'}: {window.starts_on} to {window.ends_on}")


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

        # ── Projects: the Manager manages Night Watch only (007 T005) ────────
        project = await _project(db, org, client_rec, site, "Night Watch")
        other = await _project(db, org, client_rec, site, "Day Patrol")
        manager = user_map["manager@hds.test"]
        managing = await db.execute(
            select(ProjectManager).where(
                ProjectManager.project_id == project.id, ProjectManager.user_id == manager.id
            )
        )
        if managing.scalar_one_or_none() is None:
            # The key is the pair, so no id (the generic helper would add one).
            db.add(
                ProjectManager(organization_id=org.id, project_id=project.id, user_id=manager.id)
            )
            await db.flush()
            print(f"Manager on project: {manager.email} on {project.name}")

        # The organisation's shift types come from the database trigger (migration 020).
        kinds = {
            k.name: k
            for k in (
                await db.execute(
                    select(ShiftType).where(
                        ShiftType.organization_id == org.id, ShiftType.is_active.is_(True)
                    )
                )
            ).scalars()
        }
        for linked in (project, other):
            have = {
                row.shift_type_id
                for row in await db.execute(
                    select(project_shift_types.c.shift_type_id).where(
                        project_shift_types.c.project_id == linked.id
                    )
                )
            }
            for kind in kinds.values():
                if kind.id not in have:
                    await db.execute(
                        project_shift_types.insert().values(
                            project_id=linked.id, shift_type_id=kind.id
                        )
                    )

        # ── Availability windows: last week closed, four weeks ahead open ────
        this_week = _sunday_of(datetime.datetime.now(SITE_ZONE).date())
        last_week = this_week - datetime.timedelta(days=7)
        ahead = this_week + datetime.timedelta(days=28)
        await _window(
            db, org, last_week, datetime.datetime.combine(last_week, datetime.time(), SITE_ZONE)
        )
        # Its deadline is the day before it starts, so it is always still open.
        eve = ahead - datetime.timedelta(days=1)
        await _window(db, org, ahead, datetime.datetime.combine(eve, datetime.time(), SITE_ZONE))

        # ── Shifts: this week published, a draft week four weeks ahead ───────
        employee, extra = user_map["employee@hds.test"], user_map["extra@hds.test"]
        if {"morning", "afternoon", "night"} <= kinds.keys():

            def day(offset: int) -> datetime.date:
                """The site date ``offset`` days after this week's Sunday."""
                return this_week + datetime.timedelta(days=offset)

            await _shift(
                db, project, kinds["morning"], day(1), 2, [employee, extra], published=True
            )
            await _shift(db, project, kinds["night"], day(2), 1, [employee], published=True)
            await _shift(db, other, kinds["afternoon"], day(4), 1, [extra], published=True)
            await _shift(db, project, kinds["morning"], day(29), 2, [], published=False)
            await _shift(db, project, kinds["night"], day(31), 1, [extra], published=False)
        else:
            print("Shift types missing: no shifts seeded")

        await db.commit()
        print("\nSeed complete.")

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(seed())
