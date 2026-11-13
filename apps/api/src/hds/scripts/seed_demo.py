"""Load the CEO demo's data into an empty database (CEO demo, D03).

Run with:
    uv run python -m hds.scripts.seed_demo ../../demo-data/roster.json

The file comes from ``scripts/demo/parse_roster.py``. It holds real names and pay, so it lives only
in the git-ignored ``demo-data/`` folder, and so does every database this fills. It refuses a
database that already has an organisation: start from a freshly migrated one.

Sign-ins, all with the development seed's password (or ``DEMO_PASSWORD``), local use only:
    admin@hds.test     Admin (the CEO's view)
    manager<n>@hds.test  one Manager per task manager in the sheet, on their projects

Weeks up to the current one are published; later weeks stay drafts, ready to publish.
"""

import asyncio
import datetime as dt
import json
import os
import sys
import uuid
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from hds.core.config import get_settings
from hds.core.security import hash_password
from hds.models.client import Client
from hds.models.organisation import Organisation
from hds.models.project import Project, ProjectStatus, ProjectType
from hds.models.project_manager import ProjectManager
from hds.models.project_member import ProjectMember
from hds.models.shift import Assignment, Shift
from hds.models.shift_type import ShiftType, project_shift_types
from hds.models.site import Site
from hds.models.staffing_need import StaffingNeed
from hds.models.submission_window import SubmissionWindow
from hds.models.user import User, UserRole

ZONE_NAME = "Asia/Jerusalem"
ZONE = ZoneInfo(ZONE_NAME)
FOUR = Decimal("0.0001")


def _time(text: str) -> dt.time:
    return dt.time(int(text[:2]), int(text[3:5]))


def _hours(start: str, end: str) -> Decimal:
    a, b = _time(start), _time(end)
    minutes = (b.hour * 60 + b.minute - a.hour * 60 - a.minute) % (24 * 60) or 24 * 60
    return Decimal(minutes) / 60


def _hourly(amount: Any, unit: str, window: list[str]) -> Decimal:
    """A rate per hour: as given, or a shift's sum spread over its hours."""
    value = Decimal(str(amount))
    if unit == "shift":
        value = value / _hours(*window)
    return value.quantize(FOUR, ROUND_HALF_UP)


def _label(start: str, end: str) -> str:
    """A shift type's name from its window: night, morning or evening, and the hours."""
    a = _time(start).hour
    kind = "לילה" if a >= 20 or a < 4 else "בוקר" if a < 12 else "ערב"
    return f"{kind} {start}–{end}"


def _span(day: dt.date, start: str, end: str) -> tuple[dt.datetime, dt.datetime]:
    a, b = _time(start), _time(end)
    starts = dt.datetime.combine(day, a, ZONE)
    ends = dt.datetime.combine(day + dt.timedelta(days=1) if b <= a else day, b, ZONE)
    return starts.astimezone(dt.UTC), ends.astimezone(dt.UTC)


def _own_span(
    day: dt.date,
    shift_start: dt.datetime,
    shift_end: dt.datetime,
    start: str | None,
    end: str | None,
) -> tuple[dt.datetime, dt.datetime]:
    """A person's own times on a shift, each on the shift's own night.

    02:00 on a 20:00-06:00 shift is the next morning; a start with no end runs to the shift's
    end, and an end with no start begins at the shift's start.
    """

    def near(clock: str) -> dt.datetime:
        # The day or the next, whichever puts the time nearest the shift.
        def gap(moment: dt.datetime) -> dt.timedelta:
            return max(shift_start - moment, moment - shift_end, dt.timedelta(0))

        options = [
            dt.datetime.combine(day + dt.timedelta(days=d), _time(clock), ZONE).astimezone(dt.UTC)
            for d in (0, 1)
        ]
        return min(options, key=gap)

    own_start = near(start) if start else shift_start
    own_end = near(end) if end else shift_end
    if own_end <= own_start:
        own_end += dt.timedelta(days=1)
    return own_start, own_end


def _sunday(day: dt.date) -> dt.date:
    return day - dt.timedelta(days=(day.weekday() + 1) % 7)


async def load(db: AsyncSession, data: dict[str, Any], today: dt.date) -> dict[str, int]:
    """Write ``data`` into ``db``; returns how many of each thing it wrote."""
    if (await db.execute(select(Organisation.id))).first() is not None:
        raise SystemExit("seed_demo: the database already has an organisation; use a fresh one.")
    now = dt.datetime.now(dt.UTC)
    first_day = dt.date.fromisoformat(data["weeks"][0])
    org = Organisation(id=uuid.uuid4(), name="HDS", timezone=ZONE_NAME)
    db.add(org)
    await db.flush()
    # The database's default shift types (migration 020) give way to the sheet's own windows.
    await db.execute(
        update(ShiftType).where(ShiftType.organisation_id == org.id).values(is_active=False)
    )

    def account(email: str, name: str, role: UserRole, **extra: Any) -> User:
        """Add an active account with the demo's password."""
        user = User(
            id=uuid.uuid4(),
            organisation_id=org.id,
            email=email,
            password_hash=password_hash,
            full_name=name,
            role=role,
            is_active=True,
            activated_at=now,
            **extra,
        )
        db.add(user)
        return user

    # The development seed's published password unless DEMO_PASSWORD names another.
    password_hash = hash_password(os.environ.get("DEMO_PASSWORD") or "Dev1234!")
    account("admin@hds.test", "מנהל מערכת", UserRole.admin)
    managers = {
        name: account(f"manager{i}@hds.test", name, UserRole.manager)
        for i, name in enumerate(data["managers"], start=1)
    }
    clients = {
        name: Client(id=uuid.uuid4(), organisation_id=org.id, name=name, contract_start=first_day)
        for name in data["clients"]
    }
    db.add_all(clients.values())
    await db.flush()

    types: dict[tuple[str, str], ShiftType] = {}
    for p in data["projects"]:
        for start, end in p["windows"]:
            if (start, end) not in types:
                types[(start, end)] = ShiftType(
                    id=uuid.uuid4(),
                    organisation_id=org.id,
                    name=_label(start, end),
                    starts_at_local=_time(start),
                    ends_at_local=_time(end),
                    crosses_midnight=_time(end) <= _time(start),
                    is_active=True,
                )
    db.add_all(types.values())

    sites: dict[str, Site] = {}
    projects: dict[str, Project] = {}
    pay_of: dict[str, Decimal] = {}
    for p in data["projects"]:
        site = sites.get(p["site"])
        if site is None:
            site = sites[p["site"]] = Site(
                id=uuid.uuid4(),
                organisation_id=org.id,
                name=p["site"],
                address=p["site"],
                timezone=ZONE_NAME,
            )
            db.add(site)
        rates, window = p["rates"], p["windows"][0]
        client = clients.get(p["client"]) or next(iter(clients.values()))
        projects[p["key"]] = Project(
            id=uuid.uuid4(),
            organisation_id=org.id,
            client_id=client.id,
            site_id=site.id,
            name=p["name"],
            type=ProjectType.training if p["type"] == "training" else ProjectType.security,
            billing_rate=_hourly(rates["billing"], rates["billing_unit"], window),
            start_date=first_day,
            status=ProjectStatus.active,
            managers_see_pay=True,
            terms_estimated=bool(p["rates_estimated"]),
        )
        pay_of[p["key"]] = _hourly(rates["pay"], rates["pay_unit"], window)
    await db.flush()
    db.add_all(projects.values())
    await db.flush()
    last_weeks = set(data["weeks"][-2:])
    recent = {
        s["project"]
        for s in data["shifts"]
        if str(_sunday(dt.date.fromisoformat(s["date"]))) in last_weeks and s["people"]
    }
    for p in data["projects"]:
        project = projects[p["key"]]
        for start, end in p["windows"]:
            await db.execute(
                project_shift_types.insert().values(
                    project_id=project.id, shift_type_id=types[(start, end)].id
                )
            )
        manager = managers.get(p["manager"] or "")
        if manager is not None:
            db.add(
                ProjectManager(organisation_id=org.id, project_id=project.id, user_id=manager.id)
            )
        first = types[tuple(p["windows"][0])]
        # Needs only for tasks still running (work in the last two weeks of the sheet), so a
        # schedule run fills what is live, not tasks that have ended.
        if p["key"] not in recent:
            continue
        for weekday, count in p["needs_by_weekday"].items():
            if count:
                db.add(
                    StaffingNeed(
                        id=uuid.uuid4(),
                        organisation_id=org.id,
                        project_id=project.id,
                        shift_type_id=first.id,
                        weekday=int(weekday),
                        headcount=int(count),
                    )
                )

    # An availability window open for the week after next, as the managers would have it.
    after_next = _sunday(today) + dt.timedelta(days=14)
    db.add(
        SubmissionWindow(
            id=uuid.uuid4(),
            organisation_id=org.id,
            starts_on=after_next,
            ends_on=after_next + dt.timedelta(days=6),
            deadline=dt.datetime.combine(
                after_next - dt.timedelta(days=4), dt.time(18), ZONE
            ).astimezone(dt.UTC),
        )
    )

    people: dict[str, User] = {}
    for n, person in enumerate(data["people"], start=1):
        if person["name"] in managers:
            people[person["name"]] = managers[person["name"]]
            continue
        main = person["projects"][0] if person["projects"] else None
        temporary = bool(person["temporary"])
        user = account(
            f"emp{n:03d}@demo.hds.test",
            person["name"],
            UserRole.employee,
            default_hourly_rate=pay_of.get(main) if main else None,
            employment_type="temporary" if temporary else "permanent",
            engaged_from=dt.date.fromisoformat(person["engaged_from"]) if temporary else None,
            engaged_until=dt.date.fromisoformat(person["engaged_until"]) if temporary else None,
        )
        people[person["name"]] = user
    await db.flush()
    for person in data["people"]:
        if person["temporary"]:
            for key in person["projects"]:
                db.add(
                    ProjectMember(
                        organisation_id=org.id,
                        project_id=projects[key].id,
                        user_id=people[person["name"]].id,
                    )
                )

    this_week = _sunday(today)
    shifts = assignments = 0
    # Assignments go in after their shifts: no relationship tells the session the order.
    pending: list[Assignment] = []
    for s in data["shifts"]:
        on = projects.get(s["project"])
        if on is None or s["headcount"] < 1:
            continue
        day = dt.date.fromisoformat(s["date"])
        kind = (
            types.get((s["start"], s["end"]))
            or types[
                tuple(next(p for p in data["projects"] if p["key"] == s["project"])["windows"][0])
            ]
        )
        starts_at, ends_at = _span(day, s["start"], s["end"])
        published = _sunday(day) <= this_week
        sums = [e["sum"] for e in s["people"] if "sum" in e]
        shift_rate = None
        if len(s["people"]) == 1 and sums:
            shift_rate = (Decimal(sums[0]) / _hours(s["start"], s["end"])).quantize(FOUR)
        elif s["people"] and (hourly := [e["hourly"] for e in s["people"] if "hourly" in e]):
            # One rate for the whole shift only when everyone on it has the same; otherwise each
            # person's own default rate applies (an assignment has no rate of its own).
            if len(hourly) == len(s["people"]) and len(set(hourly)) == 1:
                shift_rate = Decimal(hourly[0]).quantize(FOUR)
        shift = Shift(
            id=uuid.uuid4(),
            organisation_id=org.id,
            project_id=on.id,
            shift_type_id=kind.id,
            starts_at=starts_at,
            ends_at=ends_at,
            headcount=s["headcount"],
            pay_rate=shift_rate,
            status="published" if published else "draft",
            published_at=now if published else None,
        )
        db.add(shift)
        shifts += 1
        seen: set[uuid.UUID] = set()
        for e in s["people"]:
            person = people.get(e["name"])
            if person is None or person.id in seen:
                continue
            seen.add(person.id)
            own_start = own_end = None
            if ("start" in e or "end" in e) and (
                e.get("start", s["start"]),
                e.get("end", s["end"]),
            ) != (s["start"], s["end"]):
                own_start, own_end = _own_span(
                    day, starts_at, ends_at, e.get("start"), e.get("end")
                )
            pending.append(
                Assignment(
                    id=uuid.uuid4(),
                    organisation_id=org.id,
                    shift_id=shift.id,
                    user_id=person.id,
                    source="manual",
                    starts_at=own_start,
                    ends_at=own_end,
                    note=(e.get("note") or "")[:500] or None,
                )
            )
            assignments += 1
    await db.flush()
    db.add_all(pending)
    await db.flush()
    return {
        "clients": len(clients),
        "sites": len(sites),
        "projects": len(projects),
        "shift types": len(types),
        "people": len(people),
        "temporary": sum(1 for p in data["people"] if p["temporary"]),
        "shifts": shifts,
        "assignments": assignments,
    }


async def main(path: str) -> None:
    """Load the file at ``path`` into the database named by ``DATABASE_URL``, in one commit."""
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    engine = create_async_engine(get_settings().DATABASE_URL, echo=False)
    async with async_sessionmaker(engine, expire_on_commit=False)() as db:
        counts = await load(db, data, dt.datetime.now(ZONE).date())
        await db.commit()
    await engine.dispose()
    print("seed_demo: " + ", ".join(f"{v} {k}" for k, v in counts.items()))


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "../../demo-data/roster.json"))
