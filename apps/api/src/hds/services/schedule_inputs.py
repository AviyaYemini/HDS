"""Building a roster problem from what is stored, and its fingerprint (004 research G9).

Layer: services. Depends on ``hds.core``, ``hds.scheduling`` (pure), ``hds.models`` and the
repositories it is given; it MUST NOT import routers or know about HTTP.

The slots are the staffing needs on each date of each run project within its own dates (FR-006),
with an existing draft standing in for the need it matches, and every other draft of those projects
in the range as a slot too; a published shift is never a slot. Candidates are every active Employee
and the Managers and Admins with an open entry in the range (FR-007); people on a draft who are not
candidates are kept on it. Every other shift in the run's surroundings (the weeks the range touches,
plus the minimum rest either side, FR-016) is fixed. The fingerprint is a SHA-256 of all of it, so
any change accepting depends on changes it (FR-024).
"""

import hashlib
import json
import uuid
from collections import defaultdict
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from hds.repositories.availability import AvailabilityRepository
from hds.repositories.organisation import OrganisationRepository
from hds.repositories.scheduling_inputs import (
    ActiveProject,
    SchedulingInputRepository,
    StoredShift,
)
from hds.repositories.shift_type import ShiftTypeRepository
from hds.repositories.staffing_need import StaffingNeedRepository
from hds.scheduling import Entry, Problem, Settings, ShiftSlot, Slot, local_span
from hds.scheduling.rules import local_day, sunday_index, week_start
from hds.services.temporary import as_employee

if TYPE_CHECKING:
    from hds.services.availability_submission import AvailabilitySubmissionService


@dataclass(frozen=True)
class ShortPublished:
    """A published shift with fewer people than its staffing need; reported, never changed."""

    shift_id: uuid.UUID
    needed: int
    assigned: int


@dataclass(frozen=True)
class Built:
    """A problem ready to solve, what the proposal reports beside it, and its fingerprint."""

    problem: Problem
    project_ids: tuple[uuid.UUID, ...]
    no_availability: tuple[uuid.UUID, ...]
    short_published: tuple[ShortPublished, ...]
    fingerprint: str
    # How many locked assignments the run keeps (008 FR-018: it says so).
    locks: int = 0
    # People a rerun holds because the earlier proposal locked them, by slot key (008 FR-019):
    # kept while solving, yet the proposal's own, so accepting writes them to the roster.
    held: Mapping[str, tuple[uuid.UUID, ...]] = field(default_factory=dict)


def _dates(first: date, last: date) -> list[date]:
    return [first + timedelta(days=n) for n in range((last - first).days + 1)]


def _midnight(day: date, zone: str) -> datetime:
    return datetime.combine(day, time(0), tzinfo=ZoneInfo(zone)).astimezone(UTC)


class ScheduleInputService:
    """Builds the problem a run solves, and rebuilds it to check a proposal is still current."""

    def __init__(
        self,
        inputs: SchedulingInputRepository,
        organisations: OrganisationRepository,
        shift_types: ShiftTypeRepository,
        needs: StaffingNeedRepository,
        entries: AvailabilityRepository,
        availability: "AvailabilitySubmissionService | None" = None,
    ) -> None:
        """Wire the service to the repositories it reads.

        ``availability``, when given, supplies the entries that stand once windows close (007
        FR-014): snapshots, and a ``not_submitted`` block for whoever did not submit. Without it
        the live entries are read, as before 007.
        """
        self._inputs = inputs
        self._orgs = organisations
        self._types = shift_types
        self._needs = needs
        self._entries = entries
        self._availability = availability

    async def active_projects(
        self, org_id: uuid.UUID, ids: Collection[uuid.UUID] | None
    ) -> list[ActiveProject]:
        """The organisation's active projects, only ``ids`` when given (empty or ``None``: all)."""
        return await self._inputs.active_projects(org_id, ids or None)

    async def build(
        self,
        org_id: uuid.UUID,
        first: date,
        last: date,
        project_ids: Sequence[uuid.UUID],
        limits: Mapping[str, int] | None = None,
        also: Collection[uuid.UUID] = (),
    ) -> Built:
        """The problem for ``first`` to ``last`` over ``project_ids`` (every active one if empty).

        Args:
            org_id: The organisation.
            first: The first date of the range.
            last: The last date of the range.
            project_ids: The run's projects; empty for every active one.
            limits: ``min_rest_hours`` and ``max_weekly_hours`` to solve with, as recorded on a
                job; the organisation's current ones when ``None`` (accepting compares with these,
                so a change of limits makes a proposal out of date).
            also: People who are not candidates but are being placed by hand (008: a Manager or
                Admin without an open entry); their shifts in every project and their entries
                are read too, so the rules judge them like anyone else. They never become
                candidates, but the fingerprint then covers their shifts and entries too, so
                compare a job's fingerprint only with a build made without them.

        Raises:
            LookupError: The organisation does not exist.
        """
        org = await self._orgs.get_by_id(org_id)
        if org is None:
            raise LookupError(org_id)
        zone = org.timezone
        rest = limits["min_rest_hours"] if limits else org.min_rest_hours
        weekly = limits["max_weekly_hours"] if limits else org.max_weekly_hours
        settings = Settings(
            min_rest=timedelta(hours=rest), max_weekly=timedelta(hours=weekly), week_zone=zone
        )
        projects = await self.active_projects(org_id, project_ids)
        run_ids = {p.id for p in projects}
        types = {t.id: t for t in await self._types.list_for_org(org_id)}
        in_use = await self._inputs.project_types(run_ids)
        needs = await self._needs.for_projects(run_ids)

        first_week, last_week, window = surroundings(zone, first, last, settings.min_rest)
        shifts = await self._inputs.shifts_between(org_id, *window)
        people = await self._inputs.candidates(org_id, first, last)
        candidates = {u.id for u in people}

        by_cell: dict[tuple[uuid.UUID, uuid.UUID, date], list[StoredShift]] = defaultdict(list)
        for s in shifts:
            if s.project_id in run_ids:
                by_cell[s.project_id, s.shift_type_id, local_day(s.starts_at, s.zone)].append(s)

        slots: list[Slot] = []
        # Each run project's site, for chips that keep someone on a site (008 FR-020).
        sites = {p.id: p.site_id for p in projects}
        current: dict[str, tuple[uuid.UUID, ...]] = {}
        short: list[ShortPublished] = []
        used_drafts: set[uuid.UUID] = set()
        locks = 0

        def draft_slot(s: StoredShift) -> None:
            """Make a stored draft a slot: its candidates are current, the rest are kept on it.

            A locked candidate is kept too (008 FR-018, research P2): the run holds their place.
            """
            key = f"draft:{s.id}"
            slots.append(
                Slot(
                    key=key,
                    shift_id=s.id,
                    project_id=s.project_id,
                    shift_type_id=s.shift_type_id,
                    starts_at=s.starts_at,
                    ends_at=s.ends_at,
                    zone=s.zone,
                    headcount=s.headcount,
                    night=types[s.shift_type_id].crosses_midnight,
                    kept=tuple(p for p in s.people if p not in candidates or p in s.locked),
                    site_id=sites.get(s.project_id),
                )
            )
            current[key] = tuple(p for p in s.people if p in candidates and p not in s.locked)
            used_drafts.add(s.id)
            nonlocal locks
            locks += sum(1 for p in s.people if p in s.locked)

        needs_by_project: dict[uuid.UUID, list[Any]] = defaultdict(list)
        for need in needs:
            needs_by_project[need.project_id].append(need)
        cells_by_project: dict[uuid.UUID, list[tuple[date, list[StoredShift]]]] = defaultdict(list)
        for (pid, _, day), stored in sorted(by_cell.items(), key=lambda kv: str(kv[0])):
            cells_by_project[pid].append((day, stored))
        for project in projects:
            project_needs = sorted(needs_by_project[project.id], key=lambda n: str(n.shift_type_id))
            for day in _dates(first, last):
                if day < project.start_date or (project.end_date and day > project.end_date):
                    continue
                for need in project_needs:
                    if need.weekday != sunday_index(day):
                        continue
                    if (project.id, need.shift_type_id) not in in_use:
                        continue
                    stored = by_cell.get((project.id, need.shift_type_id, day), [])
                    published = [s for s in stored if s.status == "published"]
                    drafts = [s for s in stored if s.status == "draft"]
                    if published:
                        held = sum(len(s.people) for s in published)
                        if held < need.headcount:
                            short.append(ShortPublished(published[0].id, need.headcount, held))
                    elif drafts:
                        for s in drafts:
                            draft_slot(s)
                    else:
                        kind = types[need.shift_type_id]
                        starts_at, ends_at = local_span(
                            day,
                            kind.starts_at_local,
                            kind.ends_at_local,
                            kind.crosses_midnight,
                            project.zone,
                        )
                        key = f"need:{project.id}:{need.shift_type_id}:{day.isoformat()}"
                        slots.append(
                            Slot(
                                key=key,
                                shift_id=None,
                                project_id=project.id,
                                shift_type_id=need.shift_type_id,
                                starts_at=starts_at,
                                ends_at=ends_at,
                                zone=project.zone,
                                headcount=need.headcount,
                                night=kind.crosses_midnight,
                                site_id=project.site_id,
                            )
                        )
            # Every other draft of the project in the range is staffed too.
            for day, stored in cells_by_project[project.id]:
                if not first <= day <= last:
                    continue
                for s in stored:
                    if s.status == "draft" and s.id not in used_drafts:
                        draft_slot(s)

        relevant = candidates | {p for s in slots for p in s.kept} | set(also)
        fixed = tuple(
            ShiftSlot(
                id=s.id,
                project_id=s.project_id,
                shift_type_id=s.shift_type_id,
                starts_at=s.starts_at,
                ends_at=s.ends_at,
                zone=s.zone,
                headcount=s.headcount,
                assigned=tuple(p for p in s.people if p in relevant),
                own_times=tuple(t for t in s.own_times if t[0] in relevant),
            )
            for s in shifts
            if s.id not in used_drafts and any(p in relevant for p in s.people)
        )
        entry_first, entry_last = first_week - timedelta(days=1), last_week
        if self._availability is not None:
            loaded = await self._availability.scheduling_entries(
                org_id, candidates | set(also), entry_first, entry_last
            )
        else:
            stored_entries = await self._entries.for_users(
                org_id, candidates | set(also), entry_first, entry_last
            )
            loaded = [
                Entry(
                    employee_id=e.user_id,
                    kind=e.kind,  # type: ignore[arg-type]
                    starts_on=e.starts_on,
                    ends_on=e.ends_on,
                    shift_type_id=e.shift_type_id,
                    weekday=e.weekday,
                )
                for e in sorted(stored_entries, key=lambda e: str(e.id))
            ]
        entries = tuple(loaded)
        # A not-submitted block is not availability the employee gave, so they stay listed.
        with_entries = {
            e.employee_id
            for e in entries
            if e.starts_on <= last and e.ends_on >= first and e.reason is None
        }
        problem = Problem(
            slots=tuple(sorted(slots, key=lambda s: (s.starts_at, s.key))),
            fixed=fixed,
            candidates=tuple(as_employee(u) for u in people),
            entries=entries,
            settings=settings,
            first_day=first,
            last_day=last,
            current=current,
            night_types=frozenset(t.id for t in types.values() if t.crosses_midnight),
        )
        no_availability = tuple(sorted((c for c in candidates if c not in with_entries), key=str))
        ordered_ids = tuple(sorted(run_ids, key=str))
        return Built(
            problem=problem,
            project_ids=ordered_ids,
            no_availability=no_availability,
            short_published=tuple(short),
            fingerprint=fingerprint(problem, ordered_ids, no_availability, short, in_use),
            locks=locks,
        )


def surroundings(
    zone: str, first: date, last: date, min_rest: timedelta
) -> tuple[date, date, tuple[datetime, datetime]]:
    """The run's surroundings (FR-016).

    The Sunday of the first week, the Sunday after the last, and the UTC window from the first to
    the second, widened by the minimum rest either side.
    """
    first_week = week_start(_midnight(first, zone), zone)
    last_week = week_start(_midnight(last, zone), zone) + timedelta(days=7)
    window = (_midnight(first_week, zone) - min_rest, _midnight(last_week, zone) + min_rest)
    return first_week, last_week, window


def fingerprint(
    problem: Problem,
    project_ids: Sequence[uuid.UUID],
    no_availability: Sequence[uuid.UUID],
    short: Sequence[ShortPublished],
    project_types: Collection[tuple[uuid.UUID, uuid.UUID]] = (),
) -> str:
    """The SHA-256 of everything a proposal was solved from, in a canonical form.

    The projects' shift types count even where no need uses them, because FR-024 names a change to
    them as making a proposal out of date.
    """

    def at(moment: datetime) -> str:
        return moment.astimezone(UTC).isoformat()

    canonical = {
        "projects": [str(i) for i in project_ids],
        "project_types": sorted(f"{p}:{t}" for p, t in project_types),
        "range": [problem.first_day.isoformat(), problem.last_day.isoformat()],
        "settings": [
            problem.settings.min_rest.total_seconds(),
            problem.settings.max_weekly.total_seconds(),
            problem.settings.week_zone,
        ],
        "slots": [
            [
                s.key,
                str(s.shift_id),
                str(s.project_id),
                str(s.shift_type_id),
                at(s.starts_at),
                at(s.ends_at),
                s.zone,
                s.headcount,
                s.night,
                sorted(str(p) for p in s.kept),
                sorted(str(p) for p in problem.current.get(s.key, ())),
            ]
            for s in sorted(problem.slots, key=lambda s: s.key)
        ],
        "fixed": [
            [str(f.id), str(f.shift_type_id), at(f.starts_at), at(f.ends_at), f.zone]
            + sorted(str(p) for p in f.assigned)
            # Only when set, so the fingerprints of proposals made before 007 still match.
            + sorted(f"{p}@{at(a)}/{at(b)}" for p, a, b in f.own_times)
            for f in sorted(problem.fixed, key=lambda f: str(f.id))
        ],
        "candidates": sorted(f"{c.id}:{c.is_active}" for c in problem.candidates),
        "entries": sorted(
            json.dumps(
                [
                    str(e.employee_id),
                    e.kind,
                    e.starts_on.isoformat(),
                    e.ends_on.isoformat(),
                    str(e.shift_type_id),
                    e.weekday,
                ]
                # Only when set, so the fingerprints of proposals made before 007 still match.
                + ([e.reason] if e.reason is not None else [])
            )
            for e in problem.entries
        ),
        "nights": sorted(str(t) for t in problem.night_types),
        "no_availability": [str(p) for p in no_availability],
        "short": [[str(s.shift_id), s.needed, s.assigned] for s in short],
    }
    return hashlib.sha256(json.dumps(canonical, sort_keys=True).encode()).hexdigest()
