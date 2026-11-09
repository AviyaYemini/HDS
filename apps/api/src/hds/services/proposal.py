"""A proposal under review: read it as a change, edit it, undo an edit (008 US1, US2; ADR 020).

Layer: services. Depends on ``hds.core``, ``hds.scheduling`` (pure), ``hds.models``, the
repositories and the services it is given; it MUST NOT import routers or know about HTTP.

Rules: an Admin, or a Manager of every project in the run, reads and edits an open proposal (007
T108, 008 FR-007, FR-008). An edit changes only the working copy, never the roster, until it is
accepted. After each edit the rules are checked over the person's shifts in every project with the
copy (FR-009); a break the roster lets a person override needs a reason, which the copy keeps, and
an inactive employee is refused (FR-010, research P13). Edits are undone one at a time (FR-011).
A slot with a part accepted is a draft now and is edited on the roster.
"""

import uuid
from collections.abc import Callable, Collection, Mapping
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

from hds.core.authz import MANAGER_ROLES, ensure_role
from hds.core.exceptions import ConflictError, DomainValidationError, NotFoundError
from hds.models.schedule_job import ScheduleJob
from hds.models.user import User, UserRole
from hds.repositories.organisation import OrganisationRepository
from hds.repositories.schedule_job_event import ScheduleJobEventRepository
from hds.repositories.user import UserRepository
from hds.scheduling import Employee, RuleWarning, chips, fairness, validate
from hds.scheduling import cover as covering
from hds.scheduling.eligibility import Roster
from hds.scheduling.engine import slot_id
from hds.scheduling.problem import Problem
from hds.services import proposal_copy
from hds.services.schedule_inputs import ScheduleInputService
from hds.services.schedule_job import ScheduleJobService, check_version, stored_chip

OPS = ("assign", "unassign", "move", "swap", "lock", "unlock")
STAFF = (UserRole.employee, UserRole.manager, UserRole.admin)


class ProposalService:
    """Reads and edits proposals for one request's session."""

    def __init__(
        self,
        jobs: ScheduleJobService,
        inputs: ScheduleInputService,
        users: UserRepository,
        organisations: OrganisationRepository,
        events: ScheduleJobEventRepository,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        """Wire the service.

        Args:
            jobs: Schedule jobs: the job, its scope and its record.
            inputs: Builds the run's problem again, for the rule checks.
            users: The people an edit places, read to know they are staff and active.
            organisations: The organisation's time zone.
            events: The run record, read to undo.
            now: The current UTC time.
        """
        self._jobs = jobs
        self._inputs = inputs
        self._users = users
        self._orgs = organisations
        self._events = events
        self._now = now

    def state(self, job: ScheduleJob) -> str | None:
        """The proposal's state now, ``expired`` and ``partly_accepted`` included."""
        return self._jobs.state(job)

    async def read(self, actor: User, id: uuid.UUID) -> tuple[ScheduleJob, dict[str, Any]]:
        """The job and its proposal's working copy with change marks (FR-001, FR-002).

        Raises:
            ForbiddenError: ``users.manager_required`` or ``roster.project_not_managed`` (HTTP 403).
            NotFoundError: Not in the organisation (HTTP 404).
            ConflictError: ``schedule.not_ready`` when the job has no proposal (HTTP 409).
        """
        job = await self._readable(actor, id)
        return job, view(proposal_copy.of(job.current, job.result))

    async def _readable(self, actor: User, id: uuid.UUID) -> ScheduleJob:
        """The job, once ``actor`` may read its proposal and it has one (see ``read``)."""
        ensure_role(actor, *MANAGER_ROLES)
        job = await self._jobs.get(actor, id)
        await self._jobs.require_scope_of(actor, job)
        if job.status != "succeeded" or job.result is None:
            raise ConflictError(
                code="schedule.not_ready", detail="The job has no proposal to read."
            )
        return job

    async def edit(
        self, actor: User, id: uuid.UUID, op: Mapping[str, Any], if_match: int | None = None
    ) -> tuple[ScheduleJob, dict[str, Any], list[dict[str, Any]]]:
        """Apply one edit to an open proposal's working copy (FR-008 to FR-012).

        ``op`` has ``op`` (assign, unassign, move or swap), ``key`` and ``user_id``; a move names
        ``to_key``; a swap names ``to_key`` and ``other_user_id`` (the person there); any edit may
        give ``override_reason``.

        Returns:
            The job, the new view of its copy, and every rule warning the edit's people have on
            the slots it touched (those excused by an override included, marked).

        Raises:
            ForbiddenError: ``users.manager_required`` or ``roster.project_not_managed`` (HTTP 403).
            NotFoundError: Not in the organisation; a person not in it (HTTP 404).
            PreconditionFailedError: ``schedule.version_stale`` (HTTP 412).
            ConflictError: ``schedule.not_ready``, ``schedule.proposal_closed`` or
                ``schedule.part_accepted`` (HTTP 409).
            DomainValidationError: ``schedule.edit_invalid``, ``warnings.employee_inactive`` or
                ``schedule.override_reason_required`` with the breaks (HTTP 422).
        """
        job = await self._jobs.open_for(actor, id)
        check_version(job, if_match)
        work = proposal_copy.of(job.current, job.result)
        before, touched = apply(work, op)
        for key in touched:
            slot = proposal_copy.find(work, key)
            assert slot is not None
            if set(slot["stored"]) != set(slot["run_before"]):
                raise ConflictError(
                    code="schedule.part_accepted",
                    detail="Part of this shift is accepted; change it on the roster.",
                )
        people = {uuid.UUID(p) for who in touched.values() for p in who}
        known = await self._staff(actor.organization_id, people)
        inactive = sorted(str(p) for p, e in known.items() if not e.is_active)
        if inactive:
            raise DomainValidationError(
                code="warnings.employee_inactive",
                detail="An inactive employee cannot be placed.",
                errors=[{"user_id": p, "code": "warnings.employee_inactive"} for p in inactive],
            )
        found = await self._check(job, work, touched, known)
        causes = proposal_copy.unexcused(found, work, touched)
        reason = (op.get("override_reason") or "").strip()
        if causes and not reason:
            raise DomainValidationError(
                code="schedule.override_reason_required",
                detail="This edit breaks a rule; give a reason to keep it.",
                errors=causes,
            )
        if causes:
            override(work, causes, reason)
        job.current = work
        job.version += 1
        await self._jobs.save(job)
        recorded_op = {k: v for k, v in op.items() if k != "override_reason"}
        body = {"op": recorded_op, "before": before, "breaks": [c["code"] for c in causes]}
        await self._jobs.record(job, "edited", body, actor=actor, version=job.version)
        if causes:
            await self._jobs.record(
                job,
                "overridden",
                {"breaks": causes, "reason": reason},
                actor=actor,
                version=job.version,
            )
        return job, view(work), warnings_of(found, work, touched)

    async def undo(
        self, actor: User, id: uuid.UUID, if_match: int | None = None
    ) -> tuple[ScheduleJob, dict[str, Any]]:
        """Undo the last edit not yet undone (FR-011).

        Raises:
            ForbiddenError, NotFoundError, PreconditionFailedError: as ``edit``.
            ConflictError: ``schedule.nothing_to_undo`` when there is no edit left to undo, or its
                shifts have a part accepted since (HTTP 409).
        """
        job = await self._jobs.open_for(actor, id)
        check_version(job, if_match)
        last = await self._events.last_edit_not_undone(job.organization_id, job.id)
        if last is None:
            raise ConflictError(code="schedule.nothing_to_undo", detail="There is no edit to undo.")
        work = proposal_copy.of(job.current, job.result)
        for key, people in last.body["before"].items():
            slot = proposal_copy.find(work, key)
            if slot is None or set(slot["stored"]) != set(slot["run_before"]):
                raise ConflictError(
                    code="schedule.nothing_to_undo",
                    detail="That edit's shift has a part accepted since; change it on the roster.",
                )
            slot["people"] = people
        job.current = work
        job.version += 1
        await self._jobs.save(job)
        await self._jobs.record(
            job, "undone", {"edit": str(last.id)}, actor=actor, version=job.version
        )
        return job, view(work)

    async def cover(self, actor: User, id: uuid.UUID, key: str) -> dict[str, Any]:
        """Who else could cover slot ``key`` of the proposal (FR-014, FR-016, research P11).

        Every active staff member not on it, judged over all their shifts in every project, the
        roster's drafts and the working copy: free people first, then those who would break a
        rule, with the rule and numbers; and the staff's totals, whose averages their numbers
        are set against. Codes, numbers and ids only: never pay, a site, or another project's
        shift.

        Raises:
            ForbiddenError: ``users.manager_required`` or ``roster.project_not_managed`` (HTTP 403).
            NotFoundError: Not in the organisation, or no slot ``key`` (HTTP 404).
            ConflictError: ``schedule.not_ready`` when the job has no proposal (HTTP 409).
        """
        job = await self._readable(actor, id)
        work = proposal_copy.of(job.current, job.result)
        if proposal_copy.find(work, key) is None:
            raise NotFoundError(resource="Slot", id=slot_id(key))
        staff = await self._users.staff_of(job.organization_id)
        base = await self._copy_problem(job, work, also={u.id for u in staff})
        problem = replace(base, candidates=tuple(Employee(u.id, True) for u in staff))
        assigned = {
            s["key"]: [uuid.UUID(p) for p in proposal_copy.people(s)] for s in work["slots"]
        }
        names = {u.id: u.full_name for u in staff}
        loaded = covering.loads(problem, assigned)
        found = covering.options(problem, assigned, key, names, loaded)
        # The average counts the people who work shifts: every Employee, and a Manager or Admin
        # only when they have hours in the range, so idle office staff never pull it down.
        worked = loaded[0]
        among = [u.id for u in staff if u.role == UserRole.employee or worked[u.id] > 0]
        people, minutes, nights, weekends = covering.totals(loaded, among)
        return {
            "key": key,
            "options": [
                {
                    "user_id": o.user_id,
                    "free": o.code is None,
                    "code": o.code,
                    "have": o.detail.have if o.detail else None,
                    "need": o.detail.need if o.detail else None,
                    "week_minutes": o.detail.week_minutes if o.detail else None,
                    "slot_minutes": o.detail.slot_minutes if o.detail else None,
                    "limit": o.detail.limit if o.detail else None,
                    "close": o.close,
                    "open": o.open,
                    "minutes": o.minutes,
                    "nights": o.nights,
                    "weekends": o.weekends,
                }
                for o in found
            ],
            "totals": {
                "people": people,
                "minutes": minutes,
                "nights": nights,
                "weekends": weekends,
            },
        }

    async def fairness(self, actor: User, id: uuid.UUID) -> dict[str, Any]:
        """The proposal's fairness, trade-offs, chips and guarantees, as it stands now.

        008 FR-023 to FR-026. Judged on the working copy, so it follows every edit at once.
        Numbers only; the client words them. ``overridden`` counts the assignments kept despite
        a rule by a person's reason (FR-023); ``locks`` the locks the copy keeps (FR-018).

        Raises:
            ForbiddenError, NotFoundError, ConflictError: as ``read``.
        """
        job = await self._readable(actor, id)
        work = proposal_copy.of(job.current, job.result)
        problem = await self._copy_problem(job, work)
        problem = replace(problem, chips=tuple(stored_chip(c) for c in job.chips or ()))
        assigned = {
            s["key"]: [uuid.UUID(p) for p in proposal_copy.people(s)] for s in work["slots"]
        }
        results = chips.judge(problem, Roster(problem), assigned)
        found = fairness.panel(problem, assigned)
        traded = fairness.tradeoffs(problem, assigned, results)
        people = [p for s in work["slots"] for p in s["people"]]
        return {
            "shares": [vars(s) for s in found.shares],
            "totals": {
                "people": found.people,
                "minutes": found.minutes,
                "nights": found.nights,
                "weekends": found.weekends,
            },
            "tradeoffs": vars(traded),
            "chips": [
                {**dict(job.chips[r.index]), "state": r.state, "cause": r.cause} for r in results
            ],
            "overridden": sum(1 for p in people if p.get("override")),
            # The locks the run kept from the roster, and those set in the proposal since.
            "locks": int(work.get("locks_kept", 0)) + sum(1 for p in people if p.get("locked")),
        }

    async def _copy_problem(
        self, job: ScheduleJob, work: Mapping[str, Any], also: Collection[uuid.UUID] = ()
    ) -> Problem:
        """The run's problem rebuilt today, with the working copy's slots in place of its own.

        Drafts made by hand since the run are fixed shifts here, as the edit's check sees them.
        """
        org = await self._orgs.get_by_id(job.organization_id)
        if org is None:
            raise NotFoundError(resource="Organisation", id=job.organization_id)
        built = await self._inputs.build(
            job.organization_id,
            job.first_day,
            job.last_day,
            [uuid.UUID(p) for p in job.project_ids],
            also=set(also),
        )
        base = built.problem
        slots = tuple(
            proposal_copy.as_slot(s, org.timezone, base.night_types) for s in work["slots"]
        )
        ours = {proposal_copy.shift_id_of(s) for s in work["slots"]}
        extra = proposal_copy.unlisted(base, work, ours)
        return replace(base, slots=slots, fixed=(*base.fixed, *extra), current={})

    async def _staff(self, org_id: uuid.UUID, ids: set[uuid.UUID]) -> dict[uuid.UUID, Employee]:
        """The people an edit places, as the rules see them.

        Raises:
            NotFoundError: Someone not a staff member of the organisation.
        """
        users = await self._users.get_many(ids, org_id) if ids else []
        # A deleted account is not staff any more, whatever its active flag says (R3).
        found = {u.id: u for u in users if u.role in STAFF and not u.is_deleted}
        for person in ids:
            if person not in found:
                raise NotFoundError(resource="User", id=person)
        return {u.id: Employee(u.id, u.is_active) for u in found.values()}

    async def _check(
        self,
        job: ScheduleJob,
        work: Mapping[str, Any],
        touched: Mapping[str, set[str]],
        known: Mapping[uuid.UUID, Employee],
    ) -> list[RuleWarning]:
        """The warnings on the touched slots, with every shift of their people in every project."""
        org = await self._orgs.get_by_id(job.organization_id)
        if org is None:
            raise NotFoundError(resource="Organisation", id=job.organization_id)
        # Today's limits, as accepting judges them, so an edit and its accept never disagree (R6);
        # and the people placed, candidates or not, with all their shifts and entries (R4).
        built = await self._inputs.build(
            job.organization_id,
            job.first_day,
            job.last_day,
            [uuid.UUID(p) for p in job.project_ids],
            also=set(known),
        )
        ours = proposal_copy.shifts(work, org.timezone)
        extra = proposal_copy.unlisted(built.problem, work, {s.id for s in ours})
        every = [*built.problem.fixed, *extra, *ours]
        focus = {proposal_copy.shift_id_of(s) for s in work["slots"] if s["key"] in touched}
        return validate(
            focus=focus,
            shifts=every,
            entries=built.problem.entries,
            employees=proposal_copy.everyone(built.problem, every, known),
            settings=built.problem.settings,
        )


def apply(
    work: dict[str, Any], op: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, set[str]]]:
    """Apply one edit to ``work`` in place.

    Returns:
        The touched slots' people before the edit, by key (for undo), and the people the edit
        placed or moved, by the key of the slot they are on now (whose rule breaks count).

    Raises:
        DomainValidationError: ``schedule.edit_invalid`` for an unknown op, a slot that is not in
            the proposal, or a person who is not, or already is, where the edit says.
    """

    def invalid(detail: str) -> DomainValidationError:
        return DomainValidationError(code="schedule.edit_invalid", detail=detail)

    kind = op.get("op")
    if kind not in OPS:
        raise invalid("Unknown edit.")
    key, person = str(op.get("key")), str(op.get("user_id"))
    slot = proposal_copy.find(work, key)
    if slot is None:
        raise invalid("That shift is not in the proposal.")
    on = proposal_copy.people(slot)
    target = None
    if kind in ("move", "swap"):
        target = proposal_copy.find(work, str(op.get("to_key")))
        if target is None or target is slot:
            raise invalid("Choose another shift of the proposal.")
    before = {key: [dict(p) for p in slot["people"]]}
    if target is not None:
        before[target["key"]] = [dict(p) for p in target["people"]]

    def entry(user_id: str) -> dict[str, Any]:
        return {"user_id": user_id, "by": "person", "locked": False, "override": None}

    touched: dict[str, set[str]] = {}
    if kind in ("lock", "unlock"):
        # A lock marks a person later runs keep (008 FR-018); it changes no one's place, so no
        # rule is judged again.
        if person not in on:
            raise invalid("That person is not on this shift.")
        for p in slot["people"]:
            if p["user_id"] == person:
                p["locked"] = kind == "lock"
        return before, {key: set()}
    if kind == "assign":
        if person in on:
            raise invalid("That person is already on this shift.")
        slot["people"].append(entry(person))
        touched[key] = {person}
    elif kind == "unassign":
        if person not in on:
            raise invalid("That person is not on this shift.")
        slot["people"] = [p for p in slot["people"] if p["user_id"] != person]
        touched[key] = set()
    elif kind == "move":
        assert target is not None
        if person not in on or person in proposal_copy.people(target):
            raise invalid("Move someone on this shift to a shift they are not on.")
        slot["people"] = [p for p in slot["people"] if p["user_id"] != person]
        target["people"].append(entry(person))
        touched[key] = set()
        touched[target["key"]] = {person}
    else:
        assert target is not None
        other = str(op.get("other_user_id"))
        theirs = proposal_copy.people(target)
        if person not in on or other not in theirs or person in theirs or other in on:
            raise invalid("Swap someone on this shift with someone on the other.")
        slot["people"] = [p for p in slot["people"] if p["user_id"] != person] + [entry(other)]
        target["people"] = [p for p in target["people"] if p["user_id"] != other] + [entry(person)]
        touched[key] = {other}
        touched[target["key"]] = {person}
    return before, touched


def override(work: dict[str, Any], causes: list[dict[str, Any]], reason: str) -> None:
    """Record ``reason`` on each person whose break ``causes`` names, with the codes it excuses."""
    for cause in causes:
        slot = proposal_copy.find(work, cause["key"])
        assert slot is not None
        for p in slot["people"]:
            if cause["user_id"] is not None and p["user_id"] != cause["user_id"]:
                continue
            if cause["user_id"] is None and p["by"] != "person":
                continue
            held = set((p.get("override") or {}).get("codes", ()))
            p["override"] = {"codes": sorted(held | {cause["code"]}), "reason": reason}


def warnings_of(
    found: list[RuleWarning], work: Mapping[str, Any], touched: Mapping[str, set[str]]
) -> list[dict[str, Any]]:
    """Every warning of the edit's people on the touched slots, each marked excused or not."""
    by_id = {proposal_copy.shift_id_of(s): s for s in work["slots"]}
    shown = []
    for w in found:
        for sid in w.shift_ids:
            slot = by_id.get(sid)
            if slot is None or slot["key"] not in touched:
                continue
            person = str(w.employee_id) if w.employee_id else None
            if person is not None and person not in touched[slot["key"]]:
                continue
            codes = {
                c
                for p in slot["people"]
                if person is None or p["user_id"] == person
                for c in (p.get("override") or {}).get("codes", ())
            }
            shown.append(
                {
                    "key": slot["key"],
                    "user_id": person,
                    "code": w.code,
                    "values": dict(w.values),
                    "excused": w.code in codes,
                }
            )
            break
    return shown


def view(work: Mapping[str, Any]) -> dict[str, Any]:
    """The working copy as the review reads it: marks, places and coverage per day and project."""
    coverage: dict[tuple[str, str], dict[str, Any]] = {}
    slots = []
    for slot in work["slots"]:
        empty = proposal_copy.places(slot)
        filled = len(slot.get("kept", [])) + len(slot["people"])
        cell = coverage.setdefault(
            (slot["date"], slot["project_id"]),
            {"date": slot["date"], "project_id": slot["project_id"], "filled": 0, "needed": 0},
        )
        cell["filled"] += min(filled, slot["headcount"])
        cell["needed"] += slot["headcount"]
        slots.append(
            {
                **{k: slot[k] for k in slot if k not in ("run_before", "stored")},
                "places": empty,
                "marks": proposal_copy.marks(slot),
                # A part of it is on the roster now: edit it there, not here (R8).
                "part_accepted": set(slot["stored"]) != set(slot["run_before"]),
            }
        )
    return {
        "slots": slots,
        "counts": proposal_copy.counts(work),
        "coverage": sorted(coverage.values(), key=lambda c: (c["date"], c["project_id"])),
        "unfilled": work.get("unfilled", []),
        "employees": work.get("employees", []),
        "no_availability": work.get("no_availability", []),
        "settled": proposal_copy.is_settled(work),
    }
