"""Who else could cover a shift on the roster, and the one answer cover gives (008 FR-014, FR-034).

Layer: services. Depends on ``hds.scheduling`` (pure), the repositories and the services it is
given; it MUST NOT import routers or know about HTTP.

"Find someone" on an empty roster seat opens the same list as "Who else could cover" in a proposal:
every active staff member not on the shift, judged over all their shifts in every project and the
drafts, free people first, never pay, a site or another project's shift (FR-016). Only someone who
may edit the shift's project's roster asks for it.
"""

import uuid
from collections.abc import Mapping, Sequence
from dataclasses import replace
from typing import Any

from hds.models.user import User, UserRole
from hds.repositories.user import UserRepository
from hds.scheduling import Employee
from hds.scheduling import cover as covering
from hds.scheduling.problem import Problem, Slot
from hds.scheduling.rules import local_day
from hds.services.roster import RosterService
from hds.services.schedule_inputs import ScheduleInputService


def cover_answer(
    problem: Problem,
    assigned: Mapping[str, Sequence[uuid.UUID]],
    key: str,
    staff: Sequence[User],
) -> dict[str, Any]:
    """The cover answer for slot ``key``: each option, best first, and the staff's totals.

    The average counts the people who work shifts: every Employee, and a Manager or Admin only
    when they have hours in the range, so idle office staff never pull it down (R30).
    """
    names = {u.id: u.full_name for u in staff}
    loaded = covering.loads(problem, assigned)
    found = covering.options(problem, assigned, key, names, loaded)
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
        "totals": {"people": people, "minutes": minutes, "nights": nights, "weekends": weekends},
    }


class RosterCoverService:
    """Who else could cover one shift of the roster (FR-034)."""

    def __init__(
        self, roster: RosterService, inputs: ScheduleInputService, users: UserRepository
    ) -> None:
        """Wire the service.

        Args:
            roster: The shift, its scope and its site's zone.
            inputs: Builds the shift's day as a run would, with every staff member's shifts.
            users: The organisation's staff.
        """
        self._roster = roster
        self._inputs = inputs
        self._users = users

    async def cover(self, actor: User, shift_id: uuid.UUID) -> dict[str, Any]:
        """Who else could cover ``shift_id``, best first (FR-014, FR-016, FR-034).

        Raises:
            ForbiddenError: ``users.manager_required`` or ``roster.project_not_managed`` (HTTP 403).
            NotFoundError: The shift is not in the organisation (HTTP 404).
        """
        shift = await self._roster.shift_to_edit(actor, shift_id)
        zone = await self._roster.site_zone(shift.project_id)
        day = local_day(shift.starts_at, zone)
        staff = await self._users.staff_of(actor.organization_id)
        built = await self._inputs.build(
            actor.organization_id, day, day, [shift.project_id], also={u.id for u in staff}
        )
        problem = built.problem
        key = f"draft:{shift.id}"
        if not any(s.key == key for s in problem.slots):
            # A published shift is fixed to a run: here it is the slot, with its people kept.
            on = next((f.assigned for f in problem.fixed if f.id == shift.id), ())
            problem = replace(
                problem,
                fixed=tuple(f for f in problem.fixed if f.id != shift.id),
                slots=(
                    *problem.slots,
                    Slot(
                        key=key,
                        shift_id=shift.id,
                        project_id=shift.project_id,
                        shift_type_id=shift.shift_type_id,
                        starts_at=shift.starts_at,
                        ends_at=shift.ends_at,
                        zone=zone,
                        headcount=shift.headcount,
                        night=shift.shift_type_id in problem.night_types,
                        kept=tuple(on),
                    ),
                ),
            )
        problem = replace(problem, candidates=tuple(Employee(u.id, True) for u in staff))
        return cover_answer(problem, problem.current, key, staff)
