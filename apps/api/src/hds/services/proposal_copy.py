"""A proposal's working copy: its shape, its change marks, and the pairs still open (008, ADR 020).

Layer: services. Depends on ``hds.scheduling`` (pure) only; it MUST NOT import repositories,
routers or anything that does I/O. Every function here takes and returns plain JSON-shaped data, so
the services that store the copy and the tests that check it share one definition.

The copy has the stored proposal's keys (004 data-model ``result``), with each slot holding:

- ``run_before``: the slot's candidates on the roster when the run was made (for the change marks);
- ``stored``: the slot's candidates on the roster as far as this proposal knows, starting as
  ``run_before`` and moving with each part accepted;
- ``people``: who the proposal puts there now, each ``{"user_id", "by", "locked", "override"}``,
  where ``by`` is ``solver`` or ``person`` and ``override`` is ``{"codes", "reason"}`` or null.

A pair (slot, person) is **open** while ``people`` and ``stored`` disagree on it, and **accepted**
once it differed from ``run_before`` and they agree. The proposal is wholly accepted when no pair is
open.
"""

import copy
import uuid
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any

from hds.scheduling import Employee, Problem, RuleWarning, ShiftSlot
from hds.scheduling.engine import slot_id

Copy = dict[str, Any]
SlotCopy = dict[str, Any]

# Rule breaks a person may keep in a proposal only with a reason (research P13). An inactive
# employee is refused outright; a "would rather not" and a short-staffed slot are shown, not asked.
NEEDS_REASON = frozenset(
    (
        "warnings.overlap",
        "warnings.rest_too_short",
        "warnings.weekly_hours_exceeded",
        "warnings.blocked",
        "warnings.absence",
        "warnings.not_submitted",
        "warnings.over_headcount",
    )
)
REFUSED = frozenset(("warnings.employee_inactive",))


def initial(result: Mapping[str, Any]) -> Copy:
    """The working copy of a solver's result, before anyone changed it.

    ``run_before`` is the result's people less what it added, plus what it removed, so it is the
    roster's candidates when the run was made.
    """
    added: dict[str, set[str]] = {}
    removed: dict[str, set[str]] = {}
    for item in result.get("changes", {}).get("added", []):
        added.setdefault(item["key"], set()).add(item["user_id"])
    for item in result.get("changes", {}).get("removed", []):
        removed.setdefault(item["key"], set()).add(item["user_id"])
    out = copy.deepcopy(dict(result))
    slots = []
    for slot in result.get("slots", []):
        key = slot["key"]
        before = (set(slot["assigned"]) - added.get(key, set())) | removed.get(key, set())
        item = {k: v for k, v in slot.items() if k != "assigned"}
        item["run_before"] = sorted(before)
        item["stored"] = sorted(before)
        item["people"] = [
            {"user_id": p, "by": "solver", "locked": False, "override": None}
            for p in slot["assigned"]
        ]
        slots.append(item)
    out["slots"] = slots
    return out


def of(job_current: Mapping[str, Any] | None, job_result: Mapping[str, Any] | None) -> Copy:
    """The job's working copy: the stored one, or the result's when no one has changed it yet."""
    if job_current is not None:
        return copy.deepcopy(dict(job_current))
    return initial(job_result or {"slots": []})


def people(slot: Mapping[str, Any]) -> list[str]:
    """Who the copy puts on ``slot`` now, as user id strings, in order."""
    return [p["user_id"] for p in slot["people"]]


def open_pairs(slot: Mapping[str, Any]) -> tuple[set[str], set[str]]:
    """The slot's open pairs: people to add to the roster, and people to remove from it."""
    now, stored = set(people(slot)), set(slot["stored"])
    return now - stored, stored - now


def is_settled(work: Mapping[str, Any]) -> bool:
    """True when no pair is open anywhere: everything the proposal changes is accepted."""
    return all(not any(open_pairs(s)) for s in work["slots"])


def any_accepted(work: Mapping[str, Any]) -> bool:
    """True when at least one pair has been accepted."""
    for slot in work["slots"]:
        now, before, stored = set(people(slot)), set(slot["run_before"]), set(slot["stored"])
        changed = (now ^ before) - (now ^ stored)
        if changed:
            return True
    return False


def marks(slot: Mapping[str, Any]) -> list[dict[str, Any]]:
    """The slot's change marks against the roster when the run was made (FR-001).

    Each person now on the slot or removed from it gets one mark: ``unchanged``, ``added``,
    ``removed``, or ``changed`` (the slot given to another person: one added person paired with one
    removed person, in id order). Each mark says whether its pair is accepted, or has nothing to
    accept (unchanged).
    """
    now, before = people(slot), set(slot["run_before"])
    stored = set(slot["stored"])
    added = sorted(p for p in now if p not in before)
    removed = sorted(p for p in before if p not in now)
    found: list[dict[str, Any]] = []
    for p in now:
        if p in before:
            found.append({"mark": "unchanged", "user_id": p, "replaces": None, "accepted": True})
    for new, old in zip(added, removed, strict=False):
        found.append(
            {
                "mark": "changed",
                "user_id": new,
                "replaces": old,
                "accepted": new in stored and old not in stored,
            }
        )
    for new in added[len(removed) :]:
        found.append({"mark": "added", "user_id": new, "replaces": None, "accepted": new in stored})
    for old in removed[len(added) :]:
        found.append(
            {"mark": "removed", "user_id": old, "replaces": None, "accepted": old not in stored}
        )
    return found


def counts(work: Mapping[str, Any]) -> dict[str, int]:
    """How many marks of each kind the whole copy holds."""
    total = dict.fromkeys(("added", "changed", "removed", "unchanged"), 0)
    for slot in work["slots"]:
        for m in marks(slot):
            total[m["mark"]] += 1
    return total


def places(slot: Mapping[str, Any]) -> int:
    """The slot's empty places in the copy: its headcount less kept people and the copy's people."""
    return max(0, int(slot["headcount"]) - len(slot.get("kept", [])) - len(slot["people"]))


def find(work: Mapping[str, Any], key: str) -> SlotCopy | None:
    """The copy's slot with ``key``, or ``None``."""
    for slot in work["slots"]:
        if slot["key"] == key:
            return slot  # type: ignore[no-any-return]
    return None


def shift_id_of(slot: Mapping[str, Any]) -> uuid.UUID:
    """The id the rules judge the slot by: its draft's, or the stable id of a new slot."""
    if slot.get("shift_id"):
        return uuid.UUID(slot["shift_id"])
    return slot_id(slot["key"])


def as_shift(slot: Mapping[str, Any], zone: str) -> ShiftSlot:
    """The slot as the rules see it: kept people and the copy's people on it."""
    on = tuple(uuid.UUID(p) for p in [*slot.get("kept", []), *people(slot)])
    return ShiftSlot(
        id=shift_id_of(slot),
        project_id=uuid.UUID(slot["project_id"]),
        shift_type_id=uuid.UUID(slot["shift_type_id"]),
        starts_at=datetime.fromisoformat(slot["starts_at"]),
        ends_at=datetime.fromisoformat(slot["ends_at"]),
        zone=zone,
        headcount=slot["headcount"],
        assigned=on,
    )


def selected(
    work: Mapping[str, Any],
    *,
    days: Iterable[str] = (),
    project_ids: Iterable[str] = (),
    pairs: Iterable[tuple[str, str]] = (),
) -> dict[str, set[str]]:
    """The open pairs a part names, by slot key: all of them when the part names nothing.

    ``days`` are ISO dates and ``project_ids`` id strings; ``pairs`` are ``(key, user_id)``. A part
    takes the union of what it names; a named pair that is not open is ignored. Naming either
    person of a ``changed`` mark takes both, so a slot given to another person never keeps the
    one it replaces as well.
    """
    days, project_ids, pairs = set(days), set(project_ids), set(pairs)
    everything = not (days or project_ids or pairs)
    chosen: dict[str, set[str]] = {}
    for slot in work["slots"]:
        add, drop = open_pairs(slot)
        open_now = add | drop
        if not open_now:
            continue
        whole = everything or slot["date"] in days or slot["project_id"] in project_ids
        take = open_now if whole else {u for k, u in pairs if k == slot["key"] and u in open_now}
        for mark in marks(slot) if take and not whole else ():
            pair = {mark["user_id"], mark["replaces"]}
            if mark["mark"] == "changed" and pair & take:
                take |= pair & open_now
        if take:
            chosen[slot["key"]] = take
    return chosen


def shifts(
    work: Mapping[str, Any], zone: str, layout: Mapping[str, list[str]] | None = None
) -> list[ShiftSlot]:
    """Every slot of the copy as the rules see it; ``layout`` replaces the people of some slots.

    A slot's zone is its own when the proposal stored one (from 008), else ``zone``, the
    organisation's.
    """
    found = []
    for slot in work["slots"]:
        shown = dict(slot)
        if layout is not None and slot["key"] in layout:
            shown["people"] = [{"user_id": p} for p in layout[slot["key"]]]
        found.append(as_shift(shown, slot.get("zone") or zone))
    return found


def unlisted(problem: Problem, work: Mapping[str, Any], ours: set[uuid.UUID]) -> list[ShiftSlot]:
    """Slots of a rebuilt problem the copy does not hold (a draft made by hand since), as shifts."""
    found = []
    for s in problem.slots:
        sid = s.shift_id or slot_id(s.key)
        if sid in ours:
            continue
        on = (*problem.current.get(s.key, ()), *s.kept)
        if not on:
            continue
        found.append(
            ShiftSlot(
                id=sid,
                project_id=s.project_id,
                shift_type_id=s.shift_type_id,
                starts_at=s.starts_at,
                ends_at=s.ends_at,
                zone=s.zone,
                headcount=s.headcount,
                assigned=tuple(on),
            )
        )
    return found


def everyone(
    problem: Problem,
    all_shifts: Iterable[ShiftSlot],
    known: Mapping[uuid.UUID, Employee] | None = None,
) -> dict[uuid.UUID, Employee]:
    """Every employee on ``all_shifts``, as the rules need them.

    Candidates as the problem has them; ``known`` (read by the service) for anyone else; and,
    failing both, someone kept on a stored shift counts as active, because only the people being
    placed are judged (``unexcused``) and they are always known.
    """
    found: dict[uuid.UUID, Employee] = {e.id: e for e in problem.candidates}
    found.update(known or {})
    for shift in all_shifts:
        for person in shift.assigned:
            found.setdefault(person, Employee(person, True))
    return found


def unexcused(
    found: Iterable[RuleWarning], work: Mapping[str, Any], who: Mapping[str, set[str]]
) -> list[dict[str, Any]]:
    """The rule breaks of the people in ``who`` (slot key to user ids) that no override excuses.

    A break counts when its code needs a reason or is refused, it concerns one of those people on
    their slot, and the person's override there does not name it. A headcount break of the slot
    counts unless one of them overrides it.
    """
    slots = {s["key"]: s for s in work["slots"]}
    by_id = {shift_id_of(s): s["key"] for s in work["slots"]}
    causes: list[dict[str, Any]] = []
    for warning in found:
        if warning.code not in NEEDS_REASON and warning.code not in REFUSED:
            continue
        for sid in warning.shift_ids:
            key = by_id.get(sid)
            if key is None or key not in who:
                continue
            slot = slots[key]
            overridden = {
                p["user_id"]: set((p.get("override") or {}).get("codes", ()))
                for p in slot["people"]
            }
            if warning.employee_id is None:
                if any(warning.code in overridden.get(p, set()) for p in who[key]):
                    continue
                person = None
            else:
                person = str(warning.employee_id)
                if person not in who[key]:
                    continue
                if warning.code in overridden.get(person, set()) and warning.code not in REFUSED:
                    continue
            causes.append(
                {
                    "key": key,
                    "date": slot["date"],
                    "user_id": person,
                    "code": warning.code,
                    "values": dict(warning.values),
                    "shift_ids": [str(i) for i in warning.shift_ids],
                }
            )
            break
    return causes
