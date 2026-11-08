"""The availability scheduling reads: what each employee submitted, not what they typed since.

Layer: services. Depends on ``hds.scheduling.types`` (plain dataclasses) and the standard library;
the loader at the bottom also takes the repositories it is given. It MUST NOT import routers or
know about HTTP. It holds no roster rule: it only decides which entries stand (research R6).

Inside a window that requires submission and whose deadline has passed (a *closed* window):

- an employee who submitted is judged by their snapshot, clipped to the window's dates, so a change
  they made after submitting and never submitted again does not count (clarification Q4);
- an employee who did not submit gets one block over every date of the window, with reason
  ``not_submitted``: the auto-scheduler leaves them out there, and a manual assignment warns
  (007 FR-014).

Absences, and every date outside closed windows, keep the live entries, as before 007.
"""

import hashlib
import json
import uuid
from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from hds.scheduling.types import Entry

NOT_SUBMITTED = "not_submitted"


@dataclass(frozen=True)
class ClosedWindow:
    """A window whose deadline has passed and that requires submission."""

    id: uuid.UUID
    starts_on: date
    ends_on: date


def clip(entry: Entry, first: date, last: date) -> Entry | None:
    """``entry`` limited to the dates ``first`` to ``last``, or ``None`` when they share none."""
    starts_on, ends_on = max(entry.starts_on, first), min(entry.ends_on, last)
    if starts_on > ends_on:
        return None
    return Entry(
        entry.employee_id,
        entry.kind,
        starts_on,
        ends_on,
        entry.shift_type_id,
        entry.weekday,
        entry.reason,
    )


def outside(entry: Entry, windows: Sequence[ClosedWindow]) -> list[Entry]:
    """The parts of ``entry`` that fall on no date of ``windows`` (which never overlap)."""
    parts: list[Entry] = []
    cursor = entry.starts_on
    for window in sorted(windows, key=lambda w: w.starts_on):
        if window.ends_on < cursor or window.starts_on > entry.ends_on:
            continue
        before = clip(entry, cursor, window.starts_on - timedelta(days=1))
        if before is not None:
            parts.append(before)
        cursor = window.ends_on + timedelta(days=1)
    rest = clip(entry, cursor, entry.ends_on)
    if rest is not None:
        parts.append(rest)
    return parts


def to_json(entry: Entry) -> dict[str, Any]:
    """One snapshot item: an entry without its employee, in canonical form."""
    return {
        "kind": entry.kind,
        "starts_on": entry.starts_on.isoformat(),
        "ends_on": entry.ends_on.isoformat(),
        "shift_type_id": str(entry.shift_type_id) if entry.shift_type_id else None,
        "weekday": entry.weekday,
    }


def from_json(employee_id: uuid.UUID, item: Mapping[str, Any]) -> Entry:
    """The entry a snapshot item stands for, for ``employee_id``."""
    shift_type = item.get("shift_type_id")
    return Entry(
        employee_id,
        item["kind"],
        date.fromisoformat(item["starts_on"]),
        date.fromisoformat(item["ends_on"]),
        uuid.UUID(shift_type) if shift_type else None,
        item.get("weekday"),
    )


def snapshot(entries: Iterable[Entry], first: date, last: date) -> list[dict[str, Any]]:
    """The submitted form of an employee's entries for a window's dates, sorted.

    Only blocked, open and close entries; each clipped to ``first`` to ``last``.
    """
    items = []
    for entry in entries:
        if entry.kind == "absence":
            continue
        clipped = clip(entry, first, last)
        if clipped is not None:
            items.append(to_json(clipped))
    return canonical(items)


def canonical(items: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Snapshot items in the one order the hash is taken over."""
    return sorted((dict(i) for i in items), key=lambda i: json.dumps(i, sort_keys=True))


def patched(
    items: Sequence[Mapping[str, Any]],
    removed: Entry | None,
    added: Entry | None,
    first: date,
    last: date,
) -> list[dict[str, Any]]:
    """A snapshot with exactly one change applied: ``removed`` taken out, ``added`` put in.

    Both are clipped to the window's dates ``first`` to ``last``, as the snapshot's items are. An
    item that is not in the snapshot (the employee changed it after submitting) is not removed;
    nothing else in the snapshot is touched (007 clarification Q4).
    """
    result = [dict(i) for i in items]
    if removed is not None:
        gone = clip(removed, first, last)
        if gone is not None and to_json(gone) in result:
            result.remove(to_json(gone))
    if added is not None:
        new = clip(added, first, last)
        if new is not None:
            result.append(to_json(new))
    return canonical(result)


def snapshot_hash(items: Sequence[Mapping[str, Any]]) -> str:
    """The SHA-256 of a snapshot in canonical form."""
    return hashlib.sha256(json.dumps(list(items), sort_keys=True).encode()).hexdigest()


def effective_entries(
    live: Iterable[Entry],
    user_ids: Collection[uuid.UUID],
    closed: Sequence[ClosedWindow],
    snapshots: Mapping[tuple[uuid.UUID, uuid.UUID], Sequence[Mapping[str, Any]]],
) -> list[Entry]:
    """The entries scheduling judges ``user_ids`` by.

    Args:
        live: The employees' stored entries.
        user_ids: Every employee being judged, with or without entries.
        closed: Closed windows that require submission.
        snapshots: ``(window id, user id)`` to that employee's submitted snapshot.

    Returns:
        Absences as stored; other live entries only on dates outside closed windows; in each
        closed window the employee's snapshot, or a ``not_submitted`` block when there is none.
    """
    result: list[Entry] = []
    for entry in live:
        if entry.kind == "absence":
            result.append(entry)
        else:
            result.extend(outside(entry, closed))
    for window in closed:
        for user_id in sorted(user_ids, key=str):
            submitted = snapshots.get((window.id, user_id))
            if submitted is None:
                result.append(
                    Entry(
                        user_id,
                        "blocked",
                        window.starts_on,
                        window.ends_on,
                        None,
                        None,
                        NOT_SUBMITTED,
                    )
                )
            else:
                for item in submitted:
                    clipped = clip(from_json(user_id, item), window.starts_on, window.ends_on)
                    if clipped is not None:
                        result.append(clipped)
    return result
