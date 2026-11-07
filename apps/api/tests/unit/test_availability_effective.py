"""Which availability scheduling reads once a window closes (007 FR-014, research R6, task T049).

Covers: snapshots replace the live entries inside closed windows, a ``not_submitted`` block stands
for an employee who did not submit, absences and dates outside closed windows keep the live
entries, and the snapshot hash tells Submitted from Draft.
"""

import uuid
from datetime import date, timedelta

from hypothesis import given, settings
from hypothesis import strategies as st

from hds.scheduling.types import Entry
from hds.services.availability_effective import (
    NOT_SUBMITTED,
    ClosedWindow,
    effective_entries,
    outside,
    snapshot,
    snapshot_hash,
)

ANN, BOB = uuid.uuid4(), uuid.uuid4()
WINDOW = ClosedWindow(uuid.uuid4(), date(2026, 11, 8), date(2026, 11, 14))


def entry(kind: str, first: date, last: date, who: uuid.UUID = ANN) -> Entry:
    return Entry(who, kind, first, last, None, None)  # type: ignore[arg-type]


def test_an_entry_across_a_closed_window_keeps_only_its_outside_parts() -> None:
    long = entry("blocked", date(2026, 11, 1), date(2026, 11, 20))
    parts = outside(long, [WINDOW])
    assert [(p.starts_on, p.ends_on) for p in parts] == [
        (date(2026, 11, 1), date(2026, 11, 7)),
        (date(2026, 11, 15), date(2026, 11, 20)),
    ]


def test_someone_who_did_not_submit_is_blocked_for_the_whole_window() -> None:
    result = effective_entries([], [ANN], [WINDOW], {})
    assert result == [
        Entry(ANN, "blocked", WINDOW.starts_on, WINDOW.ends_on, None, None, NOT_SUBMITTED)
    ]


def test_a_submitter_is_judged_by_the_snapshot_not_by_later_changes() -> None:
    submitted = snapshot(
        [entry("close", date(2026, 11, 9), date(2026, 11, 9))], WINDOW.starts_on, WINDOW.ends_on
    )
    changed_since = [entry("blocked", date(2026, 11, 10), date(2026, 11, 10))]

    result = effective_entries(changed_since, [ANN], [WINDOW], {(WINDOW.id, ANN): submitted})

    assert result == [entry("close", date(2026, 11, 9), date(2026, 11, 9))]


def test_absences_always_count_and_never_enter_a_snapshot() -> None:
    sick = entry("absence", date(2026, 11, 10), date(2026, 11, 11))
    assert snapshot([sick], WINDOW.starts_on, WINDOW.ends_on) == []
    result = effective_entries([sick], [ANN], [WINDOW], {(WINDOW.id, ANN): []})
    assert result == [sick]


def test_without_closed_windows_the_live_entries_stand() -> None:
    live = [
        entry("open", date(2026, 11, 9), date(2026, 11, 9)),
        entry("blocked", date(2026, 11, 2), date(2026, 11, 2), BOB),
    ]
    assert effective_entries(live, [ANN, BOB], [], {}) == live


def test_the_hash_tells_a_changed_week_from_the_submitted_one() -> None:
    first = snapshot(
        [entry("open", date(2026, 11, 9), date(2026, 11, 9))], WINDOW.starts_on, WINDOW.ends_on
    )
    same = snapshot(
        [entry("open", date(2026, 11, 9), date(2026, 11, 9))], WINDOW.starts_on, WINDOW.ends_on
    )
    other = snapshot(
        [entry("close", date(2026, 11, 9), date(2026, 11, 9))], WINDOW.starts_on, WINDOW.ends_on
    )
    assert snapshot_hash(first) == snapshot_hash(same)
    assert snapshot_hash(first) != snapshot_hash(other)


DAYS = st.integers(min_value=0, max_value=27)
KINDS = st.sampled_from(["blocked", "open", "close", "absence"])


@st.composite
def entries(draw: st.DrawFn) -> Entry:
    start = draw(DAYS)
    length = draw(st.integers(min_value=0, max_value=10))
    first = date(2026, 11, 1) + timedelta(days=start)
    who = draw(st.sampled_from([ANN, BOB]))
    return entry(draw(KINDS), first, first + timedelta(days=length), who)


@given(st.lists(entries(), max_size=12), st.booleans())
@settings(max_examples=200)
def test_property_inside_a_closed_window_only_snapshots_blocks_and_absences_stand(
    live: list[Entry], ann_submitted: bool
) -> None:
    """Only snapshots, blocks and absences stand inside a closed window.

    No live blocked, open or close entry survives there, a non-submitter is blocked on every one
    of its dates, and outside it every live entry's dates are kept.
    """
    snapshots: dict[tuple[uuid.UUID, uuid.UUID], list[dict[str, object]]] = (
        {(WINDOW.id, ANN): []} if ann_submitted else {}
    )
    result = effective_entries(live, [ANN, BOB], [WINDOW], snapshots)

    inside = [
        e
        for e in result
        if e.kind != "absence"
        and e.reason is None
        and e.starts_on <= WINDOW.ends_on
        and e.ends_on >= WINDOW.starts_on
    ]
    assert inside == []  # Ann's snapshot is empty; Bob never submitted
    bob_blocks = [e for e in result if e.employee_id == BOB and e.reason == NOT_SUBMITTED]
    assert [(b.starts_on, b.ends_on) for b in bob_blocks] == [(WINDOW.starts_on, WINDOW.ends_on)]
    assert any(e.employee_id == ANN and e.reason == NOT_SUBMITTED for e in result) != ann_submitted

    def covered(e: Entry) -> set[date]:
        return {e.starts_on + timedelta(days=i) for i in range((e.ends_on - e.starts_on).days + 1)}

    window_days = covered(entry("open", WINDOW.starts_on, WINDOW.ends_on))
    for original in live:
        if original.kind == "absence":
            assert original in result
            continue
        kept = (
            set().union(
                *(
                    covered(e)
                    for e in result
                    if e.reason is None
                    and e.kind == original.kind
                    and e.employee_id == original.employee_id
                )
            )
            if result
            else set()
        )
        assert covered(original) - window_days <= kept
