"""An employee records their own availability inside an open window; absences any time (T043).

Covers: 003 FR-101 to FR-105a; US4.
"""

from datetime import timedelta
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.user import User
from tests.roster_support import (
    FIRST,
    LAST,
    body,
    close_window,
    constraints,
    headers_for,
    open_window,
    shift_type_id,
)


@pytest.fixture
async def window(async_client: AsyncClient, seed_users: dict[str, User]) -> Any:
    return await open_window(async_client, await headers_for(async_client, "manager"))


@pytest.fixture
async def employee(async_client: AsyncClient, seed_users: dict[str, User]) -> dict[str, str]:
    return await headers_for(async_client, "employee")


async def test_blocked_open_and_close_entries_are_stored_inside_an_open_window(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    window: Any,
    employee: dict[str, str],
) -> None:
    me = seed_users["employee"]
    morning = await shift_type_id(test_db, me.organisation_id)
    url = constraints(me.id)

    blocked = await async_client.post(url, json=body("blocked", FIRST), headers=employee)
    open_ = await async_client.post(
        url,
        json=body("open", FIRST + timedelta(days=1), shift_type_id=morning),
        headers=employee,
    )
    close = await async_client.post(
        url,
        json=body("close", FIRST + timedelta(days=2), LAST, weekday=3),
        headers=employee,
    )

    assert [r.status_code for r in (blocked, open_, close)] == [201, 201, 201], close.text
    listed = await async_client.get(url, headers=employee)
    assert sorted(e["kind"] for e in listed.json()["items"]) == ["blocked", "close", "open"]
    assert open_.json()["shift_type_id"] == morning
    assert close.json()["weekday"] == 3


async def test_an_entry_can_be_changed_and_removed_inside_an_open_window(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    window: Any,
    employee: dict[str, str],
) -> None:
    created = await async_client.post(
        constraints(seed_users["employee"].id), json=body(), headers=employee
    )
    url = f"/api/v1/constraints/{created.json()['id']}"

    changed = await async_client.patch(url, json={"kind": "close"}, headers=employee)
    removed = await async_client.delete(url, headers=employee)

    assert changed.status_code == 200, changed.text
    assert changed.json()["kind"] == "close"
    assert removed.status_code == 204


async def test_after_the_deadline_changes_are_refused_and_nothing_changes(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    window: Any,
    employee: dict[str, str],
) -> None:
    url = constraints(seed_users["employee"].id)
    created = (await async_client.post(url, json=body(), headers=employee)).json()
    await close_window(test_db, window["id"])
    entry = f"/api/v1/constraints/{created['id']}"

    added = await async_client.post(
        url, json=body("open", FIRST + timedelta(days=1)), headers=employee
    )
    changed = await async_client.patch(entry, json={"kind": "open"}, headers=employee)
    removed = await async_client.delete(entry, headers=employee)

    for refused in (added, changed, removed):
        assert refused.status_code == 409, refused.text
        assert refused.json()["code"] == "availability.window_closed"
    listed = (await async_client.get(url, headers=employee)).json()["items"]
    assert [(e["id"], e["kind"]) for e in listed] == [(created["id"], "blocked")]


async def test_dates_outside_any_window_are_refused(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    window: Any,
    employee: dict[str, str],
) -> None:
    url = constraints(seed_users["employee"].id)

    outside = await async_client.post(
        url, json=body(first=LAST + timedelta(days=1)), headers=employee
    )
    straddling = await async_client.post(
        url, json=body(first=LAST, last=LAST + timedelta(days=1)), headers=employee
    )

    for refused in (outside, straddling):
        assert refused.status_code == 409
        assert refused.json()["code"] == "availability.no_window"


async def test_an_entry_cannot_be_moved_out_of_its_open_window(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    window: Any,
    employee: dict[str, str],
) -> None:
    created = await async_client.post(
        constraints(seed_users["employee"].id), json=body(), headers=employee
    )

    moved = await async_client.patch(
        f"/api/v1/constraints/{created.json()['id']}",
        json={"starts_on": "2027-09-01", "ends_on": "2027-09-01"},
        headers=employee,
    )

    assert moved.status_code == 409
    assert moved.json()["code"] == "availability.no_window"


@pytest.mark.parametrize("kind", ["leave", "reserve_duty", "sick"])
async def test_an_absence_is_accepted_at_any_time(
    kind: str,
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    window: Any,
    employee: dict[str, str],
) -> None:
    await close_window(test_db, window["id"])
    url = constraints(seed_users["employee"].id)

    closed = await async_client.post(url, json=body("absence", absence_kind=kind), headers=employee)
    nowhere = await async_client.post(
        url, json=body("absence", FIRST + timedelta(days=60), absence_kind=kind), headers=employee
    )

    assert (closed.status_code, nowhere.status_code) == (201, 201), nowhere.text
    entry = f"/api/v1/constraints/{closed.json()['id']}"
    assert (await async_client.delete(entry, headers=employee)).status_code == 204


@pytest.mark.parametrize(
    ("extra", "code"),
    [
        ({"kind": "absence"}, "availability.absence_kind_invalid"),
        ({"kind": "blocked", "absence_kind": "sick"}, "availability.absence_kind_invalid"),
        (
            {"kind": "absence", "absence_kind": "sick", "weekday": 1},
            "availability.absence_whole_days",
        ),
        ({"ends_on": "2027-06-05"}, "availability.date_range_invalid"),
    ],
    ids=["absence-without-kind", "kind-on-blocked", "absence-on-a-weekday", "range-backwards"],
)
async def test_malformed_entries_are_refused(
    extra: dict[str, Any],
    code: str,
    async_client: AsyncClient,
    seed_users: dict[str, User],
    window: Any,
    employee: dict[str, str],
) -> None:
    refused = await async_client.post(
        constraints(seed_users["employee"].id), json={**body(), **extra}, headers=employee
    )

    assert refused.status_code == 422, refused.text
    assert refused.json()["code"] == code


@pytest.mark.parametrize("value", [{"kind": "maybe"}, {"weekday": 7}, {"absence_kind": "holiday"}])
async def test_unknown_values_are_refused_by_the_request_schema(
    value: dict[str, Any],
    async_client: AsyncClient,
    seed_users: dict[str, User],
    window: Any,
    employee: dict[str, str],
) -> None:
    refused = await async_client.post(
        constraints(seed_users["employee"].id), json={**body(), **value}, headers=employee
    )

    assert refused.status_code == 422
    assert refused.json()["code"] == "validation.request_invalid"


@pytest.mark.parametrize(
    ("first", "second"),
    [
        ({}, {"kind": "open"}),  # all day, then all day
        ({}, {"kind": "open", "shift": "morning"}),  # all day covers the morning
        ({"shift": "morning"}, {"kind": "close", "shift": "morning"}),
        (
            {"kind": "absence", "absence_kind": "sick"},
            {"kind": "absence", "absence_kind": "leave"},
        ),
        ({"last": 6, "weekday": 2}, {"kind": "open", "first": 2}),  # Tuesday 8 June
    ],
    ids=["day-day", "day-shift", "same-shift", "absence-absence", "weekday-date"],
)
async def test_two_entries_for_the_same_shift_and_date_conflict(
    first: dict[str, Any],
    second: dict[str, Any],
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    window: Any,
    employee: dict[str, str],
) -> None:
    me = seed_users["employee"]
    url = constraints(me.id)

    async def entry(spec: dict[str, Any]) -> dict[str, Any]:
        spec = dict(spec)
        extra: dict[str, Any] = {}
        if "shift" in spec:
            extra["shift_type_id"] = await shift_type_id(
                test_db, me.organisation_id, spec.pop("shift")
            )
        start = FIRST + timedelta(days=spec.pop("first", 0))
        end = FIRST + timedelta(days=spec.pop("last", 0)) if "last" in spec else start
        spec.pop("last", None)
        return body(spec.pop("kind", "blocked"), start, end, **spec, **extra)

    stored = await async_client.post(url, json=await entry(first), headers=employee)
    refused = await async_client.post(url, json=await entry(second), headers=employee)

    assert stored.status_code == 201, stored.text
    assert refused.status_code == 409, refused.text
    assert refused.json()["code"] == "availability.conflict"


@pytest.mark.parametrize(
    ("first", "second"),
    [
        ({"shift": "morning"}, {"kind": "open", "shift": "night"}),  # different shifts
        ({}, {"kind": "open", "first": 1}),  # different dates
        ({"last": 6, "weekday": 2}, {"kind": "open", "first": 3}),  # Wednesday is free
        # An absence may overlap blocked, open and close entries (owner decision, FR-104).
        ({"kind": "open", "shift": "night"}, {"kind": "absence", "absence_kind": "sick"}),
        ({"kind": "absence", "absence_kind": "sick"}, {"kind": "close"}),
    ],
    ids=["other-shift", "other-date", "other-weekday", "absence-over-open", "close-over-absence"],
)
async def test_entries_for_different_shifts_or_dates_do_not_conflict(
    first: dict[str, Any],
    second: dict[str, Any],
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    window: Any,
    employee: dict[str, str],
) -> None:
    me = seed_users["employee"]
    url = constraints(me.id)

    async def entry(spec: dict[str, Any]) -> dict[str, Any]:
        spec = dict(spec)
        extra: dict[str, Any] = {}
        if "shift" in spec:
            extra["shift_type_id"] = await shift_type_id(
                test_db, me.organisation_id, spec.pop("shift")
            )
        start = FIRST + timedelta(days=spec.pop("first", 0))
        end = FIRST + timedelta(days=spec.pop("last", 0)) if "last" in spec else start
        spec.pop("last", None)
        return body(spec.pop("kind", "blocked"), start, end, **spec, **extra)

    stored = await async_client.post(url, json=await entry(first), headers=employee)
    second_stored = await async_client.post(url, json=await entry(second), headers=employee)

    assert (stored.status_code, second_stored.status_code) == (201, 201), second_stored.text


async def test_changing_an_entry_does_not_conflict_with_itself(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    window: Any,
    employee: dict[str, str],
) -> None:
    created = await async_client.post(
        constraints(seed_users["employee"].id), json=body(), headers=employee
    )

    changed = await async_client.patch(
        f"/api/v1/constraints/{created.json()['id']}", json={"kind": "open"}, headers=employee
    )

    assert changed.status_code == 200, changed.text


async def test_a_shift_type_of_another_organisation_is_not_found(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    window: Any,
    employee: dict[str, str],
) -> None:
    refused = await async_client.post(
        constraints(seed_users["employee"].id),
        json=body(shift_type_id="00000000-0000-4000-8000-000000000000"),
        headers=employee,
    )

    assert refused.status_code == 404


async def test_an_entry_in_a_closed_window_cannot_be_moved_into_an_open_one(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    window: Any,
    employee: dict[str, str],
) -> None:
    """The window rule applies to the dates an entry has, not only the dates it gets."""
    manager = await headers_for(async_client, "manager")
    later = await open_window(
        async_client, manager, LAST + timedelta(days=1), LAST + timedelta(days=7)
    )
    created = await async_client.post(
        constraints(seed_users["employee"].id), json=body(), headers=employee
    )
    await close_window(test_db, window["id"])

    moved = await async_client.patch(
        f"/api/v1/constraints/{created.json()['id']}",
        json={"starts_on": later["starts_on"], "ends_on": later["starts_on"]},
        headers=employee,
    )

    assert moved.status_code == 409
    assert moved.json()["code"] == "availability.window_closed"


async def test_an_absence_can_be_reported_over_an_entry_in_a_closed_window(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    window: Any,
    employee: dict[str, str],
) -> None:
    """Sick on a day marked open after the deadline: the absence is stored and the entry stays."""
    url = constraints(seed_users["employee"].id)
    opened = await async_client.post(url, json=body("open"), headers=employee)
    await close_window(test_db, window["id"])

    sick = await async_client.post(url, json=body("absence", absence_kind="sick"), headers=employee)

    assert sick.status_code == 201, sick.text
    kinds = sorted(
        e["kind"] for e in (await async_client.get(url, headers=employee)).json()["items"]
    )
    assert kinds == ["absence", "open"]
    assert opened.status_code == 201


async def test_a_retired_shift_type_cannot_be_used(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    window: Any,
    employee: dict[str, str],
) -> None:
    me = seed_users["employee"]
    night = await shift_type_id(test_db, me.organisation_id, "night")
    await test_db.execute(
        text("UPDATE shift_types SET is_active = false WHERE id = :i"), {"i": night}
    )
    await test_db.commit()

    refused = await async_client.post(
        constraints(me.id), json=body(shift_type_id=night), headers=employee
    )

    assert refused.status_code == 422, refused.text
    assert refused.json()["code"] == "availability.shift_type_retired"


async def test_a_weekday_entry_needs_a_window_only_on_the_days_it_applies_to(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    window: Any,
    employee: dict[str, str],
) -> None:
    """Wednesdays from 6 to 15 June: only 9 June, inside the window; 13 to 15 June are not."""
    stored = await async_client.post(
        constraints(seed_users["employee"].id),
        json=body(first=FIRST, last=FIRST + timedelta(days=9), weekday=3),
        headers=employee,
    )

    assert stored.status_code == 201, stored.text


async def test_a_weekday_entry_that_covers_no_date_is_refused(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    window: Any,
    employee: dict[str, str],
) -> None:
    monday = FIRST + timedelta(days=1)

    refused = await async_client.post(
        constraints(seed_users["employee"].id),
        json=body(first=monday, last=monday, weekday=0),
        headers=employee,
    )

    assert refused.status_code == 422
    assert refused.json()["code"] == "availability.no_matching_day"


@pytest.mark.parametrize("value", ["0", "1780000000", "2027-6-6"])
async def test_a_date_must_be_written_as_year_month_day(
    value: str,
    async_client: AsyncClient,
    seed_users: dict[str, User],
    window: Any,
    employee: dict[str, str],
) -> None:
    refused = await async_client.post(
        constraints(seed_users["employee"].id),
        json={**body(), "starts_on": value},
        headers=employee,
    )

    assert refused.status_code == 422
    assert refused.json()["code"] == "validation.request_invalid"


async def test_a_closed_window_on_days_the_entry_does_not_cover_does_not_block_it(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    window: Any,
    employee: dict[str, str],
) -> None:
    """Wednesdays from 6 to 15 June cover only 9 June; 13 to 15 June are in a closed window."""
    manager = await headers_for(async_client, "manager")
    later = await open_window(
        async_client, manager, LAST + timedelta(days=1), LAST + timedelta(days=7)
    )
    await close_window(test_db, later["id"])

    stored = await async_client.post(
        constraints(seed_users["employee"].id),
        json=body(first=FIRST, last=FIRST + timedelta(days=9), weekday=3),
        headers=employee,
    )

    assert stored.status_code == 201, stored.text


async def test_an_absence_on_the_last_days_of_the_calendar_is_stored(
    async_client: AsyncClient, seed_users: dict[str, User], employee: dict[str, str]
) -> None:
    """The last dates there are are judged like any other, not answered with a 500 (FR-104)."""
    url = constraints(seed_users["employee"].id)
    late = {
        **body("absence", absence_kind="sick"),
        "starts_on": "9999-12-30",
        "ends_on": "9999-12-31",
    }

    answer = await async_client.post(url, json=late, headers=employee)

    assert answer.status_code == 201, answer.text
