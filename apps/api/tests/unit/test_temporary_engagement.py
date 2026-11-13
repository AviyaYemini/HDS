"""A temporary worker's engagement: at most three months, on at least one project (CEO demo)."""

from datetime import date
from types import SimpleNamespace

import pytest

from hds.core.exceptions import DomainValidationError
from hds.services.temporary import add_months, as_employee, resolve

PROJECT = "11111111-1111-1111-1111-111111111111"


def test_three_calendar_months_end_on_a_shorter_month_s_last_day() -> None:
    assert add_months(date(2026, 11, 30), 3) == date(2027, 2, 28)
    assert add_months(date(2026, 1, 15), 3) == date(2026, 4, 15)


def test_a_temporary_worker_for_up_to_three_months_on_a_project_is_accepted() -> None:
    got = resolve(
        None,
        {
            "employment_type": "temporary",
            "engaged_from": date(2026, 11, 1),
            "engaged_until": date(2027, 2, 1),
            "project_ids": [PROJECT, PROJECT],
        },
    )
    assert got == {
        "employment_type": "temporary",
        "engaged_from": date(2026, 11, 1),
        "engaged_until": date(2027, 2, 1),
        "project_ids": [PROJECT],
    }


@pytest.mark.parametrize(
    ("first", "last", "code"),
    [
        (date(2026, 11, 1), date(2027, 2, 2), "employees.temporary_period_invalid"),
        (date(2026, 11, 10), date(2026, 11, 9), "employees.temporary_period_invalid"),
        (None, date(2026, 12, 1), "employees.temporary_period_invalid"),
    ],
)
def test_a_period_too_long_backwards_or_open_is_refused(first, last, code) -> None:
    with pytest.raises(DomainValidationError) as err:
        resolve(
            None,
            {
                "employment_type": "temporary",
                "engaged_from": first,
                "engaged_until": last,
                "project_ids": [PROJECT],
            },
        )
    assert err.value.code == code


def test_a_temporary_worker_needs_a_project() -> None:
    with pytest.raises(DomainValidationError) as err:
        resolve(
            None,
            {
                "employment_type": "temporary",
                "engaged_from": date(2026, 11, 1),
                "engaged_until": date(2026, 12, 1),
                "project_ids": [],
            },
        )
    assert err.value.code == "employees.temporary_projects_required"


def test_making_someone_permanent_clears_the_dates_and_projects() -> None:
    target = SimpleNamespace(
        employment_type="temporary",
        engaged_from=date(2026, 11, 1),
        engaged_until=date(2026, 12, 1),
        project_ids=[PROJECT],
    )
    assert resolve(target, {"employment_type": "permanent"}) == {
        "employment_type": "permanent",
        "engaged_from": None,
        "engaged_until": None,
        "project_ids": [],
    }


def test_an_edit_that_touches_nothing_of_the_engagement_changes_nothing() -> None:
    target = SimpleNamespace(employment_type="permanent")
    assert resolve(target, {"full_name": "Dana Levi"}) is None


def test_an_edit_keeps_what_it_does_not_send() -> None:
    target = SimpleNamespace(
        employment_type="temporary",
        engaged_from=date(2026, 11, 1),
        engaged_until=date(2026, 12, 1),
        project_ids=[PROJECT],
    )
    got = resolve(target, {"engaged_until": date(2027, 1, 31)})
    assert got is not None and got["project_ids"] == [PROJECT]
    assert got["engaged_until"] == date(2027, 1, 31)


def test_the_rules_see_a_temporary_worker_s_engagement_and_not_a_permanent_one_s() -> None:
    temp = SimpleNamespace(
        id="t",
        is_active=True,
        employment_type="temporary",
        engaged_from=date(2026, 11, 1),
        engaged_until=date(2026, 12, 1),
        project_ids=[PROJECT],
    )
    staff = SimpleNamespace(
        id="s",
        is_active=True,
        employment_type="permanent",
        engaged_from=None,
        engaged_until=None,
        project_ids=[],
    )
    assert as_employee(temp).engagement is not None
    assert as_employee(temp).engagement.covers(PROJECT, date(2026, 11, 15))
    assert as_employee(staff).engagement is None
