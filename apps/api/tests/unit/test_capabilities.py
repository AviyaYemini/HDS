"""The capability map equals its contract table and the route gates admit what they always did.

Covers: 007 FR-001, FR-002, FR-028 (specs/007-ux-redesign/contracts/capabilities.md, research R4,
task T027).
"""

import re
import uuid
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from hds.core import deps
from hds.core.capabilities import (
    ROLE_CAPABILITIES,
    Capability,
    capabilities_for,
    has,
    has_on_project,
)
from hds.core.exceptions import ForbiddenError
from hds.models.user import User, UserRole

TABLE = Path(__file__).parents[4] / "specs/007-ux-redesign/contracts/capabilities.md"
COLUMNS = (UserRole.employee, UserRole.manager, UserRole.admin, UserRole.client)


def contract_table() -> dict[UserRole, set[str]]:
    """Each role's capabilities as the contract's table states them.

    A cell grants the capability when it is not empty: "✓", "managed projects", "all" and
    "per 005 visibility" all mean the role holds it, scoped or not. A row may list several names.
    """
    granted: dict[UserRole, set[str]] = {role: set() for role in COLUMNS}
    for line in TABLE.read_text().splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 5 or not cells[0].startswith("`"):
            continue
        names = re.findall(r"`([a-z_.:]+)`", cells[0])
        for role, cell in zip(COLUMNS, cells[1:5], strict=True):
            if cell:
                granted[role].update(names)
    return granted


def test_every_role_holds_exactly_what_the_contract_table_says() -> None:
    table = contract_table()
    assert sum(len(v) for v in table.values()) > 20, "the table was not read"
    for role in COLUMNS:
        assert set(capabilities_for(role)) == table[role], role


def test_a_manager_inherits_every_employee_capability() -> None:
    assert ROLE_CAPABILITIES[UserRole.employee] <= ROLE_CAPABILITIES[UserRole.manager]
    assert ROLE_CAPABILITIES[UserRole.manager] <= ROLE_CAPABILITIES[UserRole.admin]


def test_a_client_holds_no_staff_capability() -> None:
    client = ROLE_CAPABILITIES[UserRole.client]
    assert Capability.MY_WORK_READ not in client
    assert Capability.ROSTER_READ not in client
    assert Capability.MY_WORK_AVAILABILITY not in client


def test_the_list_is_sorted_so_the_answer_is_stable() -> None:
    for role in COLUMNS:
        names = capabilities_for(role)
        assert names == sorted(names)


def test_only_an_admin_deletes_or_edits_pay_on_a_shift() -> None:
    for capability in (Capability.STAFF_DELETE, Capability.PAY_EDIT_SHIFT):
        assert [r for r in COLUMNS if has(r, capability)] == [UserRole.admin]


def test_a_managers_project_capabilities_cover_only_their_projects() -> None:
    mine, other = uuid.uuid4(), uuid.uuid4()
    edit = Capability.ROSTER_EDIT_PROJECT
    assert has_on_project(UserRole.manager, edit, mine, {mine})
    assert not has_on_project(UserRole.manager, edit, other, {mine})
    assert has_on_project(UserRole.admin, edit, other, set())
    assert not has_on_project(UserRole.employee, edit, mine, {mine})
    assert not has_on_project(UserRole.client, edit, mine, {mine})


def test_an_unscoped_capability_ignores_the_project() -> None:
    assert has_on_project(UserRole.manager, Capability.ROSTER_READ, uuid.uuid4(), set())


def _user(role: UserRole) -> User:
    user = MagicMock(spec=User)
    user.role = role
    return user


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("gate", "admitted", "code"),
    [
        (
            deps.require_staff,
            {UserRole.employee, UserRole.manager, UserRole.admin},
            "users.forbidden",
        ),
        (deps.require_manager, {UserRole.manager, UserRole.admin}, "users.manager_required"),
        (
            deps.require_manager_or_client,
            {UserRole.manager, UserRole.admin, UserRole.client},
            "users.manager_required",
        ),
        (deps.require_admin, {UserRole.admin}, "users.admin_required"),
    ],
)
async def test_each_route_gate_admits_the_same_roles_as_before(
    gate: object, admitted: set[UserRole], code: str
) -> None:
    for role in COLUMNS:
        user = _user(role)
        if role in admitted:
            assert await gate(user) is user  # type: ignore[operator]
        else:
            with pytest.raises(ForbiddenError) as refused:
                await gate(user)  # type: ignore[operator]
            assert refused.value.code == code
