"""Unit tests for EmployeeService manager operations (T053, T143).

Roles, last-admin protection and session revocation.

Covers: FR-011, FR-011a, FR-012, FR-014, FR-015, FR-016, FR-028, NFR-005.
"""

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from hds.core.exceptions import ConflictError, DomainValidationError, ForbiddenError
from hds.models.user import User, UserRole
from hds.services.employee import EmployeeService


def _make_user(**kwargs: object) -> User:
    user = MagicMock(spec=User)
    user.id = kwargs.get("id", uuid.uuid4())
    user.organisation_id = kwargs.get("org_id", uuid.uuid4())
    user.email = kwargs.get("email", "emp@hds.test")
    user.full_name = kwargs.get("full_name", "Test Employee")
    user.role = kwargs.get("role", UserRole.employee)
    user.is_active = kwargs.get("is_active", True)
    user.is_deleted = kwargs.get("is_deleted", False)
    # Most test accounts have been active at some point; a pending one passes activated_at=None.
    user.activated_at = kwargs.get("activated_at", datetime.now(UTC))
    return user


def _svc(user_repo: AsyncMock | None = None) -> tuple[EmployeeService, AsyncMock, AsyncMock]:
    user_repo = user_repo or AsyncMock()
    token_repo = AsyncMock()
    svc = EmployeeService(
        user_repo=user_repo,
        token_repo=token_repo,
        link_repo=AsyncMock(),
        group_repo=AsyncMock(),
        qualification_repo=AsyncMock(),
        client_repo=AsyncMock(),
        project_manager_repo=AsyncMock(),
    )
    return svc, user_repo, token_repo


_VALID = {
    "email": "new@hds.test",
    "full_name": "New Emp",
    "initial_password": "correct-horse-battery",
}


# ── create ──────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_create_employee_success_hashes_password() -> None:
    actor = _make_user(role=UserRole.manager)
    svc, user_repo, _ = _svc()
    user_repo.get_by_email = AsyncMock(return_value=None)
    created = _make_user()
    user_repo.create_if_absent = AsyncMock(return_value=(created, True))

    result = await svc.create_employee(actor, dict(_VALID))

    assert result is created
    org_id, data = user_repo.create_if_absent.await_args.args
    assert org_id == actor.organisation_id
    assert "initial_password" not in data
    assert data["password_hash"].startswith("$argon2id$")
    assert data["role"] == UserRole.employee


@pytest.mark.asyncio
async def test_create_employee_by_employee_forbidden() -> None:
    svc, user_repo, _ = _svc()
    with pytest.raises(ForbiddenError) as exc:
        await svc.create_employee(_make_user(role=UserRole.employee), dict(_VALID))
    assert exc.value.code == "users.manager_required"
    user_repo.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_employee_duplicate_email_raises_conflict() -> None:
    svc, user_repo, _ = _svc()
    user_repo.get_by_email = AsyncMock(return_value=_make_user())
    with pytest.raises(ConflictError) as exc:
        await svc.create_employee(_make_user(role=UserRole.manager), dict(_VALID))
    assert exc.value.code == "users.duplicate_email"


@pytest.mark.asyncio
async def test_create_employee_short_initial_password_rejected() -> None:
    svc, user_repo, _ = _svc()
    user_repo.get_by_email = AsyncMock(return_value=None)
    with pytest.raises(DomainValidationError) as exc:
        await svc.create_employee(
            _make_user(role=UserRole.manager), {**_VALID, "initial_password": "short"}
        )
    assert exc.value.code == "users.password_too_short"
    user_repo.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_employee_that_loses_the_address_during_the_hash_is_a_duplicate() -> None:
    actor = _make_user(role=UserRole.manager)
    svc, user_repo, _ = _svc()
    user_repo.get_by_email = AsyncMock(return_value=None)
    user_repo.create_if_absent = AsyncMock(return_value=(_make_user(), False))

    with pytest.raises(ConflictError) as refused:
        await svc.create_employee(actor, dict(_VALID))

    assert refused.value.code == "users.duplicate_email"


@pytest.mark.asyncio
async def test_create_employee_password_of_exactly_12_chars_accepted() -> None:
    svc, user_repo, _ = _svc()
    user_repo.get_by_email = AsyncMock(return_value=None)
    user_repo.create_if_absent = AsyncMock(return_value=(_make_user(), True))
    await svc.create_employee(
        _make_user(role=UserRole.manager), {**_VALID, "initial_password": "x" * 12}
    )
    user_repo.create_if_absent.assert_awaited_once()


@pytest.mark.parametrize("rate", [Decimal("0"), Decimal("-1")])
@pytest.mark.asyncio
async def test_create_employee_non_positive_hourly_rate_rejected(rate: Decimal) -> None:
    svc, user_repo, _ = _svc()
    with pytest.raises(DomainValidationError) as exc:
        # An Admin: since 005 FR-001 a Manager may not send a rate at all.
        await svc.create_employee(
            _make_user(role=UserRole.admin), {**_VALID, "default_hourly_rate": rate}
        )
    assert exc.value.code == "users.hourly_rate_invalid"


@pytest.mark.asyncio
async def test_create_employee_with_client_role_rejected() -> None:
    svc, _, _ = _svc()
    with pytest.raises(DomainValidationError) as exc:
        await svc.create_employee(
            _make_user(role=UserRole.admin), {**_VALID, "role": UserRole.client}
        )
    assert exc.value.code == "users.role_invalid"


@pytest.mark.asyncio
async def test_manager_cannot_create_admin() -> None:
    svc, _, _ = _svc()
    with pytest.raises(ForbiddenError) as exc:
        await svc.create_employee(
            _make_user(role=UserRole.manager), {**_VALID, "role": UserRole.admin}
        )
    assert exc.value.code == "users.admin_required"


@pytest.mark.asyncio
async def test_a_manager_promoting_to_manager_by_edit_ends_the_sessions() -> None:
    """Covers what the removed ``promote_to_manager`` tests pinned (003 Group C)."""
    actor = _make_user(role=UserRole.manager)
    target = _make_user(role=UserRole.employee)
    svc, user_repo, token_repo = _svc()
    user_repo.get_by_id = AsyncMock(return_value=target)
    user_repo.update = AsyncMock(return_value=_make_user(role=UserRole.manager))

    result = await svc.edit_employee(actor, target.id, {"role": "manager"})

    assert result.role == UserRole.manager
    user_repo.update.assert_awaited_once_with(
        target.id, actor.organisation_id, role=UserRole.manager
    )
    token_repo.revoke_all_for_user.assert_awaited_once_with(target.id, "account")


@pytest.mark.asyncio
async def test_an_admin_making_an_admin_by_edit_ends_the_sessions() -> None:
    """Covers what the removed ``promote_to_admin`` tests pinned (003 Group C)."""
    actor = _make_user(role=UserRole.admin)
    target = _make_user(role=UserRole.employee)
    svc, user_repo, token_repo = _svc()
    user_repo.get_by_id = AsyncMock(return_value=target)
    user_repo.update = AsyncMock(return_value=_make_user(role=UserRole.admin))

    await svc.edit_employee(actor, target.id, {"role": "admin"})

    user_repo.update.assert_awaited_once_with(target.id, actor.organisation_id, role=UserRole.admin)
    token_repo.revoke_all_for_user.assert_awaited_once_with(target.id, "account")


# ── FR-011a: only an Admin may change an Admin ─────────────────────────────


@pytest.mark.parametrize("data", [{"full_name": "X"}, {"role": "manager"}], ids=["name", "role"])
@pytest.mark.asyncio
async def test_manager_cannot_edit_admin(data: dict[str, str]) -> None:
    target = _make_user(role=UserRole.admin)
    svc, user_repo, _ = _svc()
    user_repo.get_by_id = AsyncMock(return_value=target)
    with pytest.raises(ForbiddenError) as refused:
        await svc.edit_employee(_make_user(role=UserRole.manager), target.id, data)
    assert refused.value.code == "users.admin_required"
    user_repo.update.assert_not_awaited()


@pytest.mark.asyncio
async def test_manager_cannot_deactivate_admin() -> None:
    target = _make_user(role=UserRole.admin)
    svc, user_repo, _ = _svc()
    user_repo.get_by_id = AsyncMock(return_value=target)
    with pytest.raises(ForbiddenError):
        await svc.deactivate(_make_user(role=UserRole.manager), target.id)


@pytest.mark.asyncio
async def test_manager_cannot_reactivate_admin() -> None:
    target = _make_user(role=UserRole.admin, is_active=False)
    svc, user_repo, _ = _svc()
    user_repo.get_by_id = AsyncMock(return_value=target)
    with pytest.raises(ForbiddenError):
        await svc.reactivate(_make_user(role=UserRole.manager), target.id)


@pytest.mark.asyncio
async def test_manager_cannot_set_role_admin_via_edit() -> None:
    target = _make_user(role=UserRole.employee)
    svc, user_repo, _ = _svc()
    user_repo.get_by_id = AsyncMock(return_value=target)
    with pytest.raises(ForbiddenError) as refused:
        await svc.edit_employee(_make_user(role=UserRole.manager), target.id, {"role": "admin"})
    assert refused.value.code == "users.admin_required"
    user_repo.update.assert_not_awaited()


# ── deactivate / reactivate ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_deactivate_revokes_all_sessions() -> None:
    actor = _make_user(role=UserRole.manager)
    target = _make_user(role=UserRole.employee)
    svc, user_repo, token_repo = _svc()
    user_repo.get_by_id = AsyncMock(return_value=target)
    user_repo.update = AsyncMock(return_value=_make_user(is_active=False))

    await svc.deactivate(actor, target.id)

    user_repo.update.assert_awaited_once_with(target.id, actor.organisation_id, is_active=False)
    token_repo.revoke_all_for_user.assert_awaited_once_with(target.id, "account")


@pytest.mark.asyncio
async def test_reactivate_employee() -> None:
    actor = _make_user(role=UserRole.manager)
    target = _make_user(is_active=False)
    svc, user_repo, _ = _svc()
    user_repo.get_by_id = AsyncMock(return_value=target)
    user_repo.update = AsyncMock(return_value=_make_user(is_active=True))
    await svc.reactivate(actor, target.id)
    user_repo.update.assert_awaited_once_with(target.id, actor.organisation_id, is_active=True)


# ── FR-011: last active Admin ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_soft_delete_last_admin_raises_conflict() -> None:
    org_id = uuid.uuid4()
    actor = _make_user(role=UserRole.admin, org_id=org_id)
    target = _make_user(role=UserRole.admin, org_id=org_id)
    svc, user_repo, _ = _svc()
    user_repo.get_by_id = AsyncMock(return_value=target)
    user_repo.lock_active_admins = AsyncMock(return_value=[uuid.uuid4() for _ in range(1)])

    with pytest.raises(ConflictError) as exc:
        await svc.soft_delete(actor, target.id)

    assert exc.value.code == "users.last_admin"
    user_repo.soft_delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_soft_delete_non_last_admin_succeeds_and_revokes() -> None:
    org_id = uuid.uuid4()
    actor = _make_user(role=UserRole.admin, org_id=org_id)
    target = _make_user(role=UserRole.admin, org_id=org_id)
    svc, user_repo, token_repo = _svc()
    user_repo.get_by_id = AsyncMock(return_value=target)
    user_repo.lock_active_admins = AsyncMock(return_value=[uuid.uuid4() for _ in range(2)])

    await svc.soft_delete(actor, target.id)

    user_repo.soft_delete.assert_awaited_once_with(target.id, org_id)
    token_repo.revoke_all_for_user.assert_awaited_once_with(target.id, "account")


@pytest.mark.asyncio
async def test_soft_delete_by_manager_forbidden() -> None:
    svc, user_repo, _ = _svc()
    with pytest.raises(ForbiddenError) as exc:
        await svc.soft_delete(_make_user(role=UserRole.manager), uuid.uuid4())
    assert exc.value.code == "users.admin_required"
    user_repo.soft_delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_deactivate_last_admin_raises_conflict() -> None:
    target = _make_user(role=UserRole.admin)
    svc, user_repo, _ = _svc()
    user_repo.get_by_id = AsyncMock(return_value=target)
    user_repo.lock_active_admins = AsyncMock(return_value=[uuid.uuid4() for _ in range(1)])
    with pytest.raises(ConflictError) as exc:
        await svc.deactivate(_make_user(role=UserRole.admin), target.id)
    assert exc.value.code == "users.last_admin"


@pytest.mark.asyncio
async def test_demote_last_admin_via_edit_raises_conflict() -> None:
    target = _make_user(role=UserRole.admin)
    svc, user_repo, _ = _svc()
    user_repo.get_by_id = AsyncMock(return_value=target)
    user_repo.lock_active_admins = AsyncMock(return_value=[uuid.uuid4() for _ in range(1)])
    with pytest.raises(ConflictError) as exc:
        await svc.edit_employee(_make_user(role=UserRole.admin), target.id, {"role": "manager"})
    assert exc.value.code == "users.last_admin"


@pytest.mark.asyncio
async def test_deactivating_already_inactive_admin_skips_last_admin_guard() -> None:
    target = _make_user(role=UserRole.admin, is_active=False)
    svc, user_repo, _ = _svc()
    user_repo.get_by_id = AsyncMock(return_value=target)
    user_repo.lock_active_admins = AsyncMock(return_value=[uuid.uuid4() for _ in range(1)])
    user_repo.update = AsyncMock(return_value=target)
    await svc.deactivate(_make_user(role=UserRole.admin), target.id)
    user_repo.update.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("role", [UserRole.employee, UserRole.client])
async def test_list_employees_forbidden_for_non_managers(role: UserRole) -> None:
    svc, user_repo, _ = _svc()
    with pytest.raises(ForbiddenError):
        await svc.list_employees(_make_user(role=role))
    user_repo.search.assert_not_awaited()


@pytest.mark.asyncio
async def test_list_employees_scopes_to_the_actors_organisation() -> None:
    org_id = uuid.uuid4()
    actor = _make_user(role=UserRole.manager, org_id=org_id)
    svc, user_repo, _ = _svc()
    user_repo.search = AsyncMock(return_value=[])
    gid = uuid.uuid4()
    await svc.list_employees(actor, search="a", role=UserRole.employee, group_id=gid)
    user_repo.search.assert_awaited_once_with(
        org_id,
        search="a",
        role=UserRole.employee,
        is_active=None,
        group_id=gid,
        qualification_id=None,
        employment_type=None,
    )


# ── Cancelling outstanding links (T234, spec FR-207) ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_deactivate_cancels_the_users_outstanding_links() -> None:
    actor = _make_user(role=UserRole.manager)
    target = _make_user(role=UserRole.employee)
    svc, user_repo, _ = _svc()
    user_repo.get_by_id = AsyncMock(return_value=target)
    user_repo.update = AsyncMock(return_value=_make_user(is_active=False))

    await svc.deactivate(actor, target.id)

    cast(Any, svc._link_repo).cancel_for_user.assert_awaited_once_with(target.id)


@pytest.mark.asyncio
async def test_soft_delete_cancels_the_users_outstanding_links() -> None:
    org_id = uuid.uuid4()
    actor = _make_user(role=UserRole.admin, org_id=org_id)
    target = _make_user(role=UserRole.employee, org_id=org_id)
    svc, user_repo, _ = _svc()
    user_repo.get_by_id = AsyncMock(return_value=target)

    await svc.soft_delete(actor, target.id)

    cast(Any, svc._link_repo).cancel_for_user.assert_awaited_once_with(target.id)


@pytest.mark.asyncio
async def test_a_refused_deactivation_cancels_nothing() -> None:
    org_id = uuid.uuid4()
    actor = _make_user(role=UserRole.manager, org_id=org_id)
    target = _make_user(role=UserRole.admin, org_id=org_id)  # only an Admin may change an Admin
    svc, user_repo, _ = _svc()
    user_repo.get_by_id = AsyncMock(return_value=target)

    with pytest.raises(ForbiddenError):
        await svc.deactivate(actor, target.id)

    cast(Any, svc._link_repo).cancel_for_user.assert_not_awaited()
    user_repo.update.assert_not_awaited()


# ── Reactivating a pending account (T238, FR-220) ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_reactivating_a_pending_account_is_refused_and_changes_nothing() -> None:
    actor = _make_user(role=UserRole.manager)
    pending = _make_user(is_active=False, activated_at=None)
    svc, user_repo, token_repo = _svc()
    user_repo.get_by_id = AsyncMock(return_value=pending)

    with pytest.raises(ConflictError) as exc_info:
        await svc.reactivate(actor, pending.id)

    assert exc_info.value.code == "users.pending_cannot_reactivate"
    user_repo.update.assert_not_awaited()


@pytest.mark.asyncio
async def test_reactivating_a_deactivated_account_still_works_and_keeps_its_activation_time() -> (
    None
):
    actor = _make_user(role=UserRole.manager)
    deactivated = _make_user(is_active=False)  # activated_at defaults to a time in the helper
    svc, user_repo, _ = _svc()
    user_repo.get_by_id = AsyncMock(return_value=deactivated)
    user_repo.update = AsyncMock(return_value=_make_user(is_active=True))

    await svc.reactivate(actor, deactivated.id)

    user_repo.update.assert_awaited_once_with(deactivated.id, actor.organisation_id, is_active=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("client", [None, MagicMock(is_active=False)])
async def test_reactivating_a_portal_user_of_a_gone_client_is_refused(
    client: MagicMock | None,
) -> None:
    svc, user_repo, _ = _svc()
    target = _make_user(role=UserRole.client, is_active=False)
    target.client_id = uuid.uuid4()
    target.activated_at = datetime.now(UTC)
    user_repo.get_by_id = AsyncMock(return_value=target)
    cast(Any, svc._client_repo).get_by_id = AsyncMock(return_value=client)

    with pytest.raises(ConflictError) as refused:
        await svc.reactivate(_make_user(role=UserRole.admin), target.id)

    assert refused.value.code == "users.client_inactive"
    user_repo.update.assert_not_awaited()


@pytest.mark.asyncio
async def test_deactivating_a_pending_account_is_refused() -> None:
    svc, user_repo, token_repo = _svc()
    target = _make_user(is_active=False, activated_at=None)
    user_repo.get_by_id = AsyncMock(return_value=target)

    with pytest.raises(ConflictError) as refused:
        await svc.deactivate(_make_user(role=UserRole.admin), target.id)

    assert refused.value.code == "users.pending_cannot_deactivate"
    user_repo.update.assert_not_awaited()
    token_repo.revoke_all_for_user.assert_not_awaited()


@pytest.mark.parametrize("change", ["employee", "manager", "admin"])
@pytest.mark.asyncio
async def test_an_edit_of_a_client_accounts_role_is_refused(change: str) -> None:
    actor = _make_user(role=UserRole.admin)
    svc, user_repo, _ = _svc()
    target = _make_user(role=UserRole.client, org_id=actor.organisation_id)
    user_repo.get_by_id = AsyncMock(return_value=target)

    with pytest.raises(ConflictError) as refused:
        await svc.edit_employee(actor, target.id, {"role": change})

    assert refused.value.code == "users.client_role_locked"
    user_repo.update.assert_not_awaited()


# ── the default rate is Admin only (005 T012a) ──────────────────────────────


@pytest.mark.asyncio
async def test_a_manager_creating_an_employee_with_a_rate_is_refused() -> None:
    svc, user_repo, _ = _svc()
    with pytest.raises(ForbiddenError) as exc:
        await svc.create_employee(
            _make_user(role=UserRole.manager), {**_VALID, "default_hourly_rate": Decimal("50")}
        )
    assert exc.value.code == "users.rate_admin_only"
    user_repo.create_if_absent.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_manager_creating_an_employee_without_a_rate_is_allowed() -> None:
    svc, user_repo, _ = _svc()
    user_repo.get_by_email = AsyncMock(return_value=None)
    user_repo.create_if_absent = AsyncMock(return_value=(_make_user(), True))
    await svc.create_employee(
        _make_user(role=UserRole.manager), {**_VALID, "default_hourly_rate": None}
    )
    user_repo.create_if_absent.assert_awaited_once()


@pytest.mark.parametrize("rate", [Decimal("60"), None])
@pytest.mark.asyncio
async def test_a_manager_changing_or_clearing_a_rate_is_refused(rate: Decimal | None) -> None:
    svc, user_repo, _ = _svc()
    user_repo.get_by_id = AsyncMock(return_value=_make_user())
    with pytest.raises(ForbiddenError) as exc:
        await svc.edit_employee(
            _make_user(role=UserRole.manager), uuid.uuid4(), {"default_hourly_rate": rate}
        )
    assert exc.value.code == "users.rate_admin_only"
    user_repo.update.assert_not_awaited()
