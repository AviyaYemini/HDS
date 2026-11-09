"""Saved views are one user's own (007 FR-043, task T082).

Fake repository: a client has no tables and is refused; removing another user's view answers 404.
"""

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from hds.core.exceptions import ForbiddenError, NotFoundError
from hds.models.user import User, UserRole
from hds.services.saved_view import SavedViewService


def _user(role: UserRole = UserRole.manager) -> User:
    user = MagicMock(spec=User)
    user.id, user.organisation_id, user.role = uuid.uuid4(), uuid.uuid4(), role
    return user


@pytest.mark.asyncio
async def test_a_view_is_saved_for_its_owner() -> None:
    views = AsyncMock()
    me = _user()
    await SavedViewService(views).save(me, "staff", "Active", "status=active")
    assert views.create.await_args.args[1] == {
        "user_id": me.id,
        "screen": "staff",
        "name": "Active",
        "query": "status=active",
    }


@pytest.mark.asyncio
async def test_a_client_has_no_saved_views() -> None:
    with pytest.raises(ForbiddenError):
        await SavedViewService(AsyncMock()).list(_user(UserRole.client), "staff")


@pytest.mark.asyncio
async def test_removing_someone_elses_view_is_not_found() -> None:
    views = AsyncMock()
    views.delete_own.return_value = False
    with pytest.raises(NotFoundError):
        await SavedViewService(views).remove(_user(), uuid.uuid4())
