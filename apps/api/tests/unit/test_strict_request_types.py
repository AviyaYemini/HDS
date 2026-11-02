"""Request booleans must be real JSON booleans: `0`/`1`/"yes" are schema violations (422)."""

import pytest
from pydantic import ValidationError

from hds.routers.clients import ClientUpdateRequest


@pytest.mark.parametrize("bad", [0, 1, "true", "yes", "0"])
def test_client_is_active_rejects_non_boolean(bad: object) -> None:
    with pytest.raises(ValidationError):
        ClientUpdateRequest(is_active=bad)  # type: ignore[arg-type]


@pytest.mark.parametrize("good", [True, False])
def test_client_is_active_accepts_boolean(good: bool) -> None:
    assert ClientUpdateRequest(is_active=good).is_active is good
