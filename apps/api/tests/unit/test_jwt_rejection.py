"""T142: forged, expired and unsigned (alg=none) tokens are rejected.

Covers: NFR-006.
"""

import base64
import json
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from hds.core.security import create_access_token, decode_access_token
from hds.models.user import UserRole

SECRET = "s" * 48


def _token(secret: str = SECRET, ttl: int = 900) -> str:
    return create_access_token(uuid.uuid4(), uuid.uuid4(), UserRole.admin, ttl, secret)


def _b64(data: dict[str, object]) -> str:
    return base64.urlsafe_b64encode(json.dumps(data).encode()).rstrip(b"=").decode()


def test_valid_token_round_trips() -> None:
    sub, org = uuid.uuid4(), uuid.uuid4()
    token = create_access_token(sub, org, UserRole.manager, 900, SECRET)
    claims = decode_access_token(token, SECRET)
    assert claims["sub"] == str(sub) and claims["org"] == str(org) and claims["role"] == "manager"


def test_wrong_key_is_rejected() -> None:
    with pytest.raises(ValueError):
        decode_access_token(_token(secret="a" * 48), SECRET)


def test_expired_token_is_rejected() -> None:
    with pytest.raises(ValueError):
        decode_access_token(_token(ttl=-60), SECRET)


def test_alg_none_token_is_rejected() -> None:
    exp = int((datetime.now(UTC) + timedelta(hours=1)).timestamp())
    claims = {"sub": str(uuid.uuid4()), "org": str(uuid.uuid4()), "role": "admin", "exp": exp}
    unsigned = f"{_b64({'alg': 'none', 'typ': 'JWT'})}.{_b64(claims)}."
    with pytest.raises(ValueError):
        decode_access_token(unsigned, SECRET)


def test_token_with_tampered_payload_is_rejected() -> None:
    header, _, signature = _token().split(".")
    forged = f"{header}.{_b64({'sub': 'x', 'role': 'admin'})}.{signature}"
    with pytest.raises(ValueError):
        decode_access_token(forged, SECRET)


def test_garbage_is_rejected() -> None:
    with pytest.raises(ValueError):
        decode_access_token("not-a-token", SECRET)
