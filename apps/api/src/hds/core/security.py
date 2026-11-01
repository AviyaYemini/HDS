import hashlib
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError
from jose import JWTError, jwt

from hds.models.user import UserRole

_ph = PasswordHasher(time_cost=2, memory_cost=65536, parallelism=2)


def hash_password(plain: str) -> str:
    return _ph.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return _ph.verify(hashed, plain)
    except VerifyMismatchError:
        return False


def create_access_token(
    sub: uuid.UUID,
    org: uuid.UUID,
    role: UserRole,
    ttl_seconds: int,
    secret: str,
    algorithm: str = "HS256",
) -> str:
    now = datetime.now(UTC)
    payload = {
        "sub": str(sub),
        "org": str(org),
        "role": role.value,
        "iat": now,
        "exp": now + timedelta(seconds=ttl_seconds),
    }
    token: str = jwt.encode(payload, secret, algorithm=algorithm)
    return token


def decode_access_token(token: str, secret: str, algorithm: str = "HS256") -> dict[str, Any]:
    try:
        claims: dict[str, Any] = jwt.decode(token, secret, algorithms=[algorithm])
        return claims
    except JWTError as exc:
        raise ValueError(f"Invalid token: {exc}") from exc


def generate_refresh_token() -> tuple[str, str]:
    """Return (raw_token, sha256_hex_hash). Store only the hash."""
    raw = str(uuid.uuid4())
    hash_ = hashlib.sha256(raw.encode()).hexdigest()
    return raw, hash_
