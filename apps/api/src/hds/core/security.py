"""Password hashing, access-token signing and refresh-token generation (Constitution VI).

Layer: core. Imports only ``hds.models.user`` for the role enum; it MUST NOT depend on
repositories, services or routers, and it never reads settings itself (callers pass the secret).
"""

import hashlib
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import jwt
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError

from hds.models.user import UserRole

_ph = PasswordHasher(time_cost=2, memory_cost=65536, parallelism=2)


def hash_password(plain: str) -> str:
    """Hash a password with Argon2id (the only permitted algorithm).

    Args:
        plain: The clear-text password. It is never stored or logged.

    Returns:
        The encoded hash, which embeds its own salt and cost parameters.
    """
    return _ph.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    """Check a password against a stored Argon2id hash.

    Args:
        plain: The clear-text password offered at sign-in.
        hashed: The hash stored for the user.

    Returns:
        ``True`` on a match, ``False`` on a mismatch. A malformed or non-Argon2 hash raises
        the underlying ``argon2`` error rather than returning ``False``.
    """
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
    """Sign a short-lived access token.

    The role claim is informational for clients; authorisation always uses the live user
    row (see ``hds.core.deps.get_current_user``).

    Args:
        sub: User id, stored as the ``sub`` claim.
        org: Organisation id, stored as ``org``.
        role: User role, stored as ``role``.
        ttl_seconds: Lifetime; the ``exp`` claim is issued-at plus this.
        secret: HMAC signing key.
        algorithm: JWT algorithm name.

    Returns:
        The compact JWT.
    """
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
    """Verify an access token and return its claims.

    Args:
        token: The compact JWT from the ``Authorization`` header.
        secret: HMAC key the token must be signed with.
        algorithm: The single accepted algorithm.

    Returns:
        The decoded claims.

    Raises:
        ValueError: If the signature, algorithm or expiry is invalid, or ``exp`` is missing.
    """
    try:
        # The algorithm list is explicit (never taken from the token header), so alg=none and
        # algorithm-confusion tokens are refused; exp is mandatory.
        claims: dict[str, Any] = jwt.decode(
            token, secret, algorithms=[algorithm], options={"require": ["exp"]}
        )
        return claims
    except jwt.PyJWTError as exc:
        raise ValueError(f"Invalid token: {exc}") from exc


def generate_refresh_token() -> tuple[str, str]:
    """Create a refresh token and the hash to store for it.

    Only the SHA-256 hash is persisted, so a database leak does not yield usable tokens.

    Returns:
        ``(raw_token, sha256_hex_hash)``; the raw value is shown to the client once.
    """
    raw = str(uuid.uuid4())
    hash_ = hashlib.sha256(raw.encode()).hexdigest()
    return raw, hash_
