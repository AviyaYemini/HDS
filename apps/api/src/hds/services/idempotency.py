"""Retried writes: a repeat with the same ``Idempotency-Key`` gets the first answer (003 FR-115a).

Layer: services. Depends on ``hds.core.exceptions`` and the repository it is given; it MUST NOT
import routers or know about HTTP.

A request with a key claims it, runs, and stores its answer in the same transaction, so a refused
request leaves no key behind. A repeat with the same key and the same request waits for the first,
then gets its stored answer and changes nothing; the same key with another request is refused.
Keys are per caller and count for ``KEY_LIFETIME``.
"""

import hashlib
import json
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from hds.core.exceptions import DomainValidationError
from hds.repositories.idempotency_key import IdempotencyKeyRepository

KEY_LIFETIME = timedelta(hours=24)
"""How long a key keeps its first answer; the purge script removes older ones (research R8)."""


def fingerprint(request: dict[str, Any]) -> str:
    """The SHA-256 hex digest of a request's identifying parts, in a canonical form.

    Args:
        request: What identifies the request, for example the operation, the parsed path ids and
            the parsed body, already in JSON form.
    """
    return hashlib.sha256(json.dumps(request, sort_keys=True).encode()).hexdigest()


class IdempotencyService:
    """Claims keys, replays stored answers and stores new ones."""

    def __init__(
        self,
        key_repo: IdempotencyKeyRepository,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        """Wire the service to its repository.

        Args:
            key_repo: The stored keys and answers.
            now: The current UTC time.
        """
        self._keys = key_repo
        self._now = now

    async def run_once(
        self,
        user_id: uuid.UUID,
        key: str | None,
        request: dict[str, Any],
        run: Callable[[], Awaitable[tuple[int, Any]]],
    ) -> tuple[int, Any]:
        """Run ``run`` unless the same request with ``key`` already ran; answer either way.

        Args:
            user_id: The caller, whose keys these are.
            key: The ``Idempotency-Key`` header, or ``None`` to run without one.
            request: What identifies the request (see ``fingerprint``).
            run: The write; it returns the answer's status code and JSON body.

        Returns:
            ``(status_code, body)`` of this run or of the first one.

        Raises:
            DomainValidationError: ``idempotency.key_reused`` when the key was used for another
                request (HTTP 422).
        """
        if key is None:
            return await run()
        digest = fingerprint(request)
        held = await self._keys.reserve(user_id, key, digest, self._now() - KEY_LIFETIME)
        if held is not None:
            if held.request_hash != digest:
                raise DomainValidationError(
                    code="idempotency.key_reused",
                    detail="This Idempotency-Key was used for a different request.",
                )
            return held.status_code, held.response_body
        status, body = await run()
        await self._keys.store(user_id, key, status, body)
        return status, body
