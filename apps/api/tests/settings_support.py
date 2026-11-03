"""Build ``Settings`` in tests without the type checker demanding every required field.

``Settings`` reads required values (``ENVIRONMENT``, ``JWT_SECRET_KEY``) from the environment, so
a bare ``Settings()`` is correct at run time but looks incomplete to mypy. Passing the values as a
mapping keeps the call honest for both.

Covers: none directly; support for the settings tests.
"""

from typing import Any

from hds.core.config import Settings


def make_settings(**values: Any) -> Settings:
    """Create ``Settings`` from the environment plus the given overrides.

    Args:
        values: Field values (or ``_env_file``) passed straight to ``Settings``.

    Returns:
        The validated settings.

    Raises:
        pydantic.ValidationError: As ``Settings`` does, when a value is missing or refused.
    """
    return Settings(**values)
