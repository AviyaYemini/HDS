"""Request field types whose limits mirror the database column sizes.

Without them an over-long value passes validation and fails in PostgreSQL (a 500).

Layer: core. Imports only pydantic and the standard library; schemas in routers use these
aliases instead of bare ``str`` and ``Decimal``. Money is a ``Decimal``, never a ``float``
(Constitution VII).
"""

import re
from datetime import date
from decimal import Decimal
from typing import Annotated, Any, Literal

from pydantic import (
    AfterValidator,
    BeforeValidator,
    EmailStr,
    Field,
    StringConstraints,
    WithJsonSchema,
)

# PostgreSQL text cannot contain NUL; reject it at the edge instead of failing with a 500.
_NO_NUL = r"^[^\x00]*$"

# Column sizes: VARCHAR(255) names, VARCHAR(32) phones, VARCHAR(64) time zones; the rest are
# bounded for sanity (addresses are TEXT, passwords/tokens are only hashed).
_Str = StringConstraints


def _text(max_length: int) -> object:
    """Build the length-and-no-NUL constraint shared by the text types below."""
    return _Str(max_length=max_length, pattern=_NO_NUL)


def omittable() -> Any:
    """Declare a PATCH field that may be left out but not sent as null.

    Handlers read PATCH bodies with ``exclude_unset``, so the default is never used, and the plain
    type is what refuses null. FastAPI does not publish a default of ``None``.

    Returns:
        A pydantic field with a default.
    """
    return Field(default=None)


# An account address: valid as an email, then trimmed and lowercased as a whole, because
# ``Bob@Corp.com`` and ``bob@corp.com`` are one address (FR-215). The published schema stays a
# string with ``format: email``. Not for contact addresses of clients, which identify nobody.
Email = Annotated[EmailStr, AfterValidator(lambda address: address.strip().lower())]
Name = Annotated[str, _text(255)]
Phone = Annotated[str, _text(32)]
Timezone = Annotated[str, _text(64)]
Address = Annotated[str, _text(2000)]
Password = Annotated[str, _text(128)]
Token = Annotated[str, _text(512)]
# The schema's shape, plus a leading minus and zero: "> 0" is stated in the schema but enforced by
# the services with their own 422 error codes, so these must reach them. ASCII digits only, because
# ``\d`` and ``Decimal`` also accept digits of other scripts.
_MONEY_TEXT = re.compile(r"-?[0-9]{1,8}(\.[0-9]{1,4})?")


def _money_string_only(value: object) -> object:
    """Reject anything but a plain decimal string, including JSON numbers."""
    if not isinstance(value, str) or _MONEY_TEXT.fullmatch(value) is None:
        raise ValueError("Input should be a decimal string such as 75.0000")
    return value


# NUMERIC(12,4). A positive decimal STRING, exactly what responses return, so no float ever enters
# the contract. JSON numbers are refused: the schema says string, and most clients have already
# passed a number through a binary float by the time it is sent.
Money = Annotated[
    Decimal,
    BeforeValidator(_money_string_only),
    Field(max_digits=12, decimal_places=4),
    WithJsonSchema(
        {
            "type": "string",
            "pattern": r"^(?!0+(\.0+)?$)\d{1,8}(\.\d{1,4})?$",
            "example": "75.0000",
        }
    ),
]


def _iso_string_only(value: object) -> object:
    """Reject numbers, which pydantic would otherwise coerce to dates (0 -> 1970-01-01)."""
    if not isinstance(value, str | date):
        raise ValueError("Input should be an ISO-8601 date string")
    return value


StrictDate = Annotated[date, BeforeValidator(_iso_string_only)]

# Roles that may be assigned through the employee endpoints (clients have their own).
AssignableRole = Literal["employee", "manager", "admin"]
