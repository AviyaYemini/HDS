"""Request field types whose limits mirror the database column sizes.

Without them an over-long value passes validation and fails in PostgreSQL (a 500).

Layer: core. Imports only pydantic and the standard library; schemas in routers use these
aliases instead of bare ``str`` and ``Decimal``. Money is a ``Decimal``, never a ``float``
(Constitution VII).
"""

from datetime import date
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import BeforeValidator, Field, StringConstraints, WithJsonSchema

# PostgreSQL text cannot contain NUL; reject it at the edge instead of failing with a 500.
_NO_NUL = r"^[^\x00]*$"

# Column sizes: VARCHAR(255) names, VARCHAR(32) phones, VARCHAR(64) time zones; the rest are
# bounded for sanity (addresses are TEXT, passwords/tokens are only hashed).
_Str = StringConstraints


def _text(max_length: int) -> object:
    """Build the length-and-no-NUL constraint shared by the text types below."""
    return _Str(max_length=max_length, pattern=_NO_NUL)


Name = Annotated[str, _text(255)]
Phone = Annotated[str, _text(32)]
Timezone = Annotated[str, _text(64)]
Address = Annotated[str, _text(2000)]
Password = Annotated[str, _text(128)]
Token = Annotated[str, _text(512)]
# NUMERIC(12,4). Documented as a positive decimal STRING (exactly what responses return, so no
# float ever enters the contract). "> 0" is stated in the schema but enforced by the services
# with their own 422 error codes, so pydantic must not reject it first. JSON numbers are still
# parsed exactly into Decimal.
Money = Annotated[
    Decimal,
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
