"""Request field types whose limits mirror the database column sizes, and the base for PATCH bodies.

Without the types an over-long value passes validation and fails in PostgreSQL (a 500). The
``ChangeRequest`` base refuses fields a change operation does not accept.

Layer: core. Imports only pydantic and the standard library; schemas in routers use these
aliases instead of bare ``str`` and ``Decimal``. Money is a ``Decimal``, never a ``float``
(Constitution VII).
"""

import re
from datetime import date, time
from decimal import Decimal
from typing import Annotated, Any, Literal

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    EmailStr,
    Field,
    PlainSerializer,
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


class ChangeRequest(BaseModel):
    """Base for PATCH bodies: a field the operation does not accept is refused with 422.

    Dropping it silently would answer 200 to a misspelt field or one the caller may not set. The
    published schema says ``additionalProperties: false``. Self-service profile edits do not use
    this base, because their service answers an unknown field with its own 403.
    """

    model_config = ConfigDict(extra="forbid")


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


_ISO_DATE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")


def _iso_string_only(value: object) -> object:
    """Accept only ``YYYY-MM-DD`` text (ASCII digits) or a date.

    Pydantic would otherwise read a number, or a numeric string, as a timestamp
    (``0`` -> 1970-01-01), so a typo would silently mean 1970.
    """
    if isinstance(value, date) or (isinstance(value, str) and _ISO_DATE.match(value)):
        return value
    raise ValueError("Input should be an ISO-8601 date such as 2027-06-06")


StrictDate = Annotated[date, BeforeValidator(_iso_string_only)]

_ISO_DATETIME = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}")


def _iso_datetime_text(value: object) -> object:
    """Accept only ISO-8601 date-time text, refusing numbers that lax parsing reads as epochs."""
    if isinstance(value, str) and _ISO_DATETIME.match(value):
        return value
    raise ValueError("Input should be an ISO-8601 date-time with a time zone")


IsoDate = StrictDate
"""A date given as ``YYYY-MM-DD`` text only; the name used for query parameters."""

IsoDateTime = Annotated[AwareDatetime, BeforeValidator(_iso_datetime_text)]
"""An instant given as ISO-8601 text with a time zone; numbers are refused."""

_LOCAL_TIME = r"^([01][0-9]|2[0-3]):[0-5][0-9](:[0-5][0-9])?$"
_ISO_TIME = re.compile(_LOCAL_TIME)


def _iso_time_text(value: object) -> object:
    """Accept only ``HH:MM`` or ``HH:MM:SS`` text, refusing numbers read as seconds."""
    if isinstance(value, time) or (isinstance(value, str) and _ISO_TIME.match(value)):
        return value
    raise ValueError("Input should be a time of day such as 07:00")


_LOCAL_TIME_SCHEMA = {"type": "string", "pattern": _LOCAL_TIME, "example": "07:00"}

IsoTime = Annotated[time, BeforeValidator(_iso_time_text), WithJsonSchema(_LOCAL_TIME_SCHEMA)]
"""A local time of day given as ``HH:MM`` or ``HH:MM:SS`` text only, with no UTC offset.

Published as a string pattern, not ``format: time``, which requires an offset: these times are
local to a site and get their offset only when a shift on a date is made from them.
"""

LocalTimeOut = Annotated[
    time,
    PlainSerializer(lambda value: value.strftime("%H:%M:%S"), return_type=str),
    WithJsonSchema(_LOCAL_TIME_SCHEMA),
]
"""A local time of day in a response, written ``HH:MM:SS`` with no UTC offset."""

# Roles that may be assigned through the employee endpoints (clients have their own).
AssignableRole = Literal["employee", "manager", "admin"]


# A day of the week counted from Sunday, as the Israeli week runs (003 research R2): 0 Sunday to 6
# Saturday. Required wherever it is used as a whole value; strict, so "3" is refused, not cast.
Weekday = Annotated[int, Field(ge=0, le=6, strict=True, description="0 Sunday to 6 Saturday.")]
