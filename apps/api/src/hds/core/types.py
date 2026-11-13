"""Request field types whose limits mirror the database column sizes, and the base for PATCH bodies.

Without the types an over-long value passes validation and fails in PostgreSQL (a 500). The
``ChangeRequest`` base refuses fields a change operation does not accept.

Layer: core. Imports only pydantic and the standard library; schemas in routers use these
aliases instead of bare ``str`` and ``Decimal``. Money is a ``Decimal``, never a ``float``
(Constitution VII).
"""

import math
import numbers
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


def text(max_length: int, min_length: int | None = None) -> object:
    """Build the length-and-no-NUL constraint for a request's free text.

    Every free-text field of a request body uses it (or a type below built on it), so a NUL is a
    422 and never reaches the database (``test_strict_request_fields``). ``min_length`` stays
    unset unless given, so a ``Field(min_length=...)`` on a field of these types still applies.
    """
    return _Str(min_length=min_length, max_length=max_length, pattern=_NO_NUL)


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
Name = Annotated[str, text(255)]
Phone = Annotated[str, text(32)]
Timezone = Annotated[str, text(64)]
Address = Annotated[str, text(2000)]
Password = Annotated[str, text(128)]
Token = Annotated[str, text(512)]
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


def whole_number(value: object) -> object:
    """Let only a JSON integer through, or a number with no fractional part (``40.0``).

    Pydantic's lax integers read ``true`` as 1 and ``"11"`` as 11, which the published schema
    (``type: integer``) does not allow; strict integers refuse ``40.0``, which it does.
    """
    if isinstance(value, bool) or not isinstance(value, numbers.Real):
        raise ValueError("must be a whole number")
    # JSON's 1e400 reads as infinity, which no floor turns into a number (code review, PR 5).
    if not math.isfinite(value):
        raise ValueError("must be a whole number")
    whole = math.floor(value)
    if whole != value:
        raise ValueError("must be a whole number")
    return whole


# Use as ``Annotated[int, Field(ge=..., le=...), WHOLE_NUMBER]``: an integer exactly as JSON Schema
# means it, 40 and 40.0, never true, "40" or 40.5. The bounds come first, so they bind to the
# integer and the published schema keeps ``minimum`` and ``maximum``.
WHOLE_NUMBER = BeforeValidator(whole_number)


_PERCENT = r"^(?:(?:1[0-9]{2}|2[0-9]{2})(?:\.[0-9]{1,2})?|300(?:\.0{1,2})?)$"
_POINTS = r"^(?:(?!0+(?:\.0+)?$)(?:[0-9]{1,2}|1[0-9]{2})(?:\.[0-9]{1,2})?|200(?:\.0{1,2})?)$"


def _decimal_text(pattern: str) -> Any:
    """Accept only a JSON string matching ``pattern``, the shape the schema publishes."""
    compiled = re.compile(pattern)

    def check(value: object) -> object:
        if not isinstance(value, str) or compiled.fullmatch(value) is None:
            raise ValueError("Input should be a decimal string such as 125.00")
        return value

    return BeforeValidator(check)


Percent = Annotated[
    Decimal,
    _decimal_text(_PERCENT),
    Field(ge=100, le=300, decimal_places=2),
    WithJsonSchema({"type": "string", "pattern": _PERCENT, "example": "125.00"}),
]
"""A pay rate in percent of the base rate, 100 to 300, as a decimal string (005 FR-003)."""

Points = Annotated[
    Decimal,
    _decimal_text(_POINTS),
    Field(gt=0, le=200, decimal_places=2),
    WithJsonSchema({"type": "string", "pattern": _POINTS, "example": "25.00"}),
]
"""Percentage points added to a day's rate for overtime, above 0 and at most 200 (005 FR-003)."""

Minutes = Annotated[int, Field(ge=1, le=1440), WHOLE_NUMBER]
"""A length in whole minutes, 1 to 1440 (005 FR-003)."""

PercentOut = Annotated[
    Decimal,
    PlainSerializer(lambda value: f"{value:.2f}", return_type=str),
    WithJsonSchema({"type": "string", "pattern": r"^[0-9]{1,3}\.[0-9]{2}$", "example": "125.00"}),
]
"""A percentage or points in a response, always with two decimals."""

ShareOut = Annotated[
    Decimal,
    PlainSerializer(lambda value: f"{value:.2f}", return_type=str),
    WithJsonSchema({"type": "string", "pattern": r"^-?[0-9]+\.[0-9]{2}$", "example": "18.40"}),
]
"""A share in percent in a response, two decimals (CEO demo). A loss may run far below -100 when
billing is small, so the digits before the point are not bounded."""

Amount = Annotated[
    Decimal,
    PlainSerializer(lambda value: f"{value:.2f}", return_type=str),
    WithJsonSchema({"type": "string", "pattern": r"^[0-9]{1,10}\.[0-9]{2}$", "example": "612.50"}),
]
"""A money amount in a response: ``NUMERIC(12,2)`` in shape, a string with two decimals (005 R1)."""

HoursOut = Annotated[
    Decimal,
    PlainSerializer(lambda value: f"{value:.2f}", return_type=str),
    WithJsonSchema({"type": "string", "pattern": r"^[0-9]{1,7}\.[0-9]{2}$", "example": "7.50"}),
]
"""Hours in a response, a string with two decimals."""
