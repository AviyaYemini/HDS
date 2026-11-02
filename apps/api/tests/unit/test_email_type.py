"""The ``Email`` request type ignores letter case and surrounding spaces (T282, FR-215).

``Bob@Corp.com`` and ``bob@corp.com`` are one address, so every request that takes an account
address must turn it into one canonical form before it reaches a service. The type must not change
the published API: its JSON schema stays a plain string of ``format: email``.

Covers: FR-215.
"""

import pytest
from pydantic import BaseModel, EmailStr, TypeAdapter, ValidationError

from hds.core.types import Email


@pytest.mark.parametrize(
    "given, stored",
    [
        ("bob@corp.com", "bob@corp.com"),
        ("Bob@Corp.com", "bob@corp.com"),
        ("BOB@CORP.COM", "bob@corp.com"),
        ("  Bob@corp.com  ", "bob@corp.com"),
        ("Bob.Smith+Tag@Sub.Corp.com", "bob.smith+tag@sub.corp.com"),
    ],
)
def test_the_whole_address_is_trimmed_and_lowercased(given: str, stored: str) -> None:
    assert TypeAdapter(Email).validate_python(given) == stored


@pytest.mark.parametrize(
    "bad", ["", "bob", "bob@", "@corp.com", "bob@corp", "a b@corp.com", 5, None]
)
def test_invalid_addresses_are_still_rejected(bad: object) -> None:
    with pytest.raises(ValidationError):
        TypeAdapter(Email).validate_python(bad)


def test_the_published_schema_is_unchanged() -> None:
    class WithEmail(BaseModel):
        email: Email

    class WithEmailStr(BaseModel):
        email: EmailStr

    assert (
        WithEmail.model_json_schema()["properties"]["email"]
        == (WithEmailStr.model_json_schema()["properties"]["email"])
    )
    assert WithEmail.model_json_schema()["properties"]["email"]["format"] == "email"
