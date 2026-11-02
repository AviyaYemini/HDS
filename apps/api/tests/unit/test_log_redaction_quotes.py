"""A secret holding a quote or a backslash is removed whole, leaving no fragment (T369, F05).

The value pattern stopped at the first matching quote, even an escaped one, so the rest of a
password stayed in the log. Logs go to systems with weaker access control than the database, and a
fragment of a password is still part of a password.

Covers: FR-203.
"""

import json
import time

import pytest
from hypothesis import given
from hypothesis import strategies as st

from hds.core.logging import redact

# Digits cannot appear in the redacted text, so any digit left in the output is a fragment.
SECRETS = st.text(alphabet="0123456789\"'\\}{[]() ,;:", min_size=1, max_size=24).filter(
    lambda secret: any(ch.isdigit() for ch in secret)
)
# A comma, a semicolon or a space ends an unquoted value by design, so raw values leave them out.
# An opening bracket right after a closer starts the next field, as in [token=abc][user=bob].
RAW_SECRETS = st.text(alphabet="0123456789\"'\\}])", min_size=1, max_size=24)


def leaves_nothing(text: str) -> bool:
    return not any(ch.isdigit() for ch in redact(text))


def test_the_reported_case_leaves_no_fragment() -> None:
    redacted = redact('{"password": "ab\\"cd"}')

    assert "cd" not in redacted
    assert "ab" not in redacted


def test_an_unquoted_value_still_ends_at_a_comma_or_a_closing_brace() -> None:
    assert redact("password=abc, kept") == "password=[REDACTED], kept"
    assert redact("{token=abc} kept") == "{token=[REDACTED]} kept"
    assert redact("password=abc&next=1") == "password=[REDACTED]&next=1"


def test_text_after_a_quoted_value_is_kept() -> None:
    redacted = redact('{"password": "a\\"b", "name": "kept"}')

    assert '"name": "kept"' in redacted


def test_a_value_cut_off_mid_way_leaves_nothing() -> None:
    assert leaves_nothing('{"password": "12')
    assert leaves_nothing('{"password": "12\\')
    assert leaves_nothing('password="12')
    assert leaves_nothing('{"password": "12 34')
    assert leaves_nothing('password="12 34"')
    assert leaves_nothing("password='12 34'")


@given(SECRETS)
def test_json_values_leave_no_fragment(secret: str) -> None:
    assert leaves_nothing(json.dumps({"password": secret}))


@given(SECRETS)
def test_python_repr_values_leave_no_fragment(secret: str) -> None:
    assert leaves_nothing(repr({"refresh_token": secret}))


@given(RAW_SECRETS)
def test_unquoted_key_value_pairs_leave_no_fragment(secret: str) -> None:
    assert leaves_nothing(f"password={secret}")


@given(SECRETS)
def test_quoted_key_value_pairs_leave_no_fragment(secret: str) -> None:
    assert leaves_nothing(f"password={json.dumps(secret)}")
    assert leaves_nothing(f"Foo(password={secret!r}, name='x')")


@given(SECRETS)
def test_redacting_twice_equals_redacting_once(secret: str) -> None:
    for text in (json.dumps({"password": secret}), repr({"token": secret}), f"secret={secret}"):
        assert redact(redact(text)) == redact(text)


@given(SECRETS)
def test_json_inside_json_leaves_no_fragment(secret: str) -> None:
    assert leaves_nothing(json.dumps({"body": json.dumps({"password": secret})}))


@given(SECRETS)
def test_json_logged_through_a_repr_leaves_no_fragment(secret: str) -> None:
    assert leaves_nothing(repr(json.dumps({"password": secret})))
    assert leaves_nothing(repr(json.dumps({"password": secret}).encode()))


def test_the_reported_case_through_a_repr_leaves_no_fragment() -> None:
    assert "cd" not in redact(repr('{"password": "ab\\"cd"}'))


@pytest.mark.parametrize(
    "value",
    [
        '["111", "222"]',
        "b'111 222'",
        "SecretStr('111 222')",
        '{"value": "111 222"}',
        "'111'",
        "('111', '222')",
        "[['111'], ['222']]",
        "['1]1', '222']",
        "{'a': {'b': '111'}, 'd': '222'}",
    ],
)
def test_a_value_that_is_not_a_plain_string_goes_whole(value: str) -> None:
    assert leaves_nothing(f"{{'token': {value}}}")
    assert leaves_nothing(f"Foo(token={value})")


def test_a_secret_ending_in_a_backslash_does_not_expose_the_next_field() -> None:
    assert leaves_nothing(json.dumps({"password": "pw\\", "password_confirm": "123"}))
    assert leaves_nothing(repr({"password": "pw\\", "token": "123"}))


def test_containers_inside_a_json_string_are_read_as_strings() -> None:
    assert leaves_nothing(json.dumps({"b": json.dumps({"token": ["a]1", "2"]})}))
    assert leaves_nothing(json.dumps({"b": json.dumps({"token": {"a": "}1", "b": "2"}})}))


@pytest.mark.parametrize(
    "text",
    ["{'token': SecretStr('abc 123", "Foo(token=SecretStr('abc 123", "token=Foo('abc 123\nrest"],
)
def test_a_truncated_wrapper_with_an_open_string_leaves_nothing(text: str) -> None:
    assert not any(ch.isdigit() for ch in redact(text).splitlines()[0])


def test_an_object_repr_in_angle_brackets_goes_whole() -> None:
    assert leaves_nothing("{'token': <Token 111 222>, 'x': 'y'}")


@pytest.mark.parametrize(
    ("text", "kept"),
    [
        ("jwt.exceptions.InvalidTokenError: Signature has expired", "Signature has expired"),
        ("invalid token: signature has expired", "signature has expired"),
        ("def f(token: str, password: str, x: int):", "def f(token: str, password: str"),
        ("[token=abc][user=bob] started", "[user=bob] started"),
        ('{"url": "/r?token=abc"}. kept', '"}. kept'),
        ('{"password": "abc"}kept', "}kept"),
        ("'/r?token=abc': bad", "': bad"),
        ("missing bearer header in request", "bearer header in request"),
    ],
)
def test_ordinary_diagnostics_are_left_alone(text: str, kept: str) -> None:
    assert kept in redact(text)


def test_a_container_laid_out_over_several_lines_goes_whole() -> None:
    assert leaves_nothing(json.dumps({"tokens": ["111", "222"]}, indent=2))
    assert leaves_nothing(json.dumps({"token": {"value": "111"}}, indent=2))


@pytest.mark.parametrize(
    "text",
    ['password=" 123"', 'password=",123"', "password=':123'"],
)
def test_a_quoted_secret_may_start_with_a_separator(text: str) -> None:
    assert leaves_nothing(text)


@pytest.mark.parametrize(
    "text",
    [
        "a" * 41 + "password=123",
        json.dumps({"a" * 41 + "password": "123"}),
        json.dumps({"password" + "a" * 41: "123"}),
        json.dumps({"phone_number": "123"}),
        json.dumps({"email_address": "123"}),
        "x-auth-token: 123",
        "Authorization: Bearer 12345678",
        "x-auth-token: Bearer 12345678",
        "token=Bearer 12345678",
        "password = 123",
        '"password"="123"',
    ],
)
def test_long_and_header_style_names_are_covered(text: str) -> None:
    assert leaves_nothing(text)


def test_a_long_local_part_is_masked_whole() -> None:
    assert redact("a" * 100 + "@example.com") == "a***@example.com"


@pytest.mark.parametrize("key", ["x-auth-token", "password_confirm", "refresh_token_hash"])
def test_compound_names_are_covered_wherever_the_keyword_sits(key: str) -> None:
    assert leaves_nothing(json.dumps({key: "123"}))
    assert leaves_nothing(f"{key}=123")


@pytest.mark.parametrize(
    "text",
    ["password=12}34", "password={12}34", 'password=12"34', "password=12'34", 'password="12"34'],
)
def test_a_quote_or_a_brace_inside_a_key_value_secret_leaves_no_tail(text: str) -> None:
    assert leaves_nothing(text)


@pytest.mark.parametrize(
    ("text", "kept"),
    [
        ('{"url": "/reset?token=abc", "next": "kept", "status": 500}', '"next": "kept"'),
        ('{"url": "/reset?token="}', 'token=[REDACTED]"}'),
        ('\\"password\\": \\"abc\nnext line kept', "next line kept"),
        ('\\"password\\": \\"abc\nnext line \\"kept', "next line"),
        ("password=ab(c rest kept", "rest kept"),
        ("ValueError('bad token=abc')\nnext line kept", "next line kept"),
        ("(\"password=abc\", 'kept')", "'kept'"),
        ("token='abc\nnext line kept", "next line kept"),
        ('{"password": "abc\nnext line kept', "next line kept"),
        ("{'token': b'abc def', 'x': 'kept'}", "'x': 'kept'"),
        ("{'token': SecretStr('abc def'), 'x': 'kept'}", "'x': 'kept'"),
        ("{'token': {'value': 'abc def'}, 'x': 'kept'}", "'x': 'kept'"),
        ("{'token': ['abc', 'def'], 'x': 'kept'}", "'x': 'kept'"),
        ("'password': 'abc' is not valid, kept", "is not valid, kept"),
        ('password="abc" user="bob" status=500', 'user="bob" status=500'),
        ('{"password": "abc"}. kept', "}. kept"),
        ('first token="abc" second token="abc" kept', "second token=[REDACTED] kept"),
        ("{'token': 'ab\\'c abc', 'x': 'kept'}", "'x': 'kept'"),
    ],
)
def test_text_after_a_value_survives(text: str, kept: str) -> None:
    redacted = redact(text)

    assert kept in redacted
    assert "abc" not in redacted


@pytest.mark.parametrize("text", ["a@b.co@b.co", 'token=\'token\':""token":a'])
def test_a_second_pass_changes_nothing_on_inputs_that_used_to_reshape(text: str) -> None:
    assert redact(redact(text)) == redact(text)


@pytest.mark.parametrize(
    "text",
    [
        "a" * 200_000,
        "\\" * 200_000,
        "a-" * 100_000,
        '{"password": "' + "\\" * 200_000,
        "password=" + '"a' * 100_000,
        "password=" + '"' * 200_000,
        "password=" + "}" * 200_000,
        "x@" * 100_000,
        "token" * 40_000,
        '"' + "token" * 40_000,
        "token-" * 40_000,
        "token=a( " * 20_000,
        '"token": a( ' * 20_000,
        '\\"password\\": \\"' + '\\\\"' * 100_000,
    ],
    ids=[
        "word",
        "backslash-run",
        "dashes",
        "backslashes",
        "quote-pairs",
        "quotes",
        "braces",
        "ats",
        "keywords",
        "quoted-keywords",
        "dashed-keywords",
        "wrappers",
        "pair-wrappers",
        "deep-quotes",
    ],
)
def test_hostile_text_is_handled_quickly(text: str) -> None:
    started = time.perf_counter()
    redact(text)
    assert time.perf_counter() - started < 5  # about 100 times slower than the real cost
