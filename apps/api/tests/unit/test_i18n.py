"""T126: message lookup by error code, and parity between used codes and locale files."""

import ast
import json
from pathlib import Path

import pytest

from hds.core import i18n

_SRC = Path(__file__).resolve().parents[2] / "src" / "hds"
_LOCALES = _SRC / "locales"
_CODE_DIRS = ("routers", "services", "core")


def _load(lang: str) -> dict[str, str]:
    return json.loads((_LOCALES / f"{lang}.json").read_text(encoding="utf-8"))


def _used_codes() -> dict[str, str]:
    """Every error code literal passed as ``code=`` (or to ``_problem``) -> where it is used."""
    found: dict[str, str] = {}
    for directory in _CODE_DIRS:
        for path in (_SRC / directory).rglob("*.py"):
            for node in ast.walk(ast.parse(path.read_text())):
                where = f"{path.relative_to(_SRC)}:{getattr(node, 'lineno', 0)}"
                if (
                    isinstance(node, ast.keyword)
                    and node.arg == "code"
                    and isinstance(node.value, ast.Constant)
                    and isinstance(node.value.value, str)
                ):
                    found.setdefault(node.value.value, where)
                if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "_problem":
                    for arg in node.args:
                        if (
                            isinstance(arg, ast.Constant)
                            and isinstance(arg.value, str)
                            and "." in arg.value
                            and " " not in arg.value
                        ):
                            found.setdefault(arg.value, where)
    return found


# ── parity ───────────────────────────────────────────────────────────────────────────


def test_the_ast_scan_finds_codes() -> None:
    codes = _used_codes()
    assert len(codes) >= 20
    assert {"users.last_admin", "projects.date_range_invalid", "resource.not_found"} <= set(codes)


@pytest.mark.parametrize("lang", ["en", "he"])
def test_every_code_used_in_source_exists_in_locale(lang: str) -> None:
    locale = _load(lang)
    missing = {c: w for c, w in _used_codes().items() if c not in locale}
    assert not missing, f"codes without a '{lang}' message: {missing}"


def test_he_and_en_have_identical_keys() -> None:
    en, he = _load("en"), _load("he")
    assert set(en) == set(he), sorted(set(en) ^ set(he))


@pytest.mark.parametrize("lang", ["en", "he"])
def test_no_blank_messages(lang: str) -> None:
    assert all(v.strip() for v in _load(lang).values())


def test_no_legacy_employees_prefixed_error_keys() -> None:
    """Error keys are named after the ``code`` that raises them (``users.*``)."""
    legacy = {"employees.last_admin_error", "employees.duplicate_email", "employees.admin_required"}
    assert not legacy & set(_load("en"))


# ── lookup ───────────────────────────────────────────────────────────────────────────


def test_message_returns_requested_language() -> None:
    assert i18n.message("users.last_admin", "en") == _load("en")["users.last_admin"]
    assert i18n.message("users.last_admin", "he") == _load("he")["users.last_admin"]


@pytest.mark.parametrize(("raw", "expected"), [("he-IL", "he"), ("HE", "he"), ("en-US", "en")])
def test_message_normalises_language_tags(raw: str, expected: str) -> None:
    assert i18n.message("users.last_admin", raw) == _load(expected)["users.last_admin"]


def test_unknown_language_falls_back_to_english() -> None:
    assert i18n.message("users.last_admin", "fr") == _load("en")["users.last_admin"]


def test_unknown_code_returns_the_code_itself() -> None:
    assert i18n.message("no.such_code", "en") == "no.such_code"
