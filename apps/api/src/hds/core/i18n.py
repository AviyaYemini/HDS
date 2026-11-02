"""Message lookup: error ``code`` -> human text in Hebrew or English.

Locale keys are exactly the error ``code`` values raised by routers and services
(e.g. ``users.last_admin``); ``tests/unit/test_i18n.py`` fails if a code has no message.

Layer: core. Reads ``hds/locales/*.json`` and imports nothing from other ``hds`` packages.
Hebrew is the primary language, English the fallback (Constitution VIII).
"""

import json
from functools import cache
from pathlib import Path

DEFAULT_LANGUAGE = "en"
SUPPORTED_LANGUAGES = ("en", "he")
_LOCALES = Path(__file__).resolve().parent.parent / "locales"


@cache
def _catalog(language: str) -> dict[str, str]:
    data: dict[str, str] = json.loads((_LOCALES / f"{language}.json").read_text(encoding="utf-8"))
    return data


def _normalise(language: str) -> str:
    base = language.strip().lower().split("-")[0].split("_")[0]
    return base if base in SUPPORTED_LANGUAGES else DEFAULT_LANGUAGE


def message(code: str, language: str = DEFAULT_LANGUAGE) -> str:
    """Return the message for an error code in the requested language.

    Lookup never raises, so an untranslated code degrades gracefully instead of turning one
    error into another: unsupported languages and regional tags (``he-IL``) are reduced to a
    supported base language or English, a missing key falls back to English, and a key
    missing everywhere returns the code itself.

    Args:
        code: Error code, which is also the locale key.
        language: Language tag such as ``he`` or ``en-US``.

    Returns:
        The translated text, or ``code`` if no catalogue has it.
    """
    lang = _normalise(language)
    text = _catalog(lang).get(code)
    if text is None and lang != DEFAULT_LANGUAGE:
        text = _catalog(DEFAULT_LANGUAGE).get(code)
    return text if text is not None else code
