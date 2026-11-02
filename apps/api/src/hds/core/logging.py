"""Structured JSON logging with redaction of secrets and personal data.

Layer: core. Depends on ``hds.core.request_context`` only.

Logs go to stdout as one JSON object per line. Redaction runs on the final text of a record, after
the format arguments have been substituted, so it does not matter whether a secret arrived in the
message template, in a ``%s`` argument, inside a dict's repr or in an exception message. Values of
keys containing ``password``, ``token``, ``secret``, ``email`` or ``phone`` (compound names such as
``x-auth-token`` too) are replaced with ``[REDACTED]``, ``key=value`` text of the secret kind
likewise, ``key: value`` for compound names, a ``Bearer`` credential too, and any other email
address is masked as ``a***@domain``. Regular expressions find the names; a small scanner then
reads each value by its structure, so strings go by their escapes, containers go whole however they
nest, and text after a value stays. Where text cannot be read for certain, losing text is the safe
direction. This is a defence for text that should not hold secrets at all: free text cannot be
parsed exactly, so callers still must not log them.
Logs are
copied to systems with weaker access control than the database, and invitation or reset tokens in
them would let a reader take over an account (specs/002-security-hardening, FR-203).
"""

import json
import logging
import re
from collections.abc import Callable

from hds.core.request_context import get_request_id

# Regular expressions find names and a small scanner reads each value by its structure. Free text
# cannot be matched reliably by a pattern alone: a list can nest, and a pattern that guesses where a
# value ends either leaks the end of a secret or erases the rest of the record.
_SECRET_WORD = re.compile(r"""password|token|secret""", re.IGNORECASE)
_SENSITIVE_WORD = re.compile(r"""password|token|secret|email|phone""", re.IGNORECASE)
# "key": value in JSON and Python repr, and \"key\": \"value\" inside a string that is itself JSON.
# Each matches any name, once, and the name is then checked, or a name repeating a keyword makes the
# search quadratic.
_PAIR = re.compile(r"""(?<!\\)(\\*+["'])([\w-]++)\1\s*[:=]\s*""")
# key=value text, and key: value for compound names only, for the secret kinds. A plain word before
# a colon is prose: "invalid token: expired", "def f(token: str)".
_ASSIGNMENT = re.compile(r"""(?<![\w-])([\w-]++)(?:\s*=\s*|(?P<colon>:[ \t]+))""")
_COMPOUND = re.compile(r"""[\w-]+[-_]""")
_BEARER = re.compile(r"""\b(bearer)[ \t]+[A-Za-z0-9._~+/-]{8,}=*""", re.IGNORECASE)
_EMAIL = re.compile(r"(?<![\w.%+-])([\w.%+-])[\w.%+-]*@([\w-]+(?:\.[\w-]+)+)")
_WHOLE_EMAIL = re.compile(r"^[\w.%+-]+@[\w-]+(?:\.[\w-]+)+$")
_MASKED_EMAIL = r"\1***@\2"
_MAX_PASSES = 5
# One run of value characters, or of closers (a quote or a bracket) that more value follows at once.
# Other closers belong to the text around the value, as in "/reset?token=abc", "next".
_UNQUOTED = re.compile(r"""(?:[^\s&,;"'}\])]++|["'}\])]++(?=[\w\\]))*+""")
# A string with a prefix, as in b'...', and a wrapper, as in SecretStr('...').
_STRING_PREFIX = re.compile(r"""[bBrRuU]{1,2}["']""")
_WRAPPER = re.compile(r"""[\w.]++\(""")
_NOTHING_AFTER = re.compile(r"""["')\]}]++[ \t]*+(?:\n|\Z)""")
_CLOSERS = {"[": "]", "(": ")", "{": "}"}
# How far a wrapper is followed. Past this it is not a wrapper, or "a( a( a( ..." is quadratic.
_WRAPPER_REACH = 2000


def _line_end(text: str, start: int) -> int:
    """Return the index of the end of the line holding ``start``."""
    end = text.find("\n", start)
    return len(text) if end == -1 else end


def _string_end(text: str, start: int) -> int:
    """Return the end of the quoted string at ``start``, or of its line if it is never closed.

    A value cut off by truncation goes to the end of its line, so the lines after it stay. Two
    backslashes before a quote are ambiguous: an escape in a repr of JSON, where escapes are
    doubled, or the end of a value ending in a backslash, where a later field is exposed if the
    scan carries on. Neither can be told from the other, so the rest of the line goes.
    """
    quote = text[start]
    doubled = "\\" + quote
    index = start + 1
    while index < len(text):
        char = text[index]
        if char == "\n":
            return index
        if char == quote:
            return index + 1
        if char == "\\":
            if text.startswith(doubled, index + 1):
                return _line_end(text, index)
            index += 2
        else:
            index += 1
    return len(text)


def _brackets_end(text: str, start: int, stop: int, escaped: str | None = None) -> int:
    r"""Return the end of the bracketed value at ``start``, which may span lines and nest.

    Args:
        text: The whole text.
        start: The opening bracket.
        stop: Where to give up.
        escaped: The string delimiter if the brackets are inside a string, such as ``\"``.

    Returns:
        The index just after the closing bracket, or ``stop`` if it is not found.
    """
    expected = [_CLOSERS[text[start]]]
    index = start + 1
    while index < stop and expected:
        char = text[index]
        if escaped is not None and text.startswith(escaped, index):
            index = _escaped_string_end(text, index, escaped)
        elif char in "\"'":
            index = _string_end(text, index)
        elif char == "\\":
            index += 2
        else:
            if char in _CLOSERS:
                expected.append(_CLOSERS[char])
            elif char == expected[-1]:
                expected.pop()
            index += 1
    return index if not expected else stop


def _escaped_string_end(text: str, start: int, delimiter: str) -> int:
    """Return the end of a value in ``delimiter``, a quote with backslashes, inside a string.

    Only a delimiter not preceded by a further backslash closes it: more backslashes are one level
    deeper, which is a quote inside the value.
    """
    index = start + len(delimiter)
    line_end = text.find("\n", start)
    while True:
        index = text.find(delimiter, index)
        if index == -1 or -1 < line_end < index:
            return len(text) if line_end == -1 else line_end
        if text[index - 1] != "\\":
            return index + len(delimiter)
        index += len(delimiter)


def _value_end(text: str, start: int, delimiter: str | None = None) -> int:
    """Return where the value at ``start`` ends, reading it by its structure.

    Args:
        text: The whole text.
        start: Where the value starts.
        delimiter: The key's quote for a quoted pair, which has backslashes if the pair is inside a
            string; ``None`` for ``key=value`` text, where a value can carry on after a quote.

    Returns:
        The index just after the value. Containers and strings go whole; anything else ends at
        whitespace, ``&``, a comma, a semicolon, or a closing quote or bracket with no value after.
    """
    escaped = delimiter if delimiter is not None and len(delimiter) > 1 else None
    if escaped is not None and text.startswith(escaped, start):
        return _escaped_string_end(text, start, escaped)
    if _NOTHING_AFTER.match(text, start):
        return start  # the end of a string around an empty value, as in "/reset?token="
    first = text[start : start + 1]
    index = start
    if first and first in _CLOSERS:
        index = _brackets_end(text, start, len(text), escaped)
    elif first and first in "\"'":
        index = _string_end(text, start)
    elif first == "<":
        closing = text.find(">", start, _line_end(text, start))
        index = closing + 1 if closing != -1 else _line_end(text, start)
    elif prefixed := _STRING_PREFIX.match(text, start):
        index = _string_end(text, prefixed.end() - 1)
    elif wrapper := _WRAPPER.match(text, start):
        opening = wrapper.end() - 1
        stop = min(len(text), opening + _WRAPPER_REACH)
        index = _brackets_end(text, opening, stop)
        if index == stop and text[index - 1] != ")":
            # Not closed. With a string open it is a truncated value; if not, just a bracket.
            opened = text[opening + 1 : opening + 2] in ('"', "'")
            index = _line_end(text, start) if opened else start
    if delimiter is not None and index > start:
        return index
    matched = _UNQUOTED.match(text, index)
    return matched.end() if matched else index


def _replace_values(
    text: str,
    name: re.Pattern[str],
    sensitive: Callable[[re.Match[str]], bool],
    replace: Callable[[re.Match[str]], str],
) -> str:
    """Replace each sensitive name found by ``name``, with the value after it.

    Args:
        text: The text.
        name: Finds a name followed by a separator.
        sensitive: Says whether a match is sensitive.
        replace: Builds the replacement from the match.

    Returns:
        The text with each sensitive name and value replaced.
    """
    parts: list[str] = []
    position = 0
    while (match := name.search(text, position)) is not None:
        if not sensitive(match):
            parts.append(text[position : match.end()])
            position = match.end()
            continue
        delimiter = match.group(1) if name is _PAIR else None
        parts += [text[position : match.start()], replace(match)]
        position = max(_value_end(text, match.end(), delimiter), match.end())
    return "".join([*parts, text[position:]])


def _sensitive_pair(match: re.Match[str]) -> bool:
    """Say whether a quoted pair's name is sensitive."""
    return _SENSITIVE_WORD.search(match.group(2)) is not None


def _secret_assignment(match: re.Match[str]) -> bool:
    """Say whether an assignment's name is of the secret kind."""
    name = match.group(1)
    if _SECRET_WORD.search(name) is None:
        return False
    return match.group("colon") is None or _COMPOUND.match(name) is not None


def _redacted_pair(match: re.Match[str]) -> str:
    """Render a sensitive pair."""
    return f'"{match.group(2)}": "[REDACTED]"'


def _redacted_assignment(match: re.Match[str]) -> str:
    """Render a secret assignment."""
    return f"{match.group(1)}=[REDACTED]"


def mask_email(address: str) -> str:
    """Mask an email address as its first character plus ``***`` and the domain.

    The first letter keeps a masked address recognisable to the person who owns it without
    identifying them to someone reading the log.

    Args:
        address: A complete email address.

    Returns:
        ``a***@domain`` for an address; the input unchanged if it is not an address.
    """
    if not _WHOLE_EMAIL.match(address):
        return address
    return _EMAIL.sub(_MASKED_EMAIL, address)


def redact(text: str) -> str:
    """Remove secrets and mask email addresses in ``text``.

    One rule can reshape the text for another, so the rules repeat until nothing changes. That also
    means applying it again to its own output changes nothing.

    Args:
        text: Any text that may end up in a log.

    Returns:
        The text with sensitive values replaced by ``[REDACTED]`` and email addresses masked.
    """
    for _ in range(_MAX_PASSES):
        before = text
        text = _BEARER.sub(r"\1 [REDACTED]", text)
        text = _replace_values(text, _PAIR, _sensitive_pair, _redacted_pair)
        text = _replace_values(text, _ASSIGNMENT, _secret_assignment, _redacted_assignment)
        text = _EMAIL.sub(_MASKED_EMAIL, text)
        if text == before:
            break
    return text


class SensitiveDataFilter(logging.Filter):
    """Redact every record's final message and stamp it with the request id."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Rewrite the record so its text is redacted; always keep it.

        The message is formatted first, with its arguments, and then redacted as a whole. The
        arguments are cleared afterwards so no later handler can rebuild the unredacted text. A
        message whose arguments do not fit its template is kept as its template text instead of
        raising.

        Args:
            record: The log record about to be emitted.

        Returns:
            Always ``True``: records are rewritten, never dropped.
        """
        record.request_id = get_request_id()
        try:
            text = record.getMessage()
        except (TypeError, ValueError, KeyError):
            text = str(record.msg)
        record.msg = redact(text)
        record.args = None
        return True


class JsonFormatter(logging.Formatter):
    """One valid JSON object per line, whatever the message contains."""

    def format(self, record: logging.LogRecord) -> str:
        """Render ``record`` as a single-line JSON document.

        The exception text is redacted here as well, because an exception message can carry a
        token or an address just like a log message.

        Args:
            record: The log record to render.

        Returns:
            JSON text with time, level, request id, logger, message and, when present, the
            formatted exception.
        """
        entry = {
            "time": self.formatTime(record),
            "level": record.levelname,
            "request_id": getattr(record, "request_id", "-"),
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            entry["exception"] = redact(self.formatException(record.exc_info))
        return json.dumps(entry, ensure_ascii=False)


def configure_logging() -> None:
    """Install the JSON formatter and redaction filter on the root logger at INFO level.

    Side effects:
        Calls ``logging.basicConfig``; it is a no-op if the root logger already has handlers.
    """
    handler = logging.StreamHandler()
    handler.addFilter(SensitiveDataFilter())
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler])
