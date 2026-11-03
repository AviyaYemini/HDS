"""Log redaction covers format arguments, repr output, exceptions and email addresses (T218).

The filter used to scan only the message template, so a secret passed as a ``%s`` argument went
to the log untouched. It now redacts the final text, whatever way it was built, and masks email
addresses (which identify people) as ``a***@domain``.

Covers: FR-203.
"""

import json
import logging
import sys
from typing import Any

from hds.core.logging import JsonFormatter, SensitiveDataFilter


def rendered(msg: str, args: Any = None) -> str:
    """Pass a record through the filter and return the message a handler would print."""
    record = logging.LogRecord("t", logging.INFO, __file__, 1, msg, args, None)
    assert SensitiveDataFilter().filter(record) is True
    return record.getMessage()


def test_secret_in_a_percent_argument_tuple_is_redacted() -> None:
    text = rendered("request body %s", ('{"password": "hunter2"}',))
    assert "hunter2" not in text
    assert "[REDACTED]" in text


def test_secret_in_a_percent_argument_mapping_is_redacted() -> None:
    record = logging.LogRecord(
        "t", logging.INFO, __file__, 1, "body %(b)s", ({"b": '{"token": "abc123xyz"}'},), None
    )
    SensitiveDataFilter().filter(record)
    assert "abc123xyz" not in record.getMessage()


def test_secret_inside_a_python_repr_is_redacted() -> None:
    """A dict argument prints with single quotes, which the old JSON-only pattern missed."""
    text = rendered("payload %s", ({"password": "hunter2", "name": "Dana"},))
    assert "hunter2" not in text
    assert "Dana" in text  # unrelated fields stay readable


def test_secret_in_the_message_template_is_still_redacted() -> None:
    assert "hunter2" not in rendered('{"password": "hunter2"}')


def test_key_value_style_secrets_are_redacted() -> None:
    text = rendered("retry with password=hunter2 and token=abcdef123456")
    assert "hunter2" not in text
    assert "abcdef123456" not in text


def test_compound_key_names_are_redacted() -> None:
    text = rendered('{"refresh_token": "r-123456789", "new_password": "p4ssw0rd!"}')
    assert "r-123456789" not in text
    assert "p4ssw0rd!" not in text


def test_email_addresses_are_masked_in_message_and_arguments() -> None:
    text = rendered("sent to %s and bob@corp.example", ("alice@hds.test",))
    assert "alice@hds.test" not in text
    assert "bob@corp.example" not in text
    assert "a***@hds.test" in text
    assert "b***@corp.example" in text


def test_mask_email_keeps_the_first_letter_and_the_domain() -> None:
    from hds.core.logging import mask_email

    assert mask_email("alice@corp.com") == "a***@corp.com"
    assert mask_email("a@x.org") == "a***@x.org"
    assert mask_email("not an address") == "not an address"


def test_record_args_are_cleared_so_no_other_handler_reformats_the_secret() -> None:
    record = logging.LogRecord("t", logging.INFO, __file__, 1, "x %s", ('"token": "zzz"',), None)
    SensitiveDataFilter().filter(record)
    assert record.args is None


def test_a_message_that_cannot_be_formatted_is_kept_without_raising() -> None:
    text = rendered("needs two %s %s", ("only one",))
    assert isinstance(text, str)


def test_filtering_twice_changes_nothing() -> None:
    first = rendered('{"password": "hunter2"} for a@b.example')
    assert rendered(first) == first


def test_exception_text_in_the_json_line_is_redacted() -> None:
    try:
        raise ValueError('rejected {"password": "hunter2"} from dana@hds.test')
    except ValueError:
        record = logging.LogRecord("t", logging.ERROR, __file__, 1, "boom", None, sys.exc_info())
    line = JsonFormatter().format(record)
    assert "hunter2" not in line
    assert "dana@hds.test" not in line
    assert json.loads(line)["exception"]  # still valid JSON with the exception present


def test_end_to_end_handler_output_is_one_clean_json_line() -> None:
    lines: list[str] = []

    class Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            lines.append(self.format(record))

    handler = Capture()
    handler.addFilter(SensitiveDataFilter())
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger("hds.test.redaction")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        logger.info("login %s by %s", '{"password": "hunter2"}', "alice@hds.test")
    finally:
        logger.removeHandler(handler)
    assert len(lines) == 1
    assert "hunter2" not in lines[0] and "alice@hds.test" not in lines[0]
    assert "a***@hds.test" in json.loads(lines[0])["message"]
