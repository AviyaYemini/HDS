"""The review ledger: one row per finding, and the status shows how many are open (harness item 7).

``specs/<feature>/review-ledger.md`` holds a Markdown table: id, source, finding, status (open,
fixed or won't-fix) and the fix commit. /hds-review appends to it; /hds-status counts the open rows
and, while any are open, says to close them before the group is done.

Covers: none directly; the harness's review ledger (.claude/hds/harness-log.md, item 7).
"""

import importlib.util
import os
import sys
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[4]
HOOK = ROOT / ".claude" / "hooks" / "hds_hooks.py"

LEDGER = """# Review ledger: 006

| ID | Source | Finding | Status | Fix commit |
| --- | --- | --- | --- | --- |
| R1 | /code-review high | the parser skips punctuation | fixed | 86bb433 |
| R2 | HDS helper | provenance on the runner | open | |
| R3 | HDS helper | Node 20 actions | won't-fix | |
| R4 | /hds-review | a missing test | Open | |
"""


@pytest.fixture(scope="module")
def hooks() -> ModuleType:
    """The hook script, imported with the repository as its root, as Claude Code runs it."""
    previous = os.environ.get("CLAUDE_PROJECT_DIR")
    os.environ["CLAUDE_PROJECT_DIR"] = str(ROOT)
    try:
        spec = importlib.util.spec_from_file_location("hds_hooks_ledger_test", HOOK)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
    finally:
        if previous is None:
            os.environ.pop("CLAUDE_PROJECT_DIR", None)
        else:
            os.environ["CLAUDE_PROJECT_DIR"] = previous
    return module


def test_open_rows_are_counted_whatever_their_case(hooks: ModuleType, tmp_path: Path) -> None:
    ledger = tmp_path / "review-ledger.md"
    ledger.write_text(LEDGER)
    assert hooks.ledger_counts(ledger) == (2, 4)


def test_no_ledger_counts_as_nothing_open(hooks: ModuleType, tmp_path: Path) -> None:
    assert hooks.ledger_counts(tmp_path / "review-ledger.md") == (0, 0)


def test_the_status_names_the_open_rows_and_says_to_close_them(
    hooks: ModuleType, tmp_path: Path
) -> None:
    ledger = tmp_path / "review-ledger.md"
    ledger.write_text(LEDGER)
    line, blocking = hooks.ledger_line(ledger)
    assert "2 open of 4" in line
    assert blocking


def test_a_ledger_with_every_row_closed_does_not_hold_the_group(
    hooks: ModuleType, tmp_path: Path
) -> None:
    ledger = tmp_path / "review-ledger.md"
    ledger.write_text(LEDGER.replace("| open |", "| fixed |").replace("| Open |", "| fixed |"))
    line, blocking = hooks.ledger_line(ledger)
    assert "0 open of 4" in line
    assert not blocking


def test_the_template_is_an_empty_table_with_the_five_columns(hooks: ModuleType) -> None:
    template = (ROOT / ".claude" / "hds" / "review-ledger-template.md").read_text()
    assert "| ID | Source | Finding | Status | Fix commit |" in template
    assert hooks.ledger_counts(ROOT / ".claude" / "hds" / "review-ledger-template.md") == (0, 0)
