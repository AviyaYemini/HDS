"""The review ledger: one row per finding, and the status shows how many are open (harness item 7).

``specs/<feature>/review-ledger.md`` holds a Markdown table: id, source, finding and status (open,
fixed or won't-fix). Ledgers started before 2026-11-07 have a fifth column, the fix commit, and are
still read. /hds-review appends to it; /hds-status counts the open rows and, while any are open,
says to close them before the group is done.

Covers: none directly; the harness's review ledger (.claude/hds/harness-log.md, item 7), and its
four-column shape (.claude/hds/process-cuts-proposal.md, cut 1).
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

# The shape from 2026-11-07: no fix-commit column; the fix commit names the row in its message.
LEDGER_V2 = """# Review ledger: 007

| ID | Source | Finding | Status |
| --- | --- | --- | --- |
| R1 | /code-review high | the parser skips punctuation | fixed |
| R2 | Reviewer | provenance on the runner | open |
| R3 | Reviewer | Node 20 actions | won't-fix |
| R4 | /hds-review | a missing test | Open |
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


def test_the_template_is_an_empty_table_with_the_four_columns(hooks: ModuleType) -> None:
    template = (ROOT / ".claude" / "hds" / "review-ledger-template.md").read_text()
    assert "| ID | Source | Finding | Status |\n" in template
    assert "Fix commit" not in template
    assert hooks.ledger_counts(ROOT / ".claude" / "hds" / "review-ledger-template.md") == (0, 0)


def test_a_pipe_inside_a_finding_does_not_move_the_status(
    hooks: ModuleType, tmp_path: Path
) -> None:
    # The status is the second-to-last cell, so a quoted `|` in the finding shifts nothing (C3).
    ledger = tmp_path / "review-ledger.md"
    ledger.write_text(
        LEDGER.replace("the parser skips punctuation | fixed", "`cat <<EOF | sh` passes | open")
    )
    assert hooks.ledger_counts(ledger) == (3, 4)


def test_a_four_column_ledger_is_counted_from_its_last_cell(
    hooks: ModuleType, tmp_path: Path
) -> None:
    ledger = tmp_path / "review-ledger.md"
    ledger.write_text(LEDGER_V2)
    assert hooks.ledger_counts(ledger) == (2, 4)
    line, blocking = hooks.ledger_line(ledger)
    assert "2 open of 4" in line
    assert blocking


def test_a_pipe_inside_a_finding_does_not_move_a_four_column_status(
    hooks: ModuleType, tmp_path: Path
) -> None:
    ledger = tmp_path / "review-ledger.md"
    ledger.write_text(
        LEDGER_V2.replace("the parser skips punctuation | fixed", "`cat <<EOF | sh` passes | open")
    )
    assert hooks.ledger_counts(ledger) == (3, 4)


def test_a_fixed_finding_naming_a_status_word_is_not_read_as_open(
    hooks: ModuleType, tmp_path: Path
) -> None:
    # Under the four-column header only the last cell is the status, not a finding that ends "open".
    ledger = tmp_path / "review-ledger.md"
    ledger.write_text(LEDGER_V2.replace("the parser skips punctuation", "a port left | open"))
    assert hooks.ledger_counts(ledger) == (2, 4)


def test_a_wont_fix_with_its_reason_in_the_status_cell_is_counted(
    hooks: ModuleType, tmp_path: Path
) -> None:
    # 006's ledger has two such rows; before, they were not counted at all (203 of 205). The two
    # tables in one file also show each header setting the shape of the rows below it.
    ledger = tmp_path / "review-ledger.md"
    ledger.write_text(
        LEDGER.replace("| won't-fix | |", "| won't-fix: the check needs the download | |")
        + "\n"
        + LEDGER_V2.replace("| won't-fix |", "| Won't-fix: Node 20 is what the runner has |")
    )
    assert hooks.ledger_counts(ledger) == (4, 8)


def test_rows_before_any_header_are_read_in_either_shape(hooks: ModuleType, tmp_path: Path) -> None:
    rows = [
        line for text in (LEDGER, LEDGER_V2) for line in text.splitlines() if line.startswith("| R")
    ]
    ledger = tmp_path / "review-ledger.md"
    ledger.write_text("\n".join(rows) + "\n")
    assert hooks.ledger_counts(ledger) == (4, 8)
