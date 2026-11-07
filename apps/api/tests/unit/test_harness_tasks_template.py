"""The tasks template override: Spec Kit's own template plus HDS's group close, and nothing else.

``/speckit-tasks`` reads ``.specify/templates/overrides/tasks-template.md`` before the bundled
``.specify/templates/tasks-template.md`` (``resolve_template`` in
``.specify/scripts/bash/common.sh``), so every new feature's ``tasks.md`` carries the owner's group
close. An override replaces the bundled file whole, so a Spec Kit upgrade that changes the bundled
template would be hidden by a stale copy; these tests fail then, and the override is remade from
the new template plus the same block.

Covers: none directly; the harness's group close (.claude/hds/process-cuts-proposal.md, decided
2026-11-07).
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
TEMPLATES = ROOT / ".specify" / "templates"
BUNDLED = TEMPLATES / "tasks-template.md"
OVERRIDE = TEMPLATES / "overrides" / "tasks-template.md"
BLOCK_START = "**Each group ends with** (HDS"
BLOCK_END = "The full suite runs only in CI.\n"


def _block(text: str) -> str:
    """The HDS block in ``text``, from its first line to its last, followed by its blank line."""
    start = text.index(BLOCK_START)
    end = text.index(BLOCK_END, start) + len(BLOCK_END)
    return text[start : end + 1]


def test_the_override_is_the_bundled_template_plus_the_group_close() -> None:
    override = OVERRIDE.read_text()
    assert override.replace(_block(override), "") == BUNDLED.read_text()


def test_the_group_close_sits_right_after_the_organization_line() -> None:
    override = OVERRIDE.read_text()
    organization = next(
        line for line in override.splitlines() if line.startswith("**Organization**:")
    )
    assert organization + "\n\n" + BLOCK_START in override


def test_the_group_close_keeps_the_database_run_and_the_full_suite_out_of_each_group() -> None:
    block = _block(OVERRIDE.read_text())
    per_group, pr_ready = block.split("**When the pull request is ready to push, once**")
    assert "ci-local-db" not in per_group
    assert "ci-local-full" not in per_group
    assert "make ci-local-db" in pr_ready
    assert "/hds-review" in pr_ready
    assert "/code-review high" in per_group
    assert "at most three" in per_group
