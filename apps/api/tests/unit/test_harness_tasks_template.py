"""The tasks template override: Spec Kit's own template plus HDS's group close, and nothing else.

``/speckit-tasks`` reads ``.specify/templates/overrides/tasks-template.md`` before the bundled
``.specify/templates/tasks-template.md`` (``resolve_template`` in
``.specify/scripts/bash/common.sh``), so every new feature's ``tasks.md`` carries the owner's group
close. An override replaces the bundled file whole, so a Spec Kit upgrade that changes the bundled
template would be hidden by a stale copy; these tests fail then, and the override is remade from
the new template plus the same block.

Covers: none directly; the harness's group close (the owner's working rules of 2026-11-08 in
.claude/hds/CLAUDE.hds.md, replacing the close decided 2026-11-07).
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
TEMPLATES = ROOT / ".specify" / "templates"
BUNDLED = TEMPLATES / "tasks-template.md"
OVERRIDE = TEMPLATES / "overrides" / "tasks-template.md"
BLOCK_START = "**Each group ends with** (the owner's rules"
BLOCK_END = "catches line-level slips.\n"


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


def test_the_group_close_keeps_reviews_and_full_suites_out_of_each_group() -> None:
    block = _block(OVERRIDE.read_text())
    per_group, pr_ready = block.split("**When the pull request is ready to push, once**")
    assert "ci-local-db" not in per_group
    assert "ci-local-full" not in per_group
    assert "No `/code-review` at a group's close" in per_group
    assert "sign-in, pay, or who can see what" in per_group
    assert "one `/code-review`" in pr_ready
    assert "review-ledger.md" in pr_ready
    assert "No `make ci-local-full`," in pr_ready
    assert "full suites run only in CI" in pr_ready
