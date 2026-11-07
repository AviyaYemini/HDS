"""The hooks judge the checkout an event happened in, also from a linked worktree.

Claude Code sets ``CLAUDE_PROJECT_DIR`` to the project's main checkout, also for a session in a
linked worktree (the desktop app makes one per session, under ``.claude/worktrees/``). The hooks
used to read everything from there: a commit in a feature worktree was refused because the main
checkout was on ``main``, and a worktree's files had paths under ``.claude/worktrees/``, so the
edit gates and the Stop gate never saw them. They now read the event's ``cwd``.

Covers: none directly; the harness's hooks (``.claude/hooks/hds_hooks.py``, ``event_root``).
"""

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[4]
HOOK = ROOT / ".claude" / "hooks" / "hds_hooks.py"
GIT = ["git", "-c", "user.name=check", "-c", "user.email=check@example.invalid"]


def git(cwd: Path, *args: str) -> None:
    """Run a git command in ``cwd`` and fail the test if it fails."""
    subprocess.run([*GIT, *args], cwd=cwd, check=True, capture_output=True)


@pytest.fixture
def checkouts(tmp_path: Path) -> tuple[Path, Path]:
    """A main checkout on ``main`` and a linked worktree of it on ``feature``."""
    main = tmp_path / "project"
    main.mkdir()
    git(main, "init", "-q", "-b", "main")
    (main / "README.md").write_text("base\n")
    git(main, "add", "README.md")
    git(main, "commit", "-q", "-m", "chore: base")
    worktree = tmp_path / "project-feature"
    git(main, "worktree", "add", "-q", "-b", "feature", str(worktree))
    return main, worktree


def hook(
    event: str, project_dir: Path, payload: dict[str, object]
) -> subprocess.CompletedProcess[str]:
    """Run the hook as Claude Code does: the project dir in the environment, the event on stdin."""
    env = {**os.environ, "CLAUDE_PROJECT_DIR": str(project_dir)}
    env.pop("HDS_ALLOW_MAIN", None)
    env.pop("HDS_SKIP_GATE", None)
    return subprocess.run(
        [sys.executable, str(HOOK), event],
        input=json.dumps(payload),
        env=env,
        cwd=project_dir,
        capture_output=True,
        text=True,
        check=False,
    )


def commit_event(
    cwd: Path, command: str = "git commit -m 'feat(api): add a thing'"
) -> dict[str, object]:
    """A Bash tool call that commits, made in ``cwd``."""
    return {"cwd": str(cwd), "tool_input": {"command": command}}


MESSAGE = "-m 'feat(api): add a thing'"


@pytest.mark.parametrize(
    "command",
    [
        "git -C {target} commit " + MESSAGE,
        "cd {target} && git commit " + MESSAGE,
        "cd {target}; git status; git commit " + MESSAGE,
        "git --git-dir={target}/.git --work-tree={target} commit " + MESSAGE,
    ],
    ids=["git -C", "cd &&", "cd ;", "--git-dir"],
)
def test_a_commit_aimed_at_a_checkout_on_main_is_refused_from_a_feature_worktree(
    checkouts: tuple[Path, Path], command: str
) -> None:
    # Judged by the session's own checkout alone, these passed: the worktree is on `feature`.
    main, worktree = checkouts
    result = hook("pre-bash", main, commit_event(worktree, command.format(target=main)))
    assert result.returncode == 2
    assert "do not commit on main" in result.stderr


@pytest.mark.parametrize(
    "command",
    ["git -C {target} commit " + MESSAGE, "cd {target} && git commit " + MESSAGE],
    ids=["git -C", "cd &&"],
)
def test_a_commit_aimed_at_a_feature_worktree_is_allowed_from_a_checkout_on_main(
    checkouts: tuple[Path, Path], command: str
) -> None:
    main, worktree = checkouts
    assert (
        hook("pre-bash", main, commit_event(main, command.format(target=worktree))).returncode == 0
    )


def test_a_command_that_only_mentions_commit_on_main_is_not_a_commit(
    checkouts: tuple[Path, Path],
) -> None:
    main, _ = checkouts
    assert hook("pre-bash", main, commit_event(main, "git log --grep commit")).returncode == 0


def test_a_commit_in_a_feature_worktree_is_allowed_while_the_main_checkout_is_on_main(
    checkouts: tuple[Path, Path],
) -> None:
    main, worktree = checkouts
    assert hook("pre-bash", main, commit_event(worktree)).returncode == 0


def test_a_commit_in_the_main_checkout_on_main_is_still_refused(
    checkouts: tuple[Path, Path],
) -> None:
    main, _ = checkouts
    result = hook("pre-bash", main, commit_event(main))
    assert result.returncode == 2
    assert "do not commit on main" in result.stderr


def test_a_commit_in_a_worktree_on_main_is_refused_whatever_the_main_checkout_is_on(
    checkouts: tuple[Path, Path], tmp_path: Path
) -> None:
    main, _ = checkouts
    git(main, "switch", "-q", "-c", "develop")
    on_main = tmp_path / "project-main"
    git(main, "worktree", "add", "-q", str(on_main), "main")
    result = hook("pre-bash", main, commit_event(on_main))
    assert result.returncode == 2
    assert "do not commit on main" in result.stderr


def test_a_worktree_file_meets_the_edit_gates_as_its_own_path(
    checkouts: tuple[Path, Path],
) -> None:
    # packages/<x>/generated is never hand-edited (principle 5); seen from the main checkout, the
    # worktree's file was outside it and passed.
    main, worktree = checkouts
    target = worktree / "packages" / "api-client" / "src" / "generated" / "client.ts"
    event: dict[str, object] = {
        "cwd": str(worktree),
        "tool_input": {"file_path": str(target), "content": "x"},
    }
    result = hook("pre-edit", main, event)
    assert result.returncode == 2
    assert "generated API client" in result.stderr


@pytest.fixture
def hooks() -> ModuleType:
    """The hook script, imported with the repository as its root, as Claude Code runs it."""
    previous = os.environ.get("CLAUDE_PROJECT_DIR")
    os.environ["CLAUDE_PROJECT_DIR"] = str(ROOT)
    try:
        spec = importlib.util.spec_from_file_location("hds_hooks_worktree_test", HOOK)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    finally:
        if previous is None:
            os.environ.pop("CLAUDE_PROJECT_DIR", None)
        else:
            os.environ["CLAUDE_PROJECT_DIR"] = previous
    return module


def test_the_events_root_is_the_top_of_its_cwds_checkout(
    hooks: ModuleType, checkouts: tuple[Path, Path]
) -> None:
    _, worktree = checkouts
    (worktree / "apps" / "api").mkdir(parents=True)
    assert hooks.event_root({"cwd": str(worktree / "apps" / "api")}) == worktree.resolve()


def test_a_cwd_outside_any_checkout_keeps_the_project_dir(
    hooks: ModuleType, tmp_path: Path
) -> None:
    outside = tmp_path / "not-a-repository"
    outside.mkdir()
    assert hooks.event_root({"cwd": str(outside)}) == hooks.ROOT
