"""The end-of-turn gate runs the quick checks for what changed, and never a database test.

The Stop hook used to look for pyproject.toml at the root or in backend/, so it never ran. It now
checks apps/api: ruff and the format check on the changed Python files, mypy --strict, the unit
tests, and the property tests when the pure packages or the property tests changed. Never the
integration, contract or concurrency suites: they need a database and take about 25 minutes. A
failing gate does not loop: with stop_hook_active it lets Claude stop.

Covers: none directly; the harness's end-of-turn gate (.claude/hds/harness-log.md, item 1).
"""

import importlib.util
import json
import os
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[4]
HOOK = ROOT / ".claude" / "hooks" / "hds_hooks.py"


@pytest.fixture(scope="module")
def hooks() -> ModuleType:
    """The hook script, imported with the repository as its root, as Claude Code runs it.

    The hook takes its root from CLAUDE_PROJECT_DIR (or the current folder, which for pytest is
    apps/api), so the variable is set while the module loads.
    """
    previous = os.environ.get("CLAUDE_PROJECT_DIR")
    os.environ["CLAUDE_PROJECT_DIR"] = str(ROOT)
    try:
        spec = importlib.util.spec_from_file_location("hds_hooks_under_test", HOOK)
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


def labels(plan: list[tuple[str, list[str]]]) -> list[str]:
    """The step names of a plan."""
    return [label for label, _ in plan]


def command(plan: list[tuple[str, list[str]]], label: str) -> list[str]:
    """The command of the step named ``label``."""
    return next(cmd for name, cmd in plan if name == label)


def test_nothing_runs_when_no_python_changed(hooks: ModuleType) -> None:
    assert hooks.stop_plan(["README.md", "apps/client/src/App.tsx"]) == []


def test_a_changed_api_module_gets_lint_format_types_and_unit_tests(hooks: ModuleType) -> None:
    plan = hooks.stop_plan(["apps/api/src/hds/core/config.py"])
    assert labels(plan) == ["ruff", "ruff format", "mypy", "unit tests"]
    assert command(plan, "ruff")[-1] == "src/hds/core/config.py"
    assert command(plan, "ruff format")[-1] == "src/hds/core/config.py"
    assert "--strict" in command(plan, "mypy")
    assert command(plan, "unit tests")[-1] == "tests/unit/"


def test_the_pure_packages_add_the_property_tests(hooks: ModuleType) -> None:
    for changed in (
        "apps/api/src/hds/payroll/price.py",
        "apps/api/src/hds/scheduling/generate.py",
        "apps/api/tests/property/test_price_properties.py",
    ):
        assert "property tests" in labels(hooks.stop_plan([changed])), changed


def test_no_step_ever_runs_a_database_suite(hooks: ModuleType) -> None:
    plan = hooks.stop_plan(
        [
            "apps/api/src/hds/payroll/price.py",
            "apps/api/tests/integration/test_health_endpoints.py",
            "apps/api/tests/conftest.py",
        ]
    )
    # Only a pytest step can run a suite; ruff is handed changed files by path, which is fine.
    pytest_args = [arg for _, cmd in plan if "pytest" in cmd for arg in cmd[cmd.index("pytest") :]]
    for suite in ("tests/integration", "tests/contract", "tests/concurrency", "tests/"):
        assert suite not in pytest_args
    assert {"tests/unit/", "tests/property/"} >= {a for a in pytest_args if a.startswith("tests")}


def test_a_changed_hook_runs_the_unit_tests_without_linting_apps_api(hooks: ModuleType) -> None:
    plan = hooks.stop_plan([".claude/hooks/hds_hooks.py"])
    assert labels(plan) == ["unit tests"]


def test_a_deleted_file_is_not_handed_to_ruff(hooks: ModuleType) -> None:
    plan = hooks.stop_plan(["apps/api/src/hds/no_such_module_anymore.py"])
    assert "ruff" not in labels(plan)
    assert "mypy" in labels(plan)


def test_the_checks_use_the_dev_extra_so_ruff_and_mypy_are_there(hooks: ModuleType) -> None:
    for _, cmd in hooks.stop_plan(["apps/api/src/hds/core/config.py"]):
        assert cmd[:4] == ["uv", "run", "--extra", "dev"]


def test_a_gate_that_already_failed_once_lets_claude_stop(hooks: ModuleType) -> None:
    assert hooks.stop({"stop_hook_active": True}) == 0


def test_changed_paths_are_read_from_porcelain_status_including_renames(hooks: ModuleType) -> None:
    status = (
        " M apps/api/src/a.py\n?? apps/api/tests/unit/test_b.py\nR  old.py -> apps/api/src/c.py"
    )
    assert hooks.changed_paths(status) == [
        "apps/api/src/a.py",
        "apps/api/tests/unit/test_b.py",
        "apps/api/src/c.py",
    ]


def recorder(calls: list[list[str]]) -> Callable[..., tuple[int, str]]:
    """A stand-in for the hook's run() that records each command and succeeds."""

    def run(cmd: list[str], **_: object) -> tuple[int, str]:
        calls.append(cmd)
        return 0, ""

    return run


# ── Only this session's edits, within one deadline (review G4) ─────────────────────────────────


def test_edits_are_recorded_per_session_and_cleared(
    hooks: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(hooks, "STATE_DIR", tmp_path / "state")
    hooks.record_edit("session-a", "apps/api/src/hds/core/config.py")
    hooks.record_edit("session-a", "apps/api/src/hds/core/config.py")
    hooks.record_edit("session-b", "apps/api/src/hds/main.py")
    assert hooks.session_edits("session-a") == ["apps/api/src/hds/core/config.py"]
    assert hooks.session_edits("session-b") == ["apps/api/src/hds/main.py"]
    hooks.clear_edits("session-a")
    assert hooks.session_edits("session-a") == []
    assert hooks.session_edits("session-b") == ["apps/api/src/hds/main.py"]


def test_a_session_with_no_edits_runs_nothing(
    hooks: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(hooks, "STATE_DIR", tmp_path / "state")
    calls: list[list[str]] = []
    monkeypatch.setattr(hooks, "run", recorder(calls))
    assert hooks.stop({"session_id": "quiet-session"}) == 0
    assert calls == []  # not even git status: another session's changes are not this one's


def test_the_gate_checks_this_session_s_edits_and_clears_them_on_a_pass(
    hooks: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(hooks, "STATE_DIR", tmp_path / "state")
    hooks.record_edit("busy-session", "apps/api/src/hds/core/config.py")
    timeouts: list[float] = []

    def fake_run(cmd: list[str], timeout: float = 30, cwd: object = None) -> tuple[int, str]:
        timeouts.append(timeout)
        return 0, ""

    monkeypatch.setattr(hooks, "run", fake_run)
    assert hooks.stop({"session_id": "busy-session"}) == 0
    assert len(timeouts) == 4  # ruff, format, mypy, unit tests
    assert all(t <= hooks.STOP_BUDGET_SECONDS for t in timeouts)
    assert hooks.session_edits("busy-session") == []


def test_a_failing_gate_keeps_the_edits_for_the_next_turn(
    hooks: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(hooks, "STATE_DIR", tmp_path / "state")
    hooks.record_edit("failing-session", "apps/api/src/hds/core/config.py")
    monkeypatch.setattr(hooks, "run", lambda cmd, timeout=30, cwd=None: (1, "F401"))
    assert hooks.stop({"session_id": "failing-session"}) == 2
    assert hooks.session_edits("failing-session") == ["apps/api/src/hds/core/config.py"]


def test_the_steps_share_one_deadline_inside_the_hook_s_timeout(hooks: ModuleType) -> None:
    settings = json.loads((ROOT / ".claude" / "settings.json").read_text())
    timeout = settings["hooks"]["Stop"][0]["hooks"][0]["timeout"]
    assert hooks.STOP_BUDGET_SECONDS < timeout


def test_without_a_session_the_whole_tree_is_read_with_new_folders_listed(
    hooks: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[list[str]] = []
    monkeypatch.setattr(hooks, "run", recorder(seen))
    hooks.stop({})
    assert seen and seen[0][:3] == ["git", "status", "--porcelain"]
    assert "--untracked-files=all" in seen[0]


def test_in_a_linked_worktree_edits_are_recorded_in_its_own_git_folder(tmp_path: Path) -> None:
    # In a worktree, .git is a file ("gitdir: ..."), not a folder; the state must go to the
    # worktree's own git folder, or the gate is silently off there (review H1).
    main = tmp_path / "main"
    subprocess.run(["git", "init", "-q", "-b", "main", str(main)], check=True)
    git = [
        "git",
        "-c",
        "user.name=t",
        "-c",
        "user.email=t@example.com",
        "-c",
        "commit.gpgsign=false",
    ]
    subprocess.run(
        [*git, "commit", "-q", "--allow-empty", "-m", "chore: start"], cwd=main, check=True
    )
    tree = tmp_path / "tree"
    subprocess.run(["git", "worktree", "add", "-q", "-b", "work", str(tree)], cwd=main, check=True)
    assert (tree / ".git").is_file()
    (tree / "notes.md").write_text("x\n")

    hook_input = {
        "session_id": "worktree-session",
        "tool_input": {"file_path": str(tree / "notes.md")},
    }
    subprocess.run(
        [sys.executable, str(HOOK), "post-edit"],
        input=json.dumps(hook_input),
        text=True,
        capture_output=True,
        env={**os.environ, "CLAUDE_PROJECT_DIR": str(tree)},
        cwd=tree,
        check=False,
    )
    git_dir = subprocess.run(
        ["git", "rev-parse", "--absolute-git-dir"],
        cwd=tree,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    recorded = Path(git_dir) / "hds-hooks" / "edits-worktree-session"
    assert recorded.read_text().splitlines() == ["notes.md"]
    assert not (main / ".git" / "hds-hooks" / "edits-worktree-session").exists()


def test_the_gate_leaves_out_wall_clock_tests_and_keeps_the_default_exclusions(
    hooks: ModuleType,
) -> None:
    # A hard time limit flakes under load (it did three times beside the database suite); CI and
    # make ci-local run those tests, the gate does not. A second -m replaces the addopts one, so the
    # gate repeats the defaults.
    plan = hooks.stop_plan(["apps/api/src/hds/payroll/price.py"])
    for label in ("unit tests", "property tests"):
        cmd = command(plan, label)
        expression = cmd[cmd.index("-m") + 1]
        assert expression == "not timing and not benchmark and not wall_clock"
