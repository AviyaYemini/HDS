"""The end-of-turn gate runs the quick checks for what changed, and never a database test.

The Stop hook used to look for pyproject.toml at the root or in backend/, so it never ran. It now
checks apps/api: ruff and the format check on the changed Python files, mypy --strict, the unit
tests, and the property tests when the pure packages or the property tests changed. Never the
integration, contract or concurrency suites: they need a database and take about 25 minutes. A
failing gate does not loop: with stop_hook_active it lets Claude stop.

Covers: none directly; the harness's end-of-turn gate (.claude/hds/harness-log.md, item 1).
"""

import importlib.util
import os
import sys
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
