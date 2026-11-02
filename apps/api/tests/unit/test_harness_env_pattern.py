"""The harness blocks reads of real settings files but not the example files (harness fix).

The pre-bash hook exists to keep real secrets out of reach (principle 6). Its ".env" pattern also
matched ".env.example" and ".env.sample", which are the files meant to be read, so contributors
and Claude could not even look at the documented settings. These tests drive the real hook script
the way Claude Code does: a JSON command on standard input, exit status 2 meaning "blocked".

Covers: none directly; tooling guard for Constitution VI.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[4]
HOOK = ROOT / ".claude" / "hooks" / "hds_hooks.py"


def blocked(command: str) -> bool:
    """Run the pre-bash hook on ``command`` and report whether it blocked it."""
    result = subprocess.run(
        [sys.executable, str(HOOK), "pre-bash"],
        input=json.dumps({"tool_input": {"command": command}}),
        capture_output=True,
        text=True,
        env={**os.environ, "CLAUDE_PROJECT_DIR": str(ROOT)},
        cwd=ROOT,
        check=False,
    )
    return result.returncode == 2


@pytest.mark.parametrize(
    "command",
    [
        "cat .env.example",
        "grep -n JWT_SECRET_KEY .env.example",
        "head -5 .env.sample",
        "less .env.example",
        "tail -3 apps/api/.env.example",
    ],
)
def test_example_files_can_be_read(command: str) -> None:
    assert not blocked(command)


@pytest.mark.parametrize(
    "command",
    [
        "cat .env",
        "cat .env.local",
        "cat .env.production",
        "grep SECRET .env.prod",
        "head -2 .env",
        "source .env",
        "cp .env .env.backup",
        "cat .env.example .env",  # an example next to a real file is still a read of the real one
        "cat .env.sample .env.local",
    ],
)
def test_real_settings_files_stay_blocked(command: str) -> None:
    assert blocked(command)


def test_other_dangerous_commands_stay_blocked() -> None:
    assert blocked("git reset --hard HEAD~1")
    assert blocked("sudo ls")


def test_ordinary_commands_are_not_blocked() -> None:
    assert not blocked("ls apps/api")
    assert not blocked("uv run pytest tests/unit")
