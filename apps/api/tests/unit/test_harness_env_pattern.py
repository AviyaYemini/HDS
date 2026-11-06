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


def run_hook(command: str, project: Path, **env: str) -> int:
    """Run the pre-bash hook on ``command`` in ``project``; its exit status (2 means blocked)."""
    result = subprocess.run(
        [sys.executable, str(HOOK), "pre-bash"],
        input=json.dumps({"tool_input": {"command": command}}),
        capture_output=True,
        text=True,
        env={**os.environ, "CLAUDE_PROJECT_DIR": str(project), **env},
        cwd=project,
        check=False,
    )
    return result.returncode


def blocked(command: str) -> bool:
    """Run the pre-bash hook on ``command`` and report whether it blocked it.

    ``HDS_ALLOW_MAIN`` is set, because the hook also refuses any commit on ``main``, and CI checks
    out ``main`` on every push to it: without it, the commit-message cases below failed there and
    only there. The main-branch rule has its own tests, on a repository of their own.
    """
    return run_hook(command, ROOT, HDS_ALLOW_MAIN="1") == 2


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


# ── Commit message text is data, not a command ──────────────────────────────────────────────────

HEREDOC_MESSAGE = """git commit -q -m "$(cat <<'EOF'
chore: explain why cat .env.local and git reset --hard are blocked

The pattern for sudo rm and grep SECRET .env.prod matched this text.
(Closing parenthesis, "quotes" and 1) numbering are all fine in a message.)

Co-Authored-By: Claude <noreply@example.com>
EOF
)\""""


@pytest.mark.parametrize(
    "command",
    [
        'git commit -m "chore: mentions cat .env in the text"',
        "git commit -m 'chore: also grep SECRET .env.local here'",
        'git commit --message="explains why git reset --hard is blocked"',
        'git commit --message "explains sudo rm"',
        'git commit -m"chore: attached cat .env text"',
        HEREDOC_MESSAGE,
    ],
)
def test_text_inside_a_commit_message_is_not_scanned(command: str) -> None:
    assert not blocked(command)


@pytest.mark.parametrize(
    "command",
    [
        'git commit -m "ok" && cat .env',  # a real read after the message
        'git commit -m "ok" && git reset --hard HEAD~1',
        HEREDOC_MESSAGE + " && cat .env.local",
        'git commit -m "$(cat .env)"',  # command substitution inside the message really runs
        'git commit -m "`cat .env`"',
        'git commit -m "$(sudo ls)"',
        'echo "cat .env"',  # outside a commit message the text is still matched
    ],
)
def test_real_commands_next_to_or_inside_a_message_stay_blocked(command: str) -> None:
    assert blocked(command)


def repository_on(branch: str, where: Path) -> Path:
    """A new git repository at ``where`` with ``branch`` checked out and one empty commit.

    The commit matters: on a branch with none, ``git rev-parse --abbrev-ref HEAD`` fails and the
    hook sees no branch at all.
    """
    subprocess.run(["git", "init", "-q", "-b", branch, str(where)], check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@example.com",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-q",
            "--allow-empty",
            "-m",
            "chore: start",
        ],
        cwd=where,
        check=True,
    )
    return where


@pytest.mark.parametrize("branch", ["main", "master"])
def test_a_commit_on_the_main_branch_is_blocked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, branch: str
) -> None:
    monkeypatch.delenv("HDS_ALLOW_MAIN", raising=False)
    project = repository_on(branch, tmp_path / "repo")
    assert run_hook('git commit -m "chore: x"', project) == 2


def test_a_commit_on_a_feature_branch_is_not_blocked_by_the_main_rule(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("HDS_ALLOW_MAIN", raising=False)
    project = repository_on("006-feature", tmp_path / "repo")
    assert run_hook('git commit -m "chore: x"', project) == 0
