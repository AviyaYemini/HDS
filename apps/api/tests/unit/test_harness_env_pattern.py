"""The harness blocks reads of real settings files but not the example files (harness fix).

The pre-bash hook exists to keep real secrets out of reach (principle 6). Its ".env" pattern also
matched ".env.example" and ".env.sample", which are the files meant to be read, so contributors
and Claude could not even look at the documented settings. These tests drive the real hook script
the way Claude Code does: a JSON command on standard input, exit status 2 meaning "blocked".

Covers: none directly; tooling guard for Constitution VI.
"""

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

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
        'sh -c "cat .env"',  # a string a shell runs is a command, not a mention
        'eval "cat .env"',
    ],
)
def test_real_commands_next_to_or_inside_a_message_stay_blocked(command: str) -> None:
    assert blocked(command)


# ── Reading a settings file is blocked; mentioning one is not (harness item 5) ──────────────────


@pytest.mark.parametrize(
    "command",
    [
        "rsync -a --exclude .env src/ dst/",
        "rsync -a --exclude=.env src/ dst/",
        "git ls-files .env",
        'grep -rn "\\.env" docs/',
        "ls -la .env",
        'echo "cat .env"',  # printed text, not a read (this case used to be blocked)
        "docker compose -f infra/docker-compose.yml --env-file .env up -d",
        "grep -rn JWT_SECRET_KEY --exclude='.env*' .",  # a recursive grep that leaves them out
        "git grep -n JWT_SECRET_KEY",  # tracked files only
        "grep -rn JWT_SECRET_KEY docs/",  # a folder that holds no settings file
        "rg JWT_SECRET_KEY",  # rg skips git-ignored files by default
        "cat > notes.md <<'EOF'\ncp .env apps/api/.env\nEOF",  # a heredoc written to a file
        "python3 - <<'EOF'\ns = open('README.md').read()\ns += 'cp .env apps/api/.env'\nEOF",
    ],
)
def test_a_mention_of_a_settings_file_is_not_blocked(command: str) -> None:
    assert not blocked(command)


@pytest.mark.parametrize(
    "command",
    [
        "cat .env*",
        "cat < .env",
        "less ./.env",
        "head ../.env",
        "rsync -a .env backup/",
        "mv .env /tmp/settings",
        "awk 1 .env",
        "sed -n p .env.local",
        "python3 -c \"print(open('.env').read())\"",
        "python3 - <<'EOF'\nprint(open('.env').read())\nEOF",
        "bash <<'EOF'\ncat .env\nEOF",
        "docker run --rm -v $PWD:/repo alpine cat /repo/.env",
        "cat > f <<EOF\n$(cat .env)\nEOF",  # an unquoted heredoc still runs its substitutions
        "cat 'unbalanced .env",  # cannot be split into words: the old pattern decides
        # A shell's script after any flag cluster with c, and -o options (review G1).
        "bash -lc 'cat .env'",
        "sh -ec 'cat .env'",
        "bash -o pipefail -c 'cat .env'",
        "ssh host bash -c 'cat .env'",
        # A heredoc piped into a shell, or fed to an interpreter behind uv run or docker run.
        "cat <<'EOF' | sh\ncat .env\nEOF",
        "uv run python - <<'EOF'\nprint(open('.env').read())\nEOF",
        "docker run -i img python - <<'EOF'\nprint(open('.env').read())\nEOF",
        # Writing over a real settings file, as pre_edit already refuses for Write and Edit.
        "cat .env.example > .env",
        'sed "s/^KEY=.*/KEY=new/" .env.example > .env',
        "echo JWT_SECRET_KEY=x > .env",
        "echo x >> .env.local",
        "printf x | tee .env",
        # A search that would print a real key (review G2).
        "grep -rn JWT_SECRET_KEY .",
        "grep -R JWT_SECRET_KEY",
        "rg --hidden JWT_SECRET_KEY",
        "rg -uu JWT_SECRET_KEY .",
        "find . -name .env -exec cat {} +",
    ],
)
def test_reading_or_copying_a_settings_file_is_blocked(command: str) -> None:
    assert blocked(command)


# Commands the old pattern blocked although they read no real settings file, which the new guard
# lets through on purpose. Every other command the old pattern blocks must stay blocked, so a later
# rewrite cannot quietly loosen the guard (review G1).
INTENDED_MENTIONS = {
    "grep -n x .env.example",  # the example file, which stays readable
}


def old_pattern_corpus() -> list[str]:
    """Commands built from the readers and files the old pattern knew, in common forms."""
    readers = ["cat", "less", "more", "head", "tail", "grep x", "source", "cp", "scp"]
    files = [".env", ".env.local", ".env.production", ".env.prod"]
    forms = [
        "{r} {f}",
        "{r} -n {f}",
        "true && {r} {f}",
        "{r} {f} | wc -l",
        "bash -lc '{r} {f}'",
        "sh -ec '{r} {f}'",
        "bash -o pipefail -c '{r} {f}'",
        "env A=1 {r} {f}",
        "{r} {f} dest/",
    ]
    return [form.format(r=r, f=f) for r in readers for f in files for form in forms]


def test_every_command_the_old_pattern_blocked_stays_blocked() -> None:
    old = hooks_module()._SETTINGS_FALLBACK
    reads = hooks_module().reads_settings_file
    corpus = [c for c in old_pattern_corpus() if old.search(c) and c not in INTENDED_MENTIONS]
    assert len(corpus) > 200
    let_through = [c for c in corpus if not reads(c)]
    assert not let_through, let_through


def hooks_module() -> Any:
    """The hook script as a module (loaded once), with the repository as its root."""
    global _HOOKS
    if _HOOKS is None:
        previous = os.environ.get("CLAUDE_PROJECT_DIR")
        os.environ["CLAUDE_PROJECT_DIR"] = str(ROOT)
        try:
            spec = importlib.util.spec_from_file_location("hds_hooks_corpus", HOOK)
            assert spec is not None and spec.loader is not None
            module = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = module
            spec.loader.exec_module(module)
            _HOOKS = module
        finally:
            if previous is None:
                os.environ.pop("CLAUDE_PROJECT_DIR", None)
            else:
                os.environ["CLAUDE_PROJECT_DIR"] = previous
    return _HOOKS


_HOOKS: Any = None


# ── gitleaks must redact, and never scan the whole folder (harness item 6) ───────────────────

GITLEAKS_IMAGE = "ghcr.io/gitleaks/gitleaks:v8.30.1@sha256:c00b6bd0aeb3071cbcb7"


@pytest.mark.parametrize(
    "command",
    [
        "gitleaks git --no-banner .",  # no --redact
        f"docker run --rm -v $PWD:/repo -w /repo {GITLEAKS_IMAGE} git --no-banner -v .",
        # The run that printed a key: dir with several paths scanned the whole folder.
        f"docker run --rm -v $PWD:/repo -w /repo {GITLEAKS_IMAGE} dir --no-banner --redact "
        "Makefile scripts infra README.md",
        "gitleaks dir --redact .",
        "gitleaks dir --redact",
        f"docker run --rm -v $PWD:/repo {GITLEAKS_IMAGE} dir --redact /repo",
        "gitleaks detect --no-git --redact --source .",
        # Behind a wrapper or an environment assignment (review G3).
        "time gitleaks dir apps",
        "GITLEAKS_CONFIG=x gitleaks dir apps",
        "env gitleaks git .",
        "nohup gitleaks git . &",
    ],
)
def test_an_unredacted_or_whole_folder_gitleaks_run_is_blocked(command: str) -> None:
    assert blocked(command)


@pytest.mark.parametrize(
    "command",
    [
        "gitleaks git --no-banner --redact --log-opts=origin/main..HEAD .",
        f"docker run --rm -v $PWD:/repo:ro -w /repo {GITLEAKS_IMAGE} git --no-banner --redact .",
        "gitleaks dir --redact apps/api/src",
        "grep -n gitleaks .github/workflows/ci.yml",
    ],
)
def test_a_redacted_commit_scan_or_a_narrow_dir_scan_is_allowed(command: str) -> None:
    assert not blocked(command)


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
