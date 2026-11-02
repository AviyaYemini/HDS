#!/usr/bin/env python3
"""HDS 2.0 Claude Code hooks: enforce the constitution and the Spec Kit flow.

Usage (from .claude/settings.json):  hds_hooks.py <event>
Events: session-start, status, pre-edit, pre-bash, post-edit, stop

Exit code 2 blocks the action (PreToolUse) or sends the message back to Claude
(PostToolUse, Stop). Output on stdout in session-start becomes Claude's context.

Escape hatches (environment variables, set in your shell on purpose):
  HDS_SKIP_GATE=1       allow implementation edits without a tasks.md
  HDS_ALLOW_MAIN=1      allow Claude to commit on main/master
  HDS_SKIP_STOP_GATE=1  skip the lint/type/test gate when Claude finishes
"""
import ast
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(os.environ.get("CLAUDE_PROJECT_DIR") or os.getcwd()).resolve()
IMPL_ROOTS = ("backend", "apps", "packages", "infra")
CONVENTIONAL = re.compile(
    r"^(feat|fix|docs|chore|refactor|test|ci|build|perf|style|revert)(\([\w./-]+\))?!?: \S.*"
)


# ----------------------------------------------------------------- helpers
def read_input():
    try:
        return json.load(sys.stdin)
    except Exception:
        return {}


def run(cmd, timeout=30, cwd=None):
    try:
        p = subprocess.run(cmd, cwd=cwd or ROOT, capture_output=True, text=True, timeout=timeout)
        return p.returncode, (p.stdout + p.stderr).strip()
    except subprocess.TimeoutExpired:
        return 124, f"timed out after {timeout}s: {' '.join(cmd)}"
    except Exception as exc:  # tool missing, etc.
        return 127, str(exc)


def block(message):
    print(message, file=sys.stderr)
    return 2


def git_branch():
    rc, out = run(["git", "rev-parse", "--abbrev-ref", "HEAD"])
    return out if rc == 0 else ""


def feature_dir():
    """The feature Spec Kit is currently working on, or None."""
    fj = ROOT / ".specify" / "feature.json"
    if fj.exists():
        try:
            d = json.loads(fj.read_text()).get("feature_directory")
            if d and (ROOT / d).is_dir():
                return ROOT / d
        except Exception:
            pass
    b = git_branch()
    if re.match(r"^\d{3}-", b) and (ROOT / "specs" / b).is_dir():
        return ROOT / "specs" / b
    return None


def rel_path(file_path):
    p = Path(file_path)
    if not p.is_absolute():
        p = ROOT / p
    try:
        return p, p.resolve().relative_to(ROOT)
    except Exception:
        return p, None


def _without_docstrings(source):
    """Parse ``source`` and return its AST dump with every docstring removed."""
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            first = node.body[0] if node.body else None
            if (isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant)
                    and isinstance(first.value.value, str)):
                node.body = node.body[1:] or [ast.Pass()]
    return ast.dump(tree)


def documentation_only_edit(path, tool_input):
    """True when the edit changes only docstrings, comments or layout of an existing file.

    Constitution X allows documenting an applied migration because that cannot change the
    schema. Anything that alters the parsed code (imports, revision ids, operations) is not
    documentation and stays blocked.
    """
    try:
        before = path.read_text(encoding="utf-8")
        if "content" in tool_input:
            after = tool_input["content"]
        else:
            old, new = tool_input["old_string"], tool_input["new_string"]
            if old not in before:
                return False
            replace_all = tool_input.get("replace_all")
            after = before.replace(old, new) if replace_all else before.replace(old, new, 1)
        return _without_docstrings(before) == _without_docstrings(after)
    except Exception:  # unreadable file, bad edit or syntax error: treat as not documentation
        return False


def find_pyproject(start):
    cur = start if start.is_dir() else start.parent
    while True:
        if (cur / "pyproject.toml").exists():
            return cur
        if cur == ROOT or cur == cur.parent:
            return None
        cur = cur.parent


def count_tasks(tasks_md):
    text = tasks_md.read_text(errors="ignore")
    done = len(re.findall(r"^\s*- \[[xX]\]", text, re.M))
    todo = len(re.findall(r"^\s*- \[ \]", text, re.M))
    return done, done + todo


# ------------------------------------------------------------ session start
def status_text():
    lines = ["HDS 2.0 project status"]
    b = git_branch() or "(no git)"
    lines.append(f"- Branch: {b}")
    fd = feature_dir()
    if not fd:
        lines.append("- Feature: none active. Start one with /hds-run <number> specify (see .claude/hds/prompts/).")
    else:
        lines.append(f"- Feature: {fd.relative_to(ROOT)}")
        have = {n: (fd / n).exists() for n in ("spec.md", "plan.md", "tasks.md")}
        lines.append("- Artifacts: " + ", ".join(f"{n} {'yes' if v else 'no'}" for n, v in have.items()))
        if not have["spec.md"]:
            nxt = "/hds-run <n> specify"
        elif not have["plan.md"]:
            nxt = "/speckit-clarify (once), then /hds-run <n> plan"
        elif not have["tasks.md"]:
            nxt = "/speckit-tasks, then /speckit-analyze"
        else:
            done, total = count_tasks(fd / "tasks.md")
            lines.append(f"- Tasks: {done}/{total} done")
            nxt = "/speckit-implement (one phase at a time, commit after each)" if done < total else \
                  "all tasks done: run /hds-review, commit, open a PR, then start the next feature"
        lines.append(f"- Next step: {nxt}")
    lines += [
        "Rules in force (constitution): original spec wins; layered backend; scheduling package is pure;",
        "tests first; API-first generated client; Argon2id + JWT; Decimal money, UTC times; i18n strings;",
        "CI gates; Alembic migrations only. Hooks block violations; read the message and fix, do not bypass.",
    ]
    return "\n".join(lines)


def session_start(_):
    print(status_text())
    return 0


# ------------------------------------------------------------- pre-edit
def pre_edit(d):
    fp = (d.get("tool_input") or {}).get("file_path") or ""
    if not fp:
        return 0
    p, rel = rel_path(fp)
    name = p.name
    parts = rel.parts if rel else p.parts

    if name.startswith(".env") and name not in (".env.example", ".env.sample"):
        return block("Blocked: never write real secrets into .env files. Put placeholders in .env.example "
                     "and read values from the environment or Vault (constitution principle 6).")
    if p.suffix in (".pem", ".key", ".p12") or "secrets" in parts:
        return block("Blocked: key and secret files must not be created or edited by Claude (principle 6).")

    if "alembic" in parts and "versions" in parts and p.exists():
        rc, _ = run(["git", "ls-files", "--error-unmatch", str(p)])
        if rc == 0 and not documentation_only_edit(p, d.get("tool_input") or {}):
            return block(f"Blocked: {name} is a committed migration. Never edit the code of applied "
                         "migrations; create a new one with `alembic revision`. Only docstrings and "
                         "comments may change (principle 10).")

    if rel and "generated" in parts and parts[0] in ("packages", "apps"):
        return block("Blocked: generated API client code is never hand-edited. Change the OpenAPI contract "
                     "and regenerate the client (principle 5).")

    if rel and parts[0] in IMPL_ROOTS and not os.environ.get("HDS_SKIP_GATE"):
        fd = feature_dir()
        if not fd or not (fd / "tasks.md").exists():
            return block("Blocked: no implementation before the spec flow is done. There is no tasks.md for the "
                         "active feature. Run /hds-run <n> specify, then /speckit-clarify, /hds-run <n> plan, "
                         "/speckit-tasks and /speckit-analyze first. (Override on purpose: HDS_SKIP_GATE=1.)")
    return 0


# ------------------------------------------------------------- pre-bash
DANGEROUS = [
    (r"\bgit\s+push\b.*(--force\b|--force-with-lease\b|\s-f\b)", "force pushes are not allowed"),
    (r"\bgit\s+reset\s+--hard\b", "git reset --hard can destroy work; use git restore or ask the user"),
    (r"\bgit\s+clean\s+-\w*f", "git clean -f deletes untracked files; ask the user"),
    (r"\brm\s+-\w*r\w*f?\s+(/|~|\$HOME|\*|\.\s*$|\.\./)", "recursive delete of a broad path"),
    (r"\bsudo\b", "sudo is not allowed"),
    (r"(curl|wget)\b[^|;]*\|\s*(sudo\s+)?(ba|z)?sh\b", "piping downloads into a shell"),
    # The (?!...) keeps .env.example and .env.sample readable: they hold placeholders and are the
    # documented way to see the settings. Anything else starting with .env stays blocked.
    (r"\b(cat|less|more|head|tail|grep|source|cp|scp)\b[^|;&]*\s\.env"
     r"(?!\.(?:example|sample)(?:\s|$|[\"';|&)]))(\.local|\.production|\.prod)?\b",
     "reading real .env files; use .env.example (principle 6)"),
    (r"\b(psql|sqlite3)\b.*\b(drop|truncate|delete\s+from|alter)\b",
     "manual schema or data changes; use an Alembic migration (principle 10)"),
]


_MESSAGE_FLAG = re.compile(r"(?<![\w-])(?:-m|--message)(?:\s*=\s*|\s+|(?=[\"']))")
_HEREDOC_START = re.compile(r"<<-?\s*['\"]?(\w+)['\"]?")
# $(cat <<'WORD' ... WORD) with a QUOTED delimiter: the body is literal text, nothing runs.
_LITERAL_HEREDOC = re.compile(r"\$\(\s*cat\s+<<-?\s*'(\w+)'.*?\n\s*\1[ \t]*\n?\s*\)", re.S)


def _quoted_end(cmd, i):
    """Index just after the quoted string that starts at ``cmd[i]``, or -1 if it never closes.

    Understands single quotes, double quotes with backslash escapes, and a ``$( ... )``
    substitution inside double quotes, including a heredoc body inside it (where quotes and
    parentheses are plain text).
    """
    if cmd[i] == "'":
        end = cmd.find("'", i + 1)
        return -1 if end < 0 else end + 1
    depth, j = 0, i + 1
    while j < len(cmd):
        char = cmd[j]
        if char == "\\":
            j += 2
        elif cmd.startswith("$(", j):
            depth, j = depth + 1, j + 2
        elif depth and (heredoc := _HEREDOC_START.match(cmd, j)):
            closing = re.compile(r"^" + re.escape(heredoc.group(1)) + r"[ \t]*$", re.M)
            found = closing.search(cmd, heredoc.end())
            if not found:
                return -1
            j = found.end()
        elif depth:
            depth, j = depth - (char == ")"), j + 1
        elif char == '"':
            return j + 1
        else:
            j += 1
    return -1


def strip_message_arguments(cmd):
    """Blank out the text of quoted ``-m`` / ``--message`` arguments before the patterns run.

    A commit message is prose: words such as "cat .env" in it are not a command. The text is
    kept (and therefore still scanned) when the argument contains a live command substitution,
    a backtick or a ``$( ... )`` that is not a literal quoted-delimiter heredoc, because the shell
    would run that.
    """
    out, pos = [], 0
    for flag in _MESSAGE_FLAG.finditer(cmd):
        start = flag.end()
        if flag.start() < pos or start >= len(cmd) or cmd[start] not in "\"'":
            continue
        end = _quoted_end(cmd, start)
        if end < 0:
            continue
        body = cmd[start + 1 : end - 1]
        if cmd[start] == '"':
            body = _LITERAL_HEREDOC.sub("", body)
            if "`" in body or "$(" in body:
                continue
        out.append(cmd[pos:start])
        out.append('""')
        pos = end
    out.append(cmd[pos:])
    return "".join(out)


def pre_bash(d):
    cmd = (d.get("tool_input") or {}).get("command") or ""
    scanned = strip_message_arguments(cmd)
    for pattern, why in DANGEROUS:
        if re.search(pattern, scanned, re.I):
            return block(f"Blocked command: {why}.")

    if re.search(r"\bgit\s+commit\b", cmd):
        if git_branch() in ("main", "master") and not os.environ.get("HDS_ALLOW_MAIN"):
            return block("Blocked: do not commit on main. Create the feature branch first, for example "
                         "`git switch -c 001-foundation-auth-org`. (Override on purpose: HDS_ALLOW_MAIN=1.)")
        if "$(" not in cmd and "<<" not in cmd:
            m = re.search(r"""-m\s*(["'])(.*?)\1""", cmd, re.S)
            if m:
                subject = m.group(2).strip().splitlines()[0] if m.group(2).strip() else ""
                if not CONVENTIONAL.match(subject):
                    return block("Blocked: commit message must follow Conventional Commits, e.g. "
                                 "`feat(auth): add refresh token rotation`. Types: feat fix docs chore "
                                 "refactor test ci build perf style revert (principle 9).")
    return 0


# ------------------------------------------------------------- post-edit
INFRA_IMPORT = r"(fastapi|starlette|sqlalchemy|alembic|httpx|requests|redis|celery|asyncpg|psycopg2?)"
LAYER_RULES = {
    "scheduling": [
        (rf"^\s*(?:from|import)\s+{INFRA_IMPORT}\b",
         "the scheduling package must be pure: no web, database or network imports (principle 3)"),
        (r"^\s*from\s+(?!\.)[\w.]*\b(routers|services|repositories|models|schemas|api|db)\b",
         "the scheduling package must not import other app packages (principle 3)"),
        (r"^\s*from\s+\.\.", "the scheduling package must not import from outside itself (principle 3)"),
    ],
    "routers": [
        (r"^\s*(?:from|import)\s+sqlalchemy\b", "routers must not touch the database; call a service (principle 2)"),
        (r"^\s*from\s+(?!\.)[\w.]*\brepositories\b", "routers must call services, never repositories (principle 2)"),
    ],
    "services": [
        (r"^\s*(?:from|import)\s+(fastapi|starlette)\b", "services must not know about HTTP (principle 2)"),
    ],
    "repositories": [
        (r"^\s*(?:from|import)\s+(fastapi|starlette)\b", "repositories must not know about HTTP (principle 2)"),
    ],
}
MONEY_FLOAT = re.compile(
    r"^\s*\w*(amount|rate|price|pay|billing|cost|cash_?flow|total_?due)\w*\s*(:\s*(Optional\[)?float\b|=\s*float\()"
    r"|(amount|rate|price|pay|billing|cost|cash_?flow)\w*\s*[:=].*\bFloat\b",
    re.I | re.M,
)
NAIVE_TIME = re.compile(r"datetime\.utcnow\(|datetime\.now\(\s*\)|datetime\.today\(")
HARDCODED_SECRET = re.compile(r"""(secret|password|api_?key|token)\w*\s*=\s*["'][^"'\s]{8,}["']""", re.I)


def check_python(p, rel):
    issues = []
    parts = rel.parts if rel else p.parts
    is_test = "tests" in parts or p.name.startswith("test_") or p.name == "conftest.py"
    text = p.read_text(errors="ignore")
    lines = text.splitlines()

    for layer, rules in LAYER_RULES.items():
        if layer in parts:
            for pattern, why in rules:
                for i, line in enumerate(lines, 1):
                    if re.search(pattern, line):
                        issues.append(f"{p.name}:{i}: {why}")
    if not is_test:
        for m in MONEY_FLOAT.finditer(text):
            line_no = text.count("\n", 0, m.start()) + 1
            issues.append(f"{p.name}:{line_no}: money must be Decimal / NUMERIC, never float (principle 7)")
        for m in NAIVE_TIME.finditer(text):
            line_no = text.count("\n", 0, m.start()) + 1
            issues.append(f"{p.name}:{line_no}: use timezone-aware UTC (datetime.now(UTC)) (principle 7)")
        for m in HARDCODED_SECRET.finditer(text):
            line_no = text.count("\n", 0, m.start()) + 1
            issues.append(f"{p.name}:{line_no}: looks like a hard-coded secret; read it from the environment (principle 6)")
    return issues


def ruff_cmd(p):
    root = find_pyproject(p)
    if shutil.which("ruff"):
        return ["ruff"], root or ROOT
    if shutil.which("uv") and root:
        return ["uv", "run", "--quiet", "ruff"], root
    return None, None


def post_edit(d):
    fp = (d.get("tool_input") or {}).get("file_path") or ""
    if not fp:
        return 0
    p, rel = rel_path(fp)
    if not p.exists():
        return 0
    issues = []

    if p.suffix == ".py":
        # Architecture rules first: ruff --fix would delete the offending (unused) imports and hide them.
        issues += check_python(p, rel)
        base, cwd = ruff_cmd(p)
        if base:
            run(base + ["format", str(p)], cwd=cwd)
            rc, out = run(base + ["check", "--fix", "--extend-ignore", "F401", str(p)], cwd=cwd)
            if rc != 0 and out:
                issues.append("ruff:\n" + out[-1500:])
    elif p.suffix in (".ts", ".tsx", ".js", ".jsx", ".json") and shutil.which("pnpm") \
            and (ROOT / "node_modules").exists() and "node_modules" not in p.parts:
        run(["pnpm", "exec", "prettier", "--write", str(p)], timeout=60)

    if issues:
        return block("Fix these before continuing:\n" + "\n".join(issues))
    return 0


# ------------------------------------------------------------------ stop
def tail(text, n=40):
    return "\n".join(text.splitlines()[-n:])


def stop(d):
    if d.get("stop_hook_active") or os.environ.get("HDS_SKIP_STOP_GATE"):
        return 0
    rc, out = run(["git", "status", "--porcelain"])
    changed = [l[3:] for l in out.splitlines() if re.search(r"\.(py|ts|tsx)$", l)]
    if rc != 0 or not changed:
        return 0

    problems = []
    py_root = ROOT if (ROOT / "pyproject.toml").exists() else (ROOT / "backend" if (ROOT / "backend" / "pyproject.toml").exists() else None)
    if py_root and shutil.which("uv") and any(c.endswith(".py") for c in changed):
        pyproject = (py_root / "pyproject.toml").read_text(errors="ignore")
        steps = [("ruff", ["uv", "run", "--quiet", "ruff", "check", "."])]
        if "mypy" in pyproject:
            steps.append(("mypy", ["uv", "run", "--quiet", "mypy", "."]))
        if (py_root / "tests").exists():
            steps.append(("pytest", ["uv", "run", "--quiet", "pytest", "-q", "-x", "--maxfail=1"]))
        for label, cmd in steps:
            rc, out = run(cmd, timeout=170, cwd=py_root)
            if rc not in (0, 5):  # 5 = pytest found no tests
                problems.append(f"{label} failed:\n{tail(out)}")
                break

    if problems:
        return block("Quality gate (constitution principle 9) failed. Fix it before finishing:\n" + "\n\n".join(problems))
    return 0


EVENTS = {
    "session-start": session_start,
    "status": session_start,
    "pre-edit": pre_edit,
    "pre-bash": pre_bash,
    "post-edit": post_edit,
    "stop": stop,
}

if __name__ == "__main__":
    event = sys.argv[1] if len(sys.argv) > 1 else ""
    handler = EVENTS.get(event)
    if not handler:
        print(f"unknown event: {event}", file=sys.stderr)
        sys.exit(1)
    sys.exit(handler(read_input() if event not in ("session-start", "status") else {}))
