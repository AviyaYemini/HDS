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
import time
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
LEDGER_NAME = "review-ledger.md"
_LEDGER_STATUSES = ("open", "fixed", "won't-fix")


def ledger_counts(path):
    """How many findings in a review ledger are open, and how many it holds: ``(open, total)``.

    The ledger is a Markdown table (``.claude/hds/review-ledger-template.md``) whose fourth column
    is the status: open, fixed or won't-fix, in any case. It is read as the second-to-last cell, so
    a ``|`` inside a finding's text moves nothing. No file counts as ``(0, 0)``.
    """
    if not path.exists():
        return 0, 0
    open_rows = total = 0
    for line in path.read_text(errors="ignore").splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 5 or cells[-2].lower() not in _LEDGER_STATUSES:
            continue
        total += 1
        open_rows += cells[-2].lower() == "open"
    return open_rows, total


def ledger_line(path):
    """The status line for a review ledger, and whether its open rows hold the group back."""
    open_rows, total = ledger_counts(path)
    rel = path.relative_to(ROOT) if path.is_relative_to(ROOT) else path
    return f"- Review ledger: {open_rows} open of {total} ({rel})", open_rows > 0


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
        ledger, held = ledger_line(fd / LEDGER_NAME)
        lines.append(ledger)
        if held:
            nxt = "close the open review-ledger rows first (a group is done only with none open); " \
                  "then " + nxt
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
    (r"\b(psql|sqlite3)\b.*\b(drop|truncate|delete\s+from|alter)\b",
     "manual schema or data changes; use an Alembic migration (principle 10)"),
]

# ---- reading a real settings file (principle 6) ----
# A command is blocked when it would READ or COPY a real settings file (.env, .env.local, ...), not
# when it merely mentions one: an rsync --exclude, a heredoc written to a file, a grep over docs,
# `git ls-files`. .env.example and .env.sample hold placeholders and stay readable.
SETTINGS_WHY = "reading real .env files; use .env.example (principle 6)"
# The old pattern, kept only as the fallback when a command cannot be split into words (unbalanced
# quotes): then the guard is as strict as before, never looser.
_SETTINGS_FALLBACK = re.compile(
    r"\b(cat|less|more|head|tail|grep|source|cp|scp)\b[^|;&]*\s\.env"
    r"(?!\.(?:example|sample)(?:\s|$|[\"';|&)]))(\.local|\.production|\.prod)?\b", re.I)
# Commands that read a file named as an argument, or copy it somewhere it can be read.
SETTINGS_READERS = frozenset({
    "cat", "tac", "less", "more", "most", "head", "tail", "grep", "egrep", "fgrep", "rg", "ag",
    "awk", "gawk", "sed", "cut", "sort", "uniq", "nl", "strings", "xxd", "od", "hexdump", "base64",
    "bat", "view", "vi", "vim", "nano", "emacs", "diff", "cmp", "comm", "paste", "join", "tee",
    "cp", "mv", "scp", "rsync", "tar", "zip", "gzip", "openssl", "jq", "yq", "source",
})
_INTERPRETERS = frozenset({"python", "python3", "node", "ruby", "perl"})
_SHELLS = frozenset({"sh", "bash", "zsh", "dash"})
# A settings file in code: a quoted path to one, read by open(), read_text() or a dotenv loader.
_CODE_SETTINGS_PATH = re.compile(
    r"""['"](?:[\w.~-]*/)*\.env(?:\.(?!example['"]|sample['"])[\w-]+)*['"]""")
_CODE_READS = re.compile(r"\bopen\s*\(|read_text|read_bytes|dotenv|copyfile|shutil\.copy")
# Not the `<<` of a here-string (`<<<`) or of a shift (`$((1<<3))`): read as a heredoc, the lines
# after it would be dropped as its body. A `cat<<EOF` with no space is missed too, which only means
# its body is scanned as commands, the stricter reading.
_HEREDOC = re.compile(r"(?<![<\w])<<-?(?!<)\s*(['\"]?)(\w+)\1")


def is_settings_file(word):
    """Whether ``word`` names a real settings file, or a glob that would match one."""
    name = word.rstrip("/").rsplit("/", 1)[-1]
    if not name.startswith(".env"):
        return False
    if name in (".env.example", ".env.sample"):
        return False
    rest = name[len(".env"):]
    return rest == "" or rest[0] == "." or any(c in rest for c in "*?[")


def _substitutions(text):
    """The commands inside ``$( ... )`` and backticks in ``text``, outside single quotes.

    The shell runs them even inside double quotes, so they are always scanned.
    """
    found, i, quoted = [], 0, False
    while i < len(text):
        char = text[i]
        if char == "'" and not quoted:
            end = text.find("'", i + 1)
            i = len(text) if end < 0 else end + 1
            continue
        if char == '"':
            quoted = not quoted
        elif text.startswith("$(", i):
            depth, j = 1, i + 2
            while j < len(text) and depth:
                depth += {"(": 1, ")": -1}.get(text[j], 0)
                j += 1
            found.append(text[i + 2 : j - 1])
            i = j
            continue
        elif char == "`":
            end = text.find("`", i + 1)
            if end > 0:
                found.append(text[i + 1 : end])
                i = end + 1
                continue
        i += 1
    return found


def _split_heredocs(cmd):
    """``cmd`` with its heredoc bodies taken out, and the bodies that are code.

    A body fed to a shell or an interpreter is code and is returned with its kind; a body written
    to a file (``cat > f <<EOF``) is data and dropped, except that an unquoted delimiter still runs
    its ``$( ... )`` substitutions, which are kept.
    """
    lines, kept, code = cmd.split("\n"), [], []
    i = 0
    while i < len(lines):
        line = lines[i]
        kept.append(line)
        starts = list(_HEREDOC.finditer(line))
        i += 1
        for start in starts:
            quoted, word = start.group(1), start.group(2)
            body = []
            while i < len(lines) and lines[i].strip() != word:
                body.append(lines[i])
                i += 1
            i += 1  # the delimiter line
            text = "\n".join(body)
            # The body goes to whatever on its line reads standard input: `sh <<EOF`,
            # `cat <<EOF | sh`, `uv run python - <<EOF`, `docker run -i img python - <<EOF`. Any
            # shell or interpreter word on the line, before or after the `<<`, makes it code.
            names = {w.rsplit("/", 1)[-1] for w in re.split(r"[\s;&|()]+", _HEREDOC.sub(" ", line))}
            if names & _SHELLS:
                code.append(("shell", text))
            elif names & _INTERPRETERS:
                code.append(("code", text))
            elif not quoted:
                code.extend(("shell", s) for s in _substitutions(text))
    return "\n".join(kept), code


def _segments(text):
    """``text`` split into simple commands, each a list of words; None if it cannot be split."""
    try:
        lexer = shlex.shlex(text, posix=True, punctuation_chars=";&|()<>")
        lexer.whitespace_split = True
        lexer.commenters = ""
        words = list(lexer)
    except ValueError:
        return None
    segments, current = [], []
    for word in words:
        if word and set(word) <= set(";&|()"):
            segments.append(current)
            current = []
        else:
            current.append(word)
    segments.append(current)
    return [s for s in segments if s]


_WRAPPERS = frozenset({"time", "env", "command", "nice", "nohup", "xargs", "exec", "stdbuf",
                       "timeout", "sudo", "doas"})
_ASSIGNMENT = re.compile(r"[A-Za-z_]\w*=.*")
# Folders a settings file never sits in, and that are too large to walk (node_modules).
_NO_WALK = frozenset({".git", "node_modules", ".venv", "venv", "__pycache__", ".mypy_cache",
                      ".ruff_cache", ".pytest_cache", ".hypothesis", "dist", "coverage"})


def command_word_index(words):
    """The index of the command in a simple command, past assignments and wrappers.

    ``A=1 time env B=2 gitleaks dir apps`` gives the index of ``gitleaks``; ``timeout 5 cmd`` skips
    the duration, and a wrapper's own flags are skipped too.
    """
    i = 0
    while i < len(words):
        word = words[i]
        name = word.rsplit("/", 1)[-1]
        if _ASSIGNMENT.fullmatch(word):
            i += 1
        elif name in _WRAPPERS:
            i += 1
            while i < len(words) and (words[i].startswith("-") or
                                      (name == "timeout" and re.fullmatch(r"[\d.]+[smhd]?", words[i]))):
                i += 1
        else:
            return i
    return len(words)


def _shell_script(words, at):
    """The script a shell at ``words[at]`` runs with ``-c``: after any flag cluster holding ``c``."""
    i = at + 1
    while i < len(words):
        word = words[i]
        if word in ("-o", "+o"):
            i += 2
        elif re.fullmatch(r"-[A-Za-z]*c[A-Za-z]*", word):
            return words[i + 1] if i + 1 < len(words) else None
        elif word.startswith(("-", "+")):
            i += 1
        else:
            return None
    return None


def _tree_has_settings(path, limit=20000):
    """Whether a real settings file sits in the folder ``path`` or below it (the search's target).

    The walk skips folders that never hold one, and gives up as "yes" after ``limit`` entries, so a
    huge tree is refused rather than searched slowly.
    """
    target = Path(path)
    if not target.is_absolute():
        target = ROOT / target
    if not target.is_dir():
        return is_settings_file(str(path))
    seen = 0
    for _dirpath, dirnames, filenames in os.walk(target):
        dirnames[:] = [d for d in dirnames if d not in _NO_WALK]
        seen += len(filenames) + len(dirnames)
        if seen > limit or any(is_settings_file(f) for f in filenames):
            return True
    return False


def _search_reads_settings(words, at):
    """Whether a grep or rg at ``words[at]`` would search a real settings file's text.

    A recursive grep (``-r``, ``-R``) or an rg that reads ignored or hidden files (``-u``,
    ``--hidden``, ``--no-ignore``) reaches every settings file below its folders, so searching for a
    setting's name would print its real value. It passes when it excludes ``.env*``, or when no
    folder it searches holds a settings file; ``git grep`` reads tracked files only.
    """
    name = words[at].rsplit("/", 1)[-1]
    args = words[at + 1 :]
    flags = [a for a in args if a.startswith("-")]
    if name in ("grep", "egrep", "fgrep"):
        deep = any(re.fullmatch(r"-[A-Za-z]*[rR][A-Za-z]*", f) or
                   f in ("--recursive", "--dereference-recursive") for f in flags)
    else:  # rg
        deep = any(re.fullmatch(r"-u+", f) or f.startswith(("--hidden", "--no-ignore")) or
                   f == "-." for f in flags)
    if not deep:
        return False
    joined = " ".join(args)
    if re.search(r"--exclude(?:=|\s+)['\"]?\.env", joined) or \
            re.search(r"(?:-g|--glob)(?:=|\s+)['\"]?!\.env", joined):
        return False
    # The pattern is the first plain word unless -e or -f gave it; the rest are the targets.
    plain, skip, given = [], False, any(f in ("-e", "-f", "--regexp", "--file") for f in flags)
    for arg in args:
        if skip:
            skip = False
        elif arg in ("-e", "-f", "--regexp", "--file", "-m", "-A", "-B", "-C", "-g", "--glob",
                     "-t", "--type", "--include", "--exclude", "--exclude-dir"):
            skip = True
        elif not arg.startswith("-"):
            plain.append(arg)
    targets = plain if given else plain[1:]
    return any(_tree_has_settings(t) for t in (targets or ["."]))


def _segment_reads_settings(words):
    """Whether one simple command reads, copies or overwrites a real settings file."""
    for i, word in enumerate(words):
        # `< .env` reads it; `> .env` and `>> .env` overwrite the real key, which pre_edit already
        # refuses for the Write and Edit tools.
        if word in ("<", ">", ">>") and i + 1 < len(words) and is_settings_file(words[i + 1]):
            return True
    first = command_word_index(words)
    previous = ""
    reader_seen = False
    for i, word in enumerate(words):
        name = word.rsplit("/", 1)[-1]
        if name in SETTINGS_READERS or (name == "." and i == first):
            reader_seen = True
            if name in ("grep", "egrep", "fgrep", "rg") and _search_reads_settings(words, i):
                return True
        elif name in _SHELLS:
            script = _shell_script(words, i)
            if script is not None and reads_settings_file(script):
                return True
        elif name in _INTERPRETERS:
            for flag, code in zip(words[i + 1 :], words[i + 2 :]):
                if flag in ("-c", "-e") and _code_reads_settings(code):
                    return True
        elif name == "find" and any(w in ("-exec", "-execdir", "-ok") for w in words[i + 1 :]) and \
                any(is_settings_file(w.strip("'\"")) for w in words[i + 1 :]):
            return True
        if name == "eval" and reads_settings_file(" ".join(words[i + 1 :])):
            return True
        excluded = previous in ("--exclude", "--exclude-from", "--ignore") or word.startswith(
            ("--exclude=", "--exclude-from=", "--ignore=", "--exclude-dir="))
        if reader_seen and not excluded and previous not in ("-name", "-iname") and \
                is_settings_file(word):
            return True
        previous = word
    return False


def _code_reads_settings(code):
    """Whether interpreter code opens or loads a real settings file."""
    return bool(_CODE_SETTINGS_PATH.search(code) and _CODE_READS.search(code))


def reads_settings_file(cmd):
    """Whether running ``cmd`` would read or copy a real settings file (principle 6).

    Mentions are fine; reads are not. Commit messages are stripped first by the caller.
    """
    plain, code = _split_heredocs(cmd)
    for kind, body in code:
        if kind == "code" and _code_reads_settings(body):
            return True
        if kind == "shell" and reads_settings_file(body):
            return True
    if any(reads_settings_file(inner) for inner in _substitutions(plain)):
        return True
    segments = _segments(plain)
    if segments is None:  # cannot be split: as strict as the old pattern
        return bool(_SETTINGS_FALLBACK.search(plain))
    return any(_segment_reads_settings(words) for words in segments)


# ---- gitleaks (the run that printed a signing key) ----
_ROOT_PATHS = frozenset({".", "./", "/", "$PWD", "${PWD}", "/repo", "/mnt", "/src", "/work"})


def gitleaks_problem(cmd):
    """Why a gitleaks run in ``cmd`` is refused, or None.

    Every run must redact all of what it finds: ``--redact`` takes a percentage (100 by default) and
    the last one given wins, so that one must be a bare ``--redact`` or ``--redact=100``. No
    secret, nor part of one, reaches the screen or a log.
    A ``dir`` (or ``detect --no-git``) scan reads files that git ignores, such as real settings
    files, so it must name exactly one path, and not the repository's root; scan commits with
    ``gitleaks git`` instead.
    """
    if "gitleaks" not in cmd:
        return None
    segments = _segments(cmd)
    if segments is None:
        return "a gitleaks command that cannot be read is refused"
    for words in segments:
        # The gitleaks run: the binary as the command, or an image such as
        # ghcr.io/gitleaks/gitleaks:v8@sha256:... anywhere (docker run). A bare "gitleaks" later in
        # a command is an argument (grep gitleaks file), not a run.
        command = command_word_index(words)  # past A=1, time, env, nohup, ...
        at = next((i for i, w in enumerate(words)
                   if (i == command and re.search(r"(^|/)gitleaks$", w))
                   or re.search(r"/gitleaks(?:[:@].*)?$", w)), None)
        if at is None:
            continue
        args = words[at + 1 :]
        redact = [w for w in args if w == "--redact" or w.startswith("--redact=")]
        if not redact or redact[-1] not in ("--redact", "--redact=100"):
            return ("gitleaks must run with a bare --redact (or --redact=100), so no secret it "
                    "finds is printed, not even in part")
        sub = next((w for w in args if not w.startswith("-")), "")
        if sub == "dir" or "--no-git" in args:
            after = args[args.index(sub) + 1 :] if sub == "dir" else args
            # Flags that take a value: their value is not a path.
            valued = {"--config", "-c", "--report-path", "-r", "--report-format", "-f",
                      "--baseline-path", "-b", "--log-level", "-l", "--max-target-megabytes",
                      "--gitleaks-ignore-path", "-i"}
            paths, words_left = [], iter(after)
            for w in words_left:
                if w in ("--source", "-s"):  # the path, given as a flag's value
                    paths.append(next(words_left, ""))
                elif w in valued:
                    next(words_left, None)
                elif not w.startswith("-") and w != "detect":
                    paths.append(w)
            if len(paths) != 1 or paths[0].rstrip("/") in {p.rstrip("/") for p in _ROOT_PATHS}:
                return ("a gitleaks dir scan must name exactly one path below the repository root; "
                        "it reads files git ignores, such as real settings files. Scan commits "
                        "with `gitleaks git --redact` instead")
    return None


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
    if reads_settings_file(scanned):
        return block(f"Blocked command: {SETTINGS_WHY}.")
    leak = gitleaks_problem(scanned)
    if leak:
        return block(f"Blocked command: {leak}.")

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
    if rel is not None:
        record_edit(d.get("session_id"), rel.as_posix())  # for this session's end-of-turn gate
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


API = ROOT / "apps" / "api"
# The pure packages whose property tests are worth their seconds when they change.
_PROPERTY_SOURCES = ("apps/api/src/hds/payroll/", "apps/api/src/hds/scheduling/",
                     "apps/api/tests/property/")


def changed_paths(porcelain):
    """The paths in ``git status --porcelain`` output; a rename gives its new path."""
    paths = []
    for line in porcelain.splitlines():
        path = line[3:]
        if " -> " in path:
            path = path.split(" -> ", 1)[1]
        paths.append(path.strip('"'))
    return paths


def stop_plan(changed):
    """The checks the end-of-turn gate runs for ``changed`` (repository-relative paths).

    Quick ones only: ruff and the format check on the changed apps/api files that still exist,
    mypy --strict, the unit tests, and the property tests when the pure packages or their tests
    changed. Never the integration, contract or concurrency suites: they need a database and take
    about 25 minutes (`make ci-local-db` runs them). A changed hook runs the unit tests, which hold
    the hook's own tests. Every step runs with the dev extra, where ruff, mypy and pytest live.

    Returns:
        ``(label, command)`` pairs, run in apps/api in this order; empty when no Python changed.
    """
    python = [p for p in changed if p.endswith(".py")]
    api = [p for p in python if p.startswith("apps/api/")]
    hooks = [p for p in python if p.startswith(".claude/hooks/")]
    if not (api or hooks) or not (API / "pyproject.toml").exists():
        return []
    uv = ["uv", "run", "--extra", "dev", "--quiet"]
    plan = []
    present = [p[len("apps/api/"):] for p in api if (ROOT / p).exists()]
    if present:
        plan.append(("ruff", uv + ["ruff", "check", *present]))
        plan.append(("ruff format", uv + ["ruff", "format", "--check", *present]))
    if api:
        plan.append(("mypy", uv + ["mypy", "--strict", "src/", "tests/"]))
    # A hard time limit can be missed on a busy machine (beside the database suite, three times), so
    # the gate leaves out the `wall_clock` tests; CI and `make ci-local` still run them. A second
    # -m replaces the one in pyproject's addopts, so its exclusions are repeated here.
    pytest = ["pytest", "-q", "-x", "-p", "no:cacheprovider",
              "-m", "not timing and not benchmark and not wall_clock"]
    plan.append(("unit tests", uv + pytest + ["tests/unit/"]))
    if any(p.startswith(_PROPERTY_SOURCES) for p in api):
        plan.append(("property tests", uv + pytest + ["tests/property/"]))
    return plan


def _git_dir():
    """This checkout's own git folder, or None outside git.

    In a linked worktree `.git` is a file pointing elsewhere, so the folder is asked of git; each
    worktree has its own, so sessions in different worktrees never share a record.
    """
    rc, out = run(["git", "rev-parse", "--absolute-git-dir"])
    return Path(out) if rc == 0 and out else None


# Each session's edited paths, so the gate checks this session's work and not another's in the
# same checkout. In the git folder: never committed, and gone with the clone or the worktree.
_GIT_DIR = _git_dir()
STATE_DIR = _GIT_DIR / "hds-hooks" if _GIT_DIR else None
# The whole gate's time, under the Stop hook's 300 s in settings.json: past that, Claude Code kills
# the hook, which counts as a non-blocking error and lets the turn end unchecked.
STOP_BUDGET_SECONDS = 270


def _edits_file(session_id):
    """The file holding a session's edited paths (the id made safe as a file name)."""
    return STATE_DIR / ("edits-" + re.sub(r"[^\w.-]", "_", session_id))


def record_edit(session_id, path):
    """Note that this session's Write or Edit changed ``path`` (repository-relative)."""
    if not session_id or not path or STATE_DIR is None:
        return
    STATE_DIR.mkdir(exist_ok=True)
    known = set(session_edits(session_id))
    if path not in known:
        with _edits_file(session_id).open("a") as f:
            f.write(path + "\n")


def session_edits(session_id):
    """The paths this session's Write and Edit tools changed since its gate last passed."""
    if STATE_DIR is None:
        return []
    file = _edits_file(session_id)
    if not file.exists():
        return []
    return list(dict.fromkeys(l for l in file.read_text().splitlines() if l))


def clear_edits(session_id):
    """Forget a session's edits, once its gate has passed."""
    if STATE_DIR is not None:
        _edits_file(session_id).unlink(missing_ok=True)


def stop(d):
    """The end-of-turn gate: the quick checks for what changed (principle 9), never a database.

    It checks the paths this session's Write and Edit tools changed (recorded by post-edit, keyed by
    the session id), so another session's work in the same checkout never blocks this one; edits
    made through Bash are not recorded. Without a session id it reads the whole tree. All steps share
    one deadline under the hook's timeout. With ``stop_hook_active`` (the gate already sent Claude
    back once) it lets Claude stop, so a failing gate cannot loop; the edits stay recorded, so the
    next turn checks them again.
    """
    if d.get("stop_hook_active") or os.environ.get("HDS_SKIP_STOP_GATE"):
        return 0
    session_id = d.get("session_id")
    if session_id and STATE_DIR is not None:
        changed = session_edits(session_id)
    else:
        rc, out = run(["git", "status", "--porcelain", "--untracked-files=all"])
        if rc != 0:
            return 0
        changed = changed_paths(out)
    plan = stop_plan(changed)
    if not plan or not shutil.which("uv"):
        return 0
    deadline = time.monotonic() + STOP_BUDGET_SECONDS
    for label, cmd in plan:
        left = deadline - time.monotonic()
        if left < 1:
            return block("Quality gate (constitution principle 9) ran out of time before "
                         f"{label}; run `make ci-local` to finish the checks.")
        rc, out = run(cmd, timeout=left, cwd=API)
        if rc not in (0, 5):  # 5 = pytest found no tests
            return block("Quality gate (constitution principle 9) failed. Fix it before finishing:\n"
                         f"{label} failed:\n{tail(out)}")
    if session_id:
        clear_edits(session_id)
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
