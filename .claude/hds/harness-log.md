# Harness log

The harness-improvements PR (`chore/claude-harness`), after 006 PR 1 and before PR 2. A reviewer
session reviews; this session implements; nothing is pushed without the owner. One commit per item.

## Item 4: the test-database guard

- **Which databases**: `tests/run_database.allowed_database` refuses, by name, any database a run may
  not use. Outside CI: only `hds_p3_test`, or a run's own (`hds_run_<pid>_test`, and the migration
  tests' `hds_run_<pid>_<name>_test`). Those were already the design (each run gets its own unless
  `TEST_DATABASE_URL` names one), so they stay allowed. Never `hds_test` and never the development
  database `hds`. In CI (`CI` set), also `hds_test`, which the integration job sets. `hds_check`
  and `hds_scratch` are CI's too, but no pytest run uses them (the migration check and the contract
  job run Alembic and the server directly), and the existing `*_test` rule refuses them anyway.
- **One run at a time**: `tests/run_lock.RunLock` takes `pg_try_advisory_lock` on a key derived from
  the database name, on a connection of its own, before the session touches the database, and holds
  it to the end. A second run on the same database stops at once: "another test run is using the
  database 'hds_p3_test'". The connection lives in an event loop on a thread of its own, since the
  session fixture is synchronous and each `asyncio.run` ends its loop.
- **A template clone per run**: not worth it now. Creating and migrating a run's database takes a
  few seconds; the suite takes about 25 minutes. Worth revisiting if migrations grow slow.
- **Tests**: 11 new unit tests (`test_run_database.py`: allowed and refused names, in and out of CI,
  the lock key) failed first on the missing functions, then passed. Two integration tests
  (`test_run_lock.py`) run with the database suite at the end of this PR.

## Item 2: the git hooks

- `make hooks` installs the pre-commit and commit-msg hooks with pre-commit 4.7.0 through `uvx`.
- The config was stale and partly broken, so installing it as it stood would have failed or fought
  CI: ruff 0.4.4 and mypy 1.10 (src only, without the project's packages) against the project's
  ruff 0.17.3 and mypy 2.4. And the commitlint entry pointed at commitlint's own repository, which
  publishes no pre-commit hook, so it could never install. Now ruff, ruff format and mypy are local
  hooks running the project's versions (`uv run --extra dev`, from the lockfile), and commitlint
  comes from `alessandrojcm/commitlint-pre-commit-hook` v9.27.0 with config-conventional 21.2.4, on
  a Node pre-commit downloads (this Mac has none). gitleaks stays as it was.
- **Proof**, in a separate clone (its own `.git/hooks`; a worktree would share this repository's):
  `make hooks` installed both hooks; "Bad Subject." was refused (subject-empty, type-empty,
  subject-full-stop); a Python file with an unused import was refused by ruff (F401) and the format
  check; clean commits passed every hook. The first try showed `uv run ruff` cannot start ruff in a
  fresh clone, because ruff is in the `dev` extra; hence `--extra dev`. This repository's own
  `.git/hooks` were never touched.

## Item 3: make ci-local and make ci-local-db

- `scripts/ops/ci_local.sh` mirrors CI's jobs that need no database, stopping at the first failure:
  ruff and format, interrogate, `mypy --strict`, the import graph, pip-audit, the unit and property
  tests with CI's 1000 examples, the API image check, ShellCheck, the Markdown links (lychee, offline,
  over tracked Markdown only, since node_modules holds its own), gitleaks over `origin/main..HEAD`
  (redacted), commitlint over the same commits, and the client (ESLint, tsc, Jest, the generated
  client's drift) in the pinned Node image, with the OpenAPI document exported here and a stand-in
  `uv` in the container. Images pinned as VERSIONS.md (lychee 0.24.2 added).
- **commitlint**: a first version went through pre-commit's commitlint hook, and passed "Bad
  Subject.": that hook's `commitlint --edit` reads the last commit's message file, not the one
  given, and pre-commit stashes unstaged changes while it runs. It now lints each message from
  standard input in the Node image with the repository's rules (good passes, "Bad Subject." fails
  with subject-empty, type-empty, subject-full-stop). The git hook itself is right: at commit time
  that file is the new message.
- `scripts/ops/ci_local_db.sh`, opt in: the migration check on a throwaway PostgreSQL container (a
  long-lived database can hold leftovers no migration defines, as `hds_p3_test` did), then the
  integration, contract and concurrency suites on `hds_p3_test`.
- **Run**: `make ci-local` passed on the branch (1079 unit and property tests, 76 client tests,
  the generated client up to date, both commits linted).

## Item 8: CI tidying

- The quickstart job's description is above the script step again, under the `docker version` step.
- `actions/checkout@v4` (Node 20) is now `@v7` (Node 24, a major tag, as the file pins) in all 15
  places, and `astral-sh/setup-uv@v4` is now `@v10.3.0` (Node 24, 2026-10-09) in all 11: that
  action publishes no `v10` major tag, so the exact version is pinned. Its `version: 0.13.5` input is
  unchanged.
- Left for a follow-up, outside the review's list: `actions/cache`, `actions/setup-node`,
  `actions/upload-artifact` and `pnpm/action-setup` are still `@v4` on Node 20.

## Review fixes (items 2 to 4 and 8)

- **F1**: `test_run_lock.py` hard-coded the developer's server URL, so it would fail on CI's
  credentials. It now takes the session's URL and changes only the database name.
- **F2, and it touched this machine**: `ci_local.sh`'s client step mounted the checkout read-write
  and ran `pnpm install` in the Linux image, which replaced the host's `node_modules` with Linux
  builds and a store path inside the container (at 13:25). The client checks now run on a copy of
  the tracked files in the run's scratch folder. Proof: after a full `make ci-local`, the host's
  `node_modules/.modules.yaml` checksum and the `.pnpm` listing (1028 entries) are unchanged. The
  host's `node_modules` was not repaired here; the reviewer gave the owner the command.
- **F3**: the lock comment said advisory locks are server-wide; they belong to the database they are
  taken in, which is why every run takes it in `postgres`. The comment now says so.
- **F4**: setup-uv stays at v10.3.0: v10.3.3 is eight days old, and the pins wait two weeks.
- **Also caught by `make ci-local`**: once `ci_local_db.sh` was tracked, the published-values test
  found its throwaway password `check`. The script now makes a fresh random one each run.

## Items 5 and 6: the settings-file guard and the gitleaks guard (one commit: one function)

The owner approved items 1, 5, 6 and 7 directly (they change the hooks and commands under
`.claude/`, which this session does not change on another session's request).

- **Item 5**: the settings-file rule was one regex: a reading word, then `.env` anywhere after it,
  heredoc text included. It is now `reads_settings_file` in `hds_hooks.py`, which splits the
  command into words (`shlex`). Blocked: a reading or copying command (cat, less, head, grep, awk,
  sed, cp, mv, rsync, tar, source, `.`, and others) with a real settings file after it, also behind
  `docker run` or `xargs`; `< .env`; `$( ... )` and backticks wherever they are; `sh -c`, `bash -c`
  and `eval` strings; interpreter code (`python -c`, a heredoc fed to `python` or `sh`) that opens a
  quoted settings path. Not blocked: a name right after `--exclude`, a `>` target, `.env.example`
  and `.env.sample`, a heredoc written to a file (except an unquoted one's substitutions), and plain
  mentions (`git ls-files .env`, `ls`, `echo "cat .env"`, compose's `--env-file`). A command that
  cannot be split falls back to the old regex, so the guard is never looser on a parse failure.
- **Item 6**: `gitleaks_problem` refuses any gitleaks run (the binary as the command, or its image
  anywhere) without `--redact`, and any `dir` or `--no-git` scan that does not name exactly one
  path below the root (`.`, `/repo`, `$PWD` and the like count as the root). The run that printed
  the key named four paths, and gitleaks scanned the whole folder.
- **Tests** (unit, no database, branch-independent, through the real hook): 10 mentions pass, 14
  reads block, 7 bad gitleaks runs block, 4 good ones pass; the existing cases still pass. One
  existing case changed on purpose: `echo "cat .env"` was blocked as a mention and is now allowed;
  `sh -c` and `eval` with the same text were added as blocked. Against the hook as committed at
  HEAD, the new cases fail: 3 of 10 mentions wrongly blocked, 9 of 14 reads let through
  (`less ./.env`, `rsync -a .env backup/`, a Python `open` of the file, `docker run ... cat
  /repo/.env`), and all 7 bad gitleaks runs let through.
- **A consequence while writing the tests and this log**: the new guard refused two commands of
  mine, Python heredocs that both opened files and quoted the settings path as data. It cannot tell
  data from a read; those were written with the file tools instead.

## Item 1: the end-of-turn gate

- The Stop hook looked for `pyproject.toml` at the root or in `backend/`, found neither, and so
  never ran. It now checks `apps/api`. `stop_plan` (a pure function, so it is tested without running
  anything) picks the checks from the working tree's changed paths (`git status --porcelain`,
  renames by their new path): ruff and the format check on the changed apps/api files that still
  exist, `mypy --strict`, the unit tests, and the property tests when `hds.payroll`,
  `hds.scheduling` or the property tests changed. A changed hook runs the unit tests, which hold
  the hook's tests. Every step uses `uv run --extra dev`. Never a database suite.
- `stop_hook_active` lets Claude stop after one failed gate, so a failure cannot loop; the
  existing `HDS_SKIP_STOP_GATE` still switches it off.
- **Tests**: 9 unit tests (`test_harness_stop_gate.py`), which import the hook with
  `CLAUDE_PROJECT_DIR` set, as Claude Code runs it. Eight failed first on the missing functions;
  the loop guard already worked. One test was wrong at first: it looked for `tests/integration` in
  every command, but ruff is rightly handed a changed integration test by path; it now checks the
  pytest commands only.
- **By hand**: on this branch's working tree the gate passed in 22 s; with a throwaway module that
  imported `os` unused it blocked ("ruff failed: F401"), and with `stop_hook_active` it let Claude
  stop. The throwaway module was removed.

## Item 7: the review ledger

- `specs/<feature>/review-ledger.md`, from `.claude/hds/review-ledger-template.md`: one row per
  finding (id, source, finding, status open, fixed or won't-fix, fix commit).
- `/hds-review` now records its findings there, adding a row per new finding and creating the file
  from the template if needed; it still edits nothing else. Its allowed tools gain `Read` and
  `Edit`/`Write` limited to `specs/**/review-ledger.md`.
- `/hds-status` shows "Review ledger: N open of M", and while any row is open its next step says to
  close them first. The working agreement (`.claude/hds/CLAUDE.hds.md`) gains one line: a group is
  done only when no row is open. The owner approved this with item 7.
- **Tests**: 5 unit tests (`test_harness_review_ledger.py`): open rows counted whatever their case,
  no ledger is nothing open, the status line and whether it holds the group, the template's empty
  table. They failed first on the missing functions and the missing template.
- No ledger was created for 006 here: its findings so far are in `specs/006-deploy-operations/
  p1-log.md`. The next `/hds-review` starts it.

## Review fixes (items 1, 5 and 6)

- **G1, the guard let through what the old one blocked**: a shell's script now comes after any flag
  cluster holding `c` (`-lc`, `-ec`), with `-o <option>` skipped; a heredoc is code when any word on
  its line, before or after `<<`, is a shell or interpreter (`cat <<EOF | sh`, `uv run python -`,
  `docker run -i img python -`); `>` and `>>` onto a real settings file block, as `<` does, since
  `pre_edit` already refuses the Write and Edit tools there (`tee` already did). A regression
  corpus, over 200 commands built from the old pattern's readers and files in common forms, must
  stay blocked by the new guard except a short, named list of intended mentions.
- **G2, a search that prints a key**: a recursive grep, or an rg reading ignored or hidden files,
  blocks unless it excludes `.env*` or no folder it searches holds a settings file (a bounded walk
  that skips node_modules, .git and .venv, and gives up as "yes" past 20000 entries). The reviewer
  suggested blocking every recursive grep over a folder; this is narrower, because a subfolder with
  no settings file cannot leak one, and `grep -rn` over `docs/` or `apps/api/src` is an everyday
  search. `git grep` reads tracked files only. `find ... -exec` over a settings file blocks too.
- **G3**: assignments and wrappers (`time`, `env`, `command`, `nice`, `nohup`, `xargs`, `timeout`
  and its duration) are skipped before the command word, for gitleaks and for `.`.
- **G4, the gate read the shared tree**: post-edit records each path a Write or Edit changes under
  `.git/hds-hooks/edits-<session id>`, and the gate checks only those, clearing them when it passes
  and keeping them when it fails. A session with no edits runs nothing, not even `git status`.
  Edits made through Bash are not recorded, so they are not gated; CI and `make ci-local` remain the
  authority. Without a session id the gate reads the tree with `--untracked-files=all`. All steps
  share one 270 s deadline, under the hook's 300 s.
- **Tests**: 20 new guard cases and the corpus failed first, then passed; 6 new gate tests failed
  first (one was wrong at first, a fragile parse of settings.json, now read as JSON). The unit suite:
  1152 passed. Recording checked live: this session's post-edit wrote `.claude/hooks/hds_hooks.py`
  to its file.
- **H1, the gate was off in worktrees**: in a linked worktree `.git` is a file, so `ROOT/.git/
  hds-hooks` could not be made and nothing was recorded; with a session id the gate then checked
  nothing. The state folder is now asked of git (`git rev-parse --absolute-git-dir`), which in a
  worktree is its own, so sessions in different worktrees never share a record; outside git the
  gate falls back to reading the status. A new test makes a worktree in a temporary repository,
  records an edit through the real post-edit hook, and finds it in the worktree's git folder and
  not the main one. It failed first. Each hook call now runs one `git rev-parse` (about 10 ms).
- **The gate in practice**: at the end of the turn that made 73e6337 it blocked once, on the log
  redaction timing test, which runs slow beside the database suite (as twice before in 006); the
  file alone passed, 93 tests.
- **Wall-clock tests out of the gate only**: a new marker, `wall_clock` (registered in pyproject),
  marks the three unit tests with a hard time limit a busy machine can miss: the hostile-text
  redaction (14 cases), the century-long clash check and the hung reset job. The gate runs pytest
  with `-m "not timing and not benchmark and not wall_clock"`: a second `-m` replaces the one in
  addopts, so its exclusions are repeated (a test checks the exact expression). CI and
  `make ci-local` pass no `-m`, so they still run all 16. The existing `timing` marker was not
  reused: it means statistical checks skipped everywhere unless chosen, which would drop these from
  CI too. `test_login_equal_work` counts work rather than time and stays unmarked.
