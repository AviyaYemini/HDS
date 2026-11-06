# HDS 2.0 working agreement

- Source of truth for scope: the original product spec, then `docs/hds-spec.md` and `docs/hds-api.md`. Anything not in the original is marked Proposed.
- Follow the Spec Kit flow per feature: specify (what and why, no technology), clarify, plan (stack), tasks, analyze, implement. Stored prompts live in `.claude/hds/prompts/`; run them with `/hds-run <n> <stage>`. Check progress with `/hds-status`, review with `/hds-review`.
- The constitution in `.specify/memory/constitution.md` is binding. Hooks in `.claude/hooks/hds_hooks.py` enforce the mechanical parts (layering, pure scheduling package, Decimal money, UTC time, no secrets, no edits to applied migrations or generated clients, Conventional Commits, no implementation before `tasks.md`). If a hook blocks you, fix the cause; never work around it.
- Implement one task phase at a time, run the tests, then commit on the feature branch.
- Record every review finding as a row in the feature's `review-ledger.md` (`/hds-review` adds its own). A group is done only when no row is open; `/hds-status` shows the count.
- Use exactly the prompt text a stage file contains; do not add requirements, principles or technology of your own.
