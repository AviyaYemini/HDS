# ADR 021: One spelling for organisation, renamed in one step

**Date**: 2026-11-09
**Status**: Accepted (owner, 2026-11-09). An exemption from Constitution V and X, for this change only.

## Context

The tables, the models and the documentation say "organisation"; every foreign-key column, its
attribute and the API's fields said `organization_id`. The product spec used both spellings, and
the first commit of feature 001 took the column name from its data-model table. Feature 001's
analysis caught the mix (finding I6) and kept the columns as they were, to avoid a rename migration
(specs/001-foundation-auth-org/tasks.md, notes). Every feature since followed that rule, so the
mix grew to 22 columns and 8 API schemas.

Two rules stand against renaming it in one step:

- Constitution X: a destructive change ships in two releases, and `infra/tests/compat.sh` checks
  that the previous release's code still works on the new schema. A renamed column breaks it.
- Constitution V: a breaking change to the API ships under a new `/api/v2` prefix. A renamed field
  is breaking.

## Decision

Rename it in one step, while nothing is deployed:

- Migration `037_organisation_spelling` renames `organization_id` to `organisation_id` in all 22
  tables, with their foreign keys, and rewrites the one trigger function that names the column
  (`hds_default_shift_types`). Its downgrade restores both.
- The API's fields become `organisation_id` under `/api/v1`. The only consumer is the HDS client,
  generated from the same contract and changed in the same pull request.
- `infra/tests/compat.sh` reads `infra/tests/compat-exemptions.txt`: a check whose base predates
  a listed revision passes with a note instead of running the old code, since that code cannot run
  on the renamed schema and no release of it exists. Once the rename is on `main`, every later base
  holds it and the check runs as before.

## Consequences

- One spelling everywhere in the product's code, schema and contract. Only third-party names and
  the applied migrations, whose code is never edited, keep the American spelling.
- Idempotent replays cached before the migration answer with the old field name until they expire;
  nothing is deployed, so only test data holds them.
- The exemption is for this revision alone. After the first deployment, a rename follows
  Constitution X and V in full: two releases, and a new API version for a breaking field.
