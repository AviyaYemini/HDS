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
  (`hds_default_shift_types`), and renames the field in stored idempotent replays. Its downgrade
  restores all three.
- The API's fields become `organisation_id` under `/api/v1`. The only consumer is the HDS client,
  generated from the same contract and changed in the same pull request.
- The two checks that run the previous release on the new schema follow
  `infra/tests/compat-exemptions.txt`, through `infra/tests/compat_exempt.sh`, but only for the
  exact step onto a listed migration: the head's newest migration is the one listed, and the
  base's newest is the one it revises.
  - `infra/tests/compat.sh` then passes with a note.
  - `infra/tests/rolling_update.sh` starts from the head's own release, so the cluster, the
    upgrade across its throwaway migration, the load and Flux's check still run.

  A later migration on top, or a base further back, is checked as usual. Once the rename is on
  `main`, every later base holds it.

## Consequences

- One spelling everywhere in the product's code, schema and contract. Only third-party names and
  the applied migrations, whose code is never edited, keep the American spelling.
- Idempotent replays stored before the migration are rewritten with the new field name, so a retry
  answers as the endpoint does now; the downgrade rewrites them back.
- The exemption is for this revision alone. After the first deployment, a rename follows
  Constitution X and V in full: two releases, and a new API version for a breaking field.
