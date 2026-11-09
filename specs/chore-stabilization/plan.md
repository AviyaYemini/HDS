# Stabilization after 008: plan (chore lane)

**Date**: 2026-11-09 | **Base**: `main` at `d1b2497` (008 merged as #24) | **Status**: approved (owner, 2026-11-09)

Three pieces of work before 009, each its own pull request, cut from `main`. The rest of the earlier
stabilization prompt is dropped (owner, 2026-11-09). This is a chore lane: this plan, a short
`tasks.md` (the edit hook requires one, and it lets `/hds-status` count progress) and a review
ledger, but no spec. The standing workflow applies:
- the scoped `make ci-local` per commit, with the full suites in CI;
- one `/code-review` per pull request at PR-ready;
- Conventional Commits;
- the owner pushes and opens each pull request.

## Order

1. **One spelling for organisation**, first: it touches the seed and the client too, so doing it
   first avoids writing either twice.
2. **Client structure.**
3. **Expanded dev dataset**, last: it is written once, in the final spelling and on the final
   client.

## PR 1: one spelling, "organisation" (behaviour change for API consumers)

The tables are already `organisations`; the columns, fields and identifiers said "organization":
22 `organization_id` columns, each with its foreign key, one trigger function that names the
column, 8 API schemas, the 001 contract, the client and the seed.

- **Migration:** one migration (037) renames every `organization_id` column and its foreign key,
  rewrites the trigger function, and renames the field in stored idempotent replays. It has a
  working downgrade. Indexes never held the word, so they keep their names.
- **Code:** models, repositories, services, routers and schemas follow. API fields become
  `organisation_id`. The 001 contract is updated, `packages/api-client` regenerated, and the client
  updated to match. seed_dev, the tests and the docs follow.
- **Exemptions:** the rename is done in one step, so two rules get a recorded exemption (ADR 021):
  - Constitution X: a column rename breaks old code on the new schema. Nothing is deployed, so no
    old pod exists. For the step from 036 to 037 only, `infra/tests/compat.sh` passes with a note,
    and `infra/tests/rolling_update.sh` starts from the head's own release
    (`infra/tests/compat_exempt.sh`).
  - Constitution V: renamed API fields are a breaking change, which would mean `/api/v2`. Our own
    client, regenerated in the same pull request, is the only consumer.
- **Check:** afterwards, `git grep -i organization` shows only third-party names, listed in the
  pull request.

## PR 2: client structure

There is no change to what renders, the routes or the tests' expectations. Each screen file is
left holding the screen, and its parts move beside it in the screen's folder.

| File | Lines | Split into |
| --- | --- | --- |
| `screens/roster/RosterScreen.tsx` | 855 | the screen; `AssignSheet`, `WarningSheet`, `PublishSheet`, `AlertChoice`, `Block` in their own files; the API calls (13) in a `useRoster` hook |
| `screens/admin/AdminScreens.tsx` | 686 | one file per screen (8), with `ManagersEditor` and `NeedsForm` beside theirs |
| `screens/runs/ReviewWeek.tsx` | 687 | the review; `EmptyReasons`, `SlotBlock`; its edits and accepts in a hook |
| `screens/admin/AdminTable.tsx` | 662 | the table; its cell rendering (`cellText`, `PhoneList`) apart |
| `screens/my-work/availability/AvailabilityScreen.tsx` | 633 | the screen; `Cell`; its loading and saving in a hook |
| `screens/account/AccountScreen.tsx` | 403 | `LanguageSwitch`, `ProfileForm`, `Sessions`, `ContactDetailsForm` in their own files |
| `screens/runs/RunScreen.tsx`, `RunsScreen.tsx` | 364, 343 | the hooks (`useJob`, `useNames`, `useRunBar`, `useRuns`, `useRunProjects`) apart from the views; `GenerateForm` and `RunRow` in their own files |

Rules for the split:
- A hook holds the data calls and state, and a view only renders.
- A folder's screens are imported through its `index.ts`, by the routes. Roster and runs import each
  other's pure helpers (`model.ts`, `review.ts`, `rosterHref`) from the module itself, not the index,
  so the two folders never import each other's index in a cycle.
- Nothing moves into `design/`.
- One commit per screen, so each can be checked on its own.

The rest is left out: TanStack Query, and any change to what a screen does.

## PR 3: expanded dev dataset

This is the same kind of seed we have now, not a separate demo:
- the same accounts and dev password;
- small enough to read;
- run by `make up` and the e2e setup.

It goes deeper and wider:
- **Records added:** about 40 people, several managers, 4 clients, 6 sites, 8 projects of both
  types, groups, qualifications, shift types and staffing needs.
- **Pay and calendar:** pay settings with premiums, and special days including the Hebrew holidays
  in range.
- **Availability:** past weeks, and an open window, mostly submitted, some late.
- **Published history:** 4 past weeks plus the current one, made by the greedy scheduler and
  accepted and published through the services, so they obey every rule.
- **A ready proposal:** a week ahead, with empty seats of several causes.
- **Manual edits:** a few, with warnings and recorded overrides.
- **Content everywhere:** every screen from 001 to 008 has something to show.
- **Unchanged:** today's records and accounts stay as they are, so the tests that use them don't
  change.
- **Repeatable:** a fixed random seed, dates relative to today, and re-running adds nothing.
- **Tests:** every published shift passes `hds.scheduling.validate`, and a re-run changes nothing.
- **Docs:** a README section on what the seed contains.

**Open:** whether the e2e setup loads the expanded data too, or only the base. The default is only
the base: the e2e specs count rows and empty states, and the extra data would change them. Both
come from one script with one switch, `make up` loads both, and CI's e2e loads the base.
