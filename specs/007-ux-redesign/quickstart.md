# Quickstart: seeing 007 work

## Prerequisites

- `make env && make up` (the local stack, as in 006), or the API run by Playwright's web server.
- Seeded users from `hds.scripts.seed_dev`: an employee, a manager of one project, an admin and a
  client user (password `Dev1234!`).
- Tests only on `hds_p3_test` or a run's own throwaway container.

## Run the app

```bash
cd apps/client
EXPO_PUBLIC_PREVIEWS=1 pnpm web      # previews and the gallery on
pnpm web                              # as production: no previews
```

## Check a group

| Group | Command | Expected |
| --- | --- | --- |
| A | `pnpm test -- tokens` then `pnpm e2e -- ux/a-design-system.spec.ts` | contrast test green; gallery renders Option B in he and en, the date picker passes keyboard and preset checks, axe clean |
| B | `pnpm e2e -- ux/b-roles.spec.ts` | each role's exact tab set; the client sees no staff screen; a forbidden link shows the no-access page; previews absent with the switch off |
| C | `pnpm e2e -- ux/c-my-work.spec.ts` | Not submitted → Draft → Submitted → Locked; "counted as unavailable" after a missed deadline; Upcoming and Past split; pay breakdown sums to the total |
| D | `pnpm e2e -- ux/d-roster.spec.ts` | partial and extended bars, overnight continuation, filled/required, tap-assign, warning badge with numbers, publish review counts, a manager limited to their projects |
| E | `pnpm e2e -- ux/e-schedule-runs.spec.ts` | preset range, progress, the honesty label, accept and discard |
| F | `pnpm e2e -- ux/f-admin.spec.ts` | sort, filter, saved views kept in the address, picker finds Hebrew and English names, error summary takes focus, specific pay confirmation |
| G | `pnpm e2e -- ux/g-reports.spec.ts` | the current pay period by default; key figures match the table |
| H | `pnpm e2e -- ux/h-a11y-sweep.spec.ts` | every screen, both languages and both themes, axe with no serious issues |

API side for each group: `make ci-local` (scoped) while building; `make ci-local-db` once at PR-ready.

## Manual checks per release

- VoiceOver (iOS device) and TalkBack (Android device) in Hebrew and English.
- The Intl polyfills on a physical Android phone (dates, money, plurals).
- A grayscale pass over the roster and the availability grid.
