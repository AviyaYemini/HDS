# UI routes, shells and previews

Routes are expo-router file routes; on the web each is a real address (A19). Every route declares
the capability it needs; navigation lists only routes the user's capabilities allow. A route the
user may not open renders the no-access page (FR-007). Playwright specs select by role and
accessible name first, and by `data-testid` where a role is ambiguous; the ids below are stable
contract between the [CORE] specs and the [UI] screens.

## Staff shell (`(staff)`)

| Group | Route | Capability | Phone tab | Test id |
| --- | --- | --- | --- | --- |
| — | `/home` | `my_work.read` | Home | `screen-home` |
| My work | `/my/availability` | `my_work.availability` | Availability | `screen-availability` |
| My work | `/my/shifts` (`?view=upcoming\|past`) | `my_work.read` | My shifts | `screen-my-shifts` |
| My work | `/my/shifts/[id]` | `my_work.read` | — | `screen-shift-detail` |
| My work | `/my/pay` (`?period=`) | `my_work.read` | Pay | `screen-my-pay` |
| Manage | `/roster` (`?week=&span=&group=&project=`) | `roster.read` | Roster | `screen-roster` |
| Manage | `/roster/submissions` | `availability.read_others` | — | `screen-submissions` |
| Manage | `/runs`, `/runs/[id]` | `schedule.generate` | — | `screen-runs` |
| Manage | `/reports/work-log`, `/reports/hours` | `reports.read` | Reports | `screen-work-log`, `screen-hours` |
| Admin | `/admin/staff`, `/admin/clients`, `/admin/sites`, `/admin/projects`, `/admin/shift-templates`, `/admin/staffing-needs` | `*.manage` | More | `screen-admin-<name>` |
| Admin | `/admin/special-days`, `/admin/pay-settings` | `special_days.edit`, `pay_settings.edit` (read for `pay.read_others`) | More | |
| — | `/account` | `account.self` | More | `screen-account` |

Phone tabs: employee Home, Availability, My shifts, Pay, More; manager and admin Home, Roster, My
work, Reports, More (N6). Web: a side navigation with the three groups, on the right in Hebrew.

## Client shell (`(client)`)

| Route | Capability | Tab | Test id |
| --- | --- | --- | --- |
| `/client` (home: one card per project) | `client_portal.read` | Projects | `screen-client-home` |
| `/client/projects/[id]` | `client_portal.read` | — | `screen-client-project` |
| `/client/billing` | preview (009) | Billing | `preview-billing` |
| `/client/requests` | preview (009) | Requests | `preview-requests` |
| `/client/contact` | `client_portal.read` | Contact | `screen-contact` |
| `/account` | `account.self` | (header) | `screen-account` |

## Shared

`/sign-in`, `/set-password` (invitation, in the invitee's language), `/no-access` (`screen-no-access`,
also rendered in place for any forbidden route), `/help`, `/accessibility` (statement text from the
owner), `/dev/gallery` (previews switch only).

Persistent on every screen: sign-out (`action-sign-out`), help, contact, the accessibility statement
(`link-accessibility`).

## Navigation test ids (b-roles.spec.ts)

- **Phone** (`< 768 px` wide): a bottom bar `nav-tabs` whose items are `tab-<key>`. Employee: `home`,
  `availability`, `my-shifts`, `pay`, `more`. Manager and Admin: `home`, `roster`, `my-work`,
  `reports`, `more`. Client: `projects`, `contact`, and with previews on also `billing` and
  `requests` (previews of 009, marked as below).
- **Desktop**: a side navigation `nav-side` with groups `nav-group-my-work`, `nav-group-manage`,
  `nav-group-admin` (client: one group `nav-group-client`) whose items are `nav-<key>`:
  - Employee: `home`, `availability`, `my-shifts`, `pay`.
  - Manager: the employee's, plus `roster`, `submissions`, `runs`, `work-log`, `hours`, `staff`,
    `clients`, `sites`, `projects`, `shift-templates`, `staffing-needs`, `pay-settings`.
  - Admin: the manager's, plus `special-days`.
  - Client: `client-home`, `contact`, and with previews on also `billing` and `requests`.
  - A navigation item that opens a preview carries `data-preview="true"` and is left out of these
    sets; previews are checked by their routes below. With previews off no such item is drawn, so
    every tab and side item a role is offered opens a screen of its own, never the 404 page.
- **Every screen**: `action-sign-out`, `link-help`, `link-contact`, `link-accessibility`, and the
  offline banner `banner-offline` while the network is down.
- **Pages**: `screen-no-access` with `action-go-home`; `screen-not-found` (an unknown address) with
  `action-go-home`; `splash` (static markup in the web page, shown until the app has started).
- **Account**: `screen-account` with the language choice `language-he` and `language-en`
  (`role="radio"`), and the profile's phone `profile-phone` with `action-save-profile`.
- **Sign-in** (kept from 001's specs, which still use them): `email`, `password`, `sign-in-btn`,
  and the refusal `error-message`.
- **Deep links**: the web keeps no session across a page load (ADR 010), so an address of the
  signed-in area opened in a fresh page shows sign-in and, once signed in, opens that address
  (path and query), where the guard then decides as usual (the no-access page for a forbidden one,
  FR-007). The address to return to is only ever a path of this app, never another site. An
  unknown address shows `screen-not-found`, signed in or not.

## My work test ids (c-my-work.spec.ts)

- **Availability** (`/my/availability`, `?window=<id>` opens one window): `screen-availability`;
  the week's cells are `availability-cell` buttons with `data-date` (`YYYY-MM-DD`),
  `data-shift-type` (a shift type's id, or `all-day`) and `data-state` (`unmarked`, `open`,
  `close`, `blocked`); a tap moves to the next state in that order. `availability-prev-week`,
  `availability-next-week`; `availability-rules` (FR-016); absences as `absence-band` with
  `data-date`.
- **Banner**: `availability-banner` with `data-status` (`not_submitted`, `draft`, `submitted`,
  `locked`) and, when locked, `data-counted-unavailable` (`true` or `false`). Before the deadline,
  without a submission, it holds `availability-not-submitted-warning` (not submitting means not
  being scheduled).
- **Actions**: `action-review` opens `availability-review` (a dialog with one `review-row` per day
  that has a mark) whose `action-submit` submits; `action-undo` (in the notice after a change);
  `action-copy-last-week`.
- **My shifts** (`/my/shifts?view=upcoming|past`): `screen-my-shifts`; `view-upcoming` and
  `view-past` (`role="radio"`); `next-shift-card` first in Upcoming; `week-group` (Upcoming, with
  `data-week-start`) and `period-group` (Past, with `data-period`) holding `shift-row` items with
  `data-shift-id`; each row's `shift-time` shows "+1" when it ends the next day. A row opens
  `/my/shifts/<id>`: `screen-shift-detail` with the site's address, `shift-contact` items and
  `action-add-to-calendar`. Previews here: `preview-report-hours`, `preview-cant-make-it`.
- **My pay** (`/my/pay?period=YYYY-MM`): `screen-my-pay`; `pay-period-picker`; `view-hours` and
  `view-pay` (`role="radio"`); `pay-total` and `pay-breakdown-row` items, each with `data-amount`
  (the exact decimal string from the API); `pay-estimate-label`. In Hours: `preview-approved-hours`.

## Roster test ids (d-roster.spec.ts)

- **Week** (`/roster?project=<id>&week=YYYY-MM-DD`, the week's Sunday): `screen-roster`; seven
  `roster-day` columns with `data-date`; a `roster-row` per shift template with
  `data-shift-type`; each shift a `roster-block` with `data-shift-id`, `data-status` (`draft`,
  `published`, `changed`) and `data-editable` (`true` or `false`); its `block-coverage` reads
  filled over required (`2/3`); each person a `block-person` with `data-user-id` and
  `data-own-times`, drawn as a `person-bar` whose box is the person's own times against the block's
  (a half shift narrower than the block, extra hours past its edge); each unfilled seat an
  `empty-seat`. An overnight shift sits on the day it starts, with a `roster-continuation`
  (`data-shift-id`) on the next day, and its time shows "+1".
- **Assigning without dragging**: tapping a block or an `empty-seat` opens `assign-sheet` with
  `assign-option` items (`data-user-id`, those who prefer the shift first); choosing one assigns.
  At most three taps from the roster to an assigned seat. Tapping a `block-person` offers
  `action-unassign` and changing their own times.
- **Warnings**: `block-warning-badge` (its text is the count) opens `warning-detail`, which names
  the person, the rule and the numbers; `action-override` asks for `override-reason` and
  `action-confirm-override`.
- **Publishing**: `action-publish` opens `publish-review` with `review-shifts`, `review-people`,
  `review-unfilled` and `review-warnings` (each holds its number), `notify-everyone` and
  `publish-silently` (`role="radio"`), and `action-confirm-publish`. `action-undo-roster` steps
  back one unpublished change.
- **Stale save**: `banner-roster-changed`.
- **Submissions** (`/roster/submissions?window=<id>`): `screen-submissions` with
  `submission-row` items (`data-user-id`, `data-status`).
- **Phone**: `roster-agenda`, one day's shifts as `roster-block` items, with the same assign path
  and `action-publish`.
- Previews here: `preview-cost-strip` (pay viewers), `preview-qualification-expiry` in the
  assign sheet.

## Schedule run test ids (e-schedule-runs.spec.ts)

- `/runs`: `screen-runs`, `run-row` items (`data-job-id`, `data-status`); `action-new-run` opens
  the generate form with `date-range` (the presets of the date picker) and `action-generate`.
- `/runs/<id>`: `screen-run`; while it runs, the `loading-bar` is determinate
  (`aria-valuenow` from elapsed over `time_limit_seconds`, only rising); when done,
  `run-honesty-label` ("Optimal" or "Best found in N s"), `run-filled` (filled over required),
  `action-accept` and `action-discard`; `preview-proposal-diff` while previews are on.

## Preview stubs (only with `EXPO_PUBLIC_PREVIEWS`)

Each renders an Option B empty state with `data-testid="preview-<key>"`, the feature that brings it,
and no live controls. Routed previews have their own address; the others sit inside a screen and
are checked by that screen's group.

| Address | Test id | Roles |
| --- | --- | --- |
| `/client/billing` | `preview-billing` | client |
| `/client/requests` | `preview-requests` | client |
| `/manage/requests` | `preview-requests` | manager, admin |
| `/manage/overview` | `preview-overview` | manager, admin |
| `/manage/approvals` | `preview-approvals` | manager, admin |
| `/my/requests` | `preview-absence-requests` | employee, manager, admin |
| `/inbox` | `preview-inbox` | all |
| `/account/notifications` | `preview-notification-prefs` | all |
| `/admin/audit-log` | `preview-audit-log` | admin |

In a build with previews off, each of these addresses shows `screen-not-found`.

| Key | Role (capability) | Where | Feature |
| --- | --- | --- | --- |
| `billing` | client | client shell | 009 |
| `requests` | client; managers (`client_requests.answer`) | client shell; Manage | 009 |
| `inbox`, `notification-prefs` | all | header; Account | 010 |
| `report-hours`, `cant-make-it` | employee | My shifts | 011 |
| `approved-hours` | employee | My pay › Hours | 011 |
| `overview` | manager, admin | Manage › Home | 012 |
| `cost-strip` | `pay.read_others` | Roster | 012 |
| `exports`, `calendar-feed` | all | Reports; My shifts | 013 |
| `history`, `audit-log` | admin | each record; Admin | 014 |
| `qualification-expiry` | admin, manager | Staff; roster pickers | 015 |
| `absence-requests`, `swaps` | employee; managers | My work; Manage | 016 |
| `proposal-diff` | manager | Schedule runs | 008 |
| `pay-rules-by-group` | admin | Pay settings | 017 |
