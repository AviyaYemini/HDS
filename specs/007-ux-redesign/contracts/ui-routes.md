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

## Preview stubs (only with `EXPO_PUBLIC_PREVIEWS`)

Each renders an Option B empty state with `data-testid="preview-<key>"`, the feature that brings it,
and no live controls.

| Key | Role (capability) | Where | Feature |
| --- | --- | --- | --- |
| `billing` | client | client shell | 009 |
| `requests` | client; managers (`client_requests.answer`) | client shell; Manage | 009 |
| `inbox`, `notification-prefs` | all | header; Account | 010 |
| `report-hours`, `cant-make-it` | employee | My shifts | 011 |
| `overview` | manager, admin | Manage › Home | 012 |
| `cost-strip` | `pay.read_others` | Roster | 012 |
| `exports`, `calendar-feed` | all | Reports; My shifts | 013 |
| `history`, `audit-log` | admin | each record; Admin | 014 |
| `qualification-expiry` | admin, manager | Staff; roster pickers | 015 |
| `absence-requests`, `swaps` | employee; managers | My work; Manage | 016 |
| `proposal-diff` | manager | Schedule runs | 008 |
| `pay-rules-by-group` | admin | Pay settings | 017 |
