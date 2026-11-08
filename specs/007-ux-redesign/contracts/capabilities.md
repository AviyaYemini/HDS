# Capability map

One table, `hds.core.capabilities`, served on `GET /me` and enforced in services. The UI reads only
this list. A contract test asserts that each role's list equals this table, and a client unit test
asserts that every capability the UI reads appears here. Rows follow `docs/hds-spec.md` section 2;
rows marked **Proposed** come from 007's spec.

| Capability | Employee | Manager | Admin | Client | Notes |
| --- | --- | --- | --- | --- | --- |
| `my_work.read` (My shifts, My pay) | ✓ | ✓ | ✓ | | manager inherits employee |
| `my_work.availability` (submit own) | ✓ | ✓ | ✓ | | |
| `roster.read` (all projects) | | ✓ | ✓ | | |
| `roster.edit:project` | | managed projects | all | | **Proposed** (O9) |
| `roster.publish:project` | | managed projects | all | | the owner's decision of 2026-11-08: a Manager publishes only the projects they manage |
| `roster.override:project` | | managed projects | all | | **Proposed** (S10) |
| `schedule.generate` | | ✓ | ✓ | | |
| `availability.read_others` | | ✓ | ✓ | | submission tracker |
| `availability.edit_others` | | | ✓ | | original: admin only |
| `reports.read` (work log, hours) | | ✓ | ✓ | | |
| `pay.read_others` | | per 005 visibility | ✓ | | 005 FR-022 |
| `pay.edit_shift` | | | ✓ | | |
| `pay_settings.edit` | | | ✓ | | |
| `staff.manage`, `clients.manage`, `sites.manage`, `projects.manage` | | ✓ | ✓ | | create and edit |
| `staff.delete`, `clients.delete`, `projects.delete` | | | ✓ | | |
| `special_days.edit` | | | ✓ | | |
| `organisation.contact.edit` | | | ✓ | | **Proposed** |
| `projects.read` | | ✓ | ✓ | ✓ | a client's own projects only |
| `client_portal.read` (own projects) | | | | ✓ | never employee fields |
| `client_portal.requests` | | | | ✓ | preview until 009 |
| `client_requests.answer` | | ✓ | ✓ | | preview until 009 |
| `account.self` | ✓ | ✓ | ✓ | ✓ | profile, language, sessions |

`managed_project_ids` qualifies every `:project` capability for managers; for admins every project
qualifies.
