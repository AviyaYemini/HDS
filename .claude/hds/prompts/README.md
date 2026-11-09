# Stored Spec Kit prompts

Run with `/hds-run <feature-number> <stage>`, for example `/hds-run 1 specify`.
Stages with no stored prompt (`/speckit-clarify`, `/speckit-tasks`, `/speckit-analyze`, `/speckit-implement`) take no input: run them directly.

| # | Folder | Feature | Stages stored |
| --- | --- | --- | --- |
| 0 | 00-constitution | Constitution | constitution |
| 1 | 01-foundation | Accounts, roles, organisation | specify, plan |
| 2 | 02-availability | Availability and manual shifts | specify, plan |
| 3 | 03-scheduling | Schedule generation | specify, plan |
| 4 | 04-pay-reports | Pay forecast and basic reports | specify, plan |
| 5 | 05-deploy | Deployment and operations | specify, plan |
| 6 | 06-ux-redesign | UX foundation and redesign (feature 007) | specify, plan |
| 7 | 07-auto-scheduler-review | Auto-scheduler review (008) | specify, plan |
| 8 | 08-client-portal | Client portal (009) | specify (stub) |
| 9 | 09-notifications | Notifications and native builds (010) | specify (stub) |
| 10 | 10-hours-worked | Hours worked (011) | specify (stub) |
| 11 | 11-billing-overview | Billing, cash flow and overview (012) | specify (stub) |
| 12 | 12-calendar-exports | Calendar and exports (013) | specify (stub) |
| 13 | 13-audit-log | Audit log (014) | specify (stub) |
| 14 | 14-qualifications | Qualifications and security workflows (015) | specify (stub) |
| 15 | 15-requests | Requests: absences, swaps, open shifts (016) | specify (stub) |
| 16 | 16-pay-compliance | Pay compliance, after legal review (017) | specify (stub) |

Folders 06 to 16 come from the [UX working plan](../../../docs/plans/ux-working-plan.md). 07 was reviewed with the owner and has its plan prompt (2026-11-08). The stubs (08 to 16) are drafts: review each with the owner, and add its plan prompt, before running it.

New features: copy this skeleton into a new folder `17-<name>/specify.md`:

    <Feature name>. Use docs/hds-spec.md rows <IDs> and docs/hds-api.md section <name>. Roles allowed: <roles>. Business rules from the original: <list>. No technology details. Out of scope: <neighbouring features>.
