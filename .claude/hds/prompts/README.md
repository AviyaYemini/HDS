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

v1 features (hours worked, billing and cash flow, overview, client portal, calendar and exports, push and native builds, audit log): copy this skeleton into a new folder `06-<name>/specify.md`:

    <Feature name>. Use docs/hds-spec.md rows <IDs> and docs/hds-api.md section <name>. Roles allowed: <roles>. Business rules from the original: <list>. No technology details. Out of scope: <neighbouring features>.
