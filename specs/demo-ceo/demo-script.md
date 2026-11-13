# CEO demo: setup and walkthrough

About ten minutes, in Hebrew, on a laptop. The data comes from the team's real roster workbook (Google Sheets), 6.9 to 21.11. It stays on this Mac.

## Before the meeting (5 minutes)

1. Start Docker Desktop.
2. Start the demo. It parses the workbook and loads it the first time:

   ```bash
   scripts/demo/run_demo.sh "/path/to/the roster workbook.xlsx"
   ```

   After that, `scripts/demo/run_demo.sh` alone reuses the data. `DEMO_FRESH=1` reloads it into a new, empty database.
3. Open http://localhost:8081, switch the language to עברית, and sign in as `admin@hds.test` with the development password.
4. Keep a second tab signed in as `manager1@hds.test`, for the "each manager sees only their own projects" point.

## The walkthrough

| # | Screen | What to show | The point |
| --- | --- | --- | --- |
| 1 | **מבט כולל** (overview, where you land) | The month at a glance:<br>- billing so far and the forecast to the end of the month;<br>- gross pay and margin;<br>- this week's coverage;<br>- people, and how many are near the 48-hour limit. | The whole business in one screen, from the roster that already exists. No more adding things up in a spreadsheet. |
| 2 | Same screen: the weekly bars | Past weeks solid, the current and next weeks lighter (forecast from the roster). | Cash flow forward, not only backward. |
| 3 | Same screen: the project table | Coverage, margin and a status chip for each project. One project shows a **loss**: its pay with night premiums is above its billing. A "*" marks estimated terms. | Spot the loss-making contract before the month ends. |
| 4 | Same screen: the dark panel, **עובדים זמניים** | How many temporary workers there are, how many end soon, and a link to the list. | The CEO's request, already working. |
| 5 | **סידור עבודה** (roster), this week | The same layout as the sheet: projects as rows grouped by client, days as columns, names as blocks, "זמני" chips, warnings. | Familiar to the managers on day one. |
| 6 | Roster, **שבוע הבא** | Next week's drafts: the dashed "ריק" places are the "?" cells from the sheet. Then the warnings strip. | Gaps and rule breaks show before publishing, not after. |
| 7 | Roster: open one project, assign a temporary worker to another project | The warning "עובד זמני משובץ מחוץ לפרויקטים או לתאריכים שלו". | A temporary worker stays on their projects and dates. The auto-scheduler never places them outside. |
| 8 | **צוות** (staff) | Hours this week and this month, the "near 48h" flag, the permanent/temporary filter, and the "זמני · עד …" chip. Then open a temporary worker's card: type, dates (3 months at most) and projects. | Staff control in one table. |
| 9 | **דוח שעות** (hours and costs) | Hours, billing and gross pay by project, with bars. | What used to take a day of spreadsheet work. |
| 10 | Second tab, as a manager | The same overview, showing only that manager's projects. | Each manager sees their own work, nothing else. |

## Facts behind the numbers

Say these if asked:
- **Billing:** person-hours × the project's billing rate.
- **Pay:** gross, with night, overtime and rest-day premiums.
- **Rates:** where the sheet states them (a billing/pay pair or a pay rate), they're used. Elsewhere they're estimates of 110 ₪ billing and 75 ₪ pay an hour, marked with "*".
- **Who is temporary:** in the demo data, a person is temporary if they first appeared in the last twelve weeks, still worked in the last three, and worked on one or two projects. Their engagement runs three months from their first week.
- **The client portal** is not in this demo; it is the next feature (009).

## Stop

Press Ctrl+C in the terminal. To stop the database too:

```bash
docker stop hds-ceo-demo-pg
```
