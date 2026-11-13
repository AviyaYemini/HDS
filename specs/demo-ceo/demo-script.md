# CEO demo: setup and walkthrough

About ten minutes, in Hebrew, on a laptop. The data comes from the team's real roster workbook (Google Sheets), 6.9 to 21.11. It stays on this Mac.

## Before the meeting (5 minutes)

1. Start Docker Desktop.
2. Start the demo. It parses the workbook and loads it the first time (use `DEMO_FRESH=1` before the meeting, so test runs and edits are cleared):

   ```bash
   scripts/demo/run_demo.sh "/path/to/the roster workbook.xlsx"
   ```

   After that, `scripts/demo/run_demo.sh` alone reuses the data. `DEMO_FRESH=1` reloads it into a new, empty database.
3. Open http://localhost:8081, switch the language to עברית, and sign in as `admin@hds.test` with the development password.
4. Keep a second tab signed in as `manager1@hds.test`, for the "each manager sees only their own projects" point.

## The walkthrough

| # | Screen | What to show | The point |
| --- | --- | --- | --- |
| 1 | **מבט כולל** (overview, where you land) | The month at a glance:<br>- billing so far and the forecast to the end of the month;<br>- gross pay and margin;<br>- this week's coverage;<br>- people. | The whole business in one screen, from the roster that already exists. No more adding things up in a spreadsheet. |
| 2 | Same screen: the weekly bars and the candles | Bars: past weeks solid, the current and next weeks lighter (forecast from the roster). Candles: each week's daily billing (open, close, highest and lowest day). Switch to רבעון to see the trend. | Cash flow forward, not only backward. |
| 2a | Same screen: the two donuts | Billing by client (who the business depends on) and the permanent/temporary mix. | Concentration risk and staffing mix at a glance. |
| 2b | Same screen: the changes on the key figures | ▲/▼ against last month: so far against the same days in October, and the forecast against October's total. Next week's coverage sits beside this week's. | Is the business ahead or behind, at a glance. |
| 3 | Same screen: the project table | Coverage, margin and a status chip for each project. One project shows a **loss**: its pay with night premiums is above its billing. A "*" marks estimated terms. | Spot the loss-making contract before the month ends. |
| 4 | Same screen: the dark panel, **עובדים זמניים** | How many temporary workers there are, how many end soon, and a link to the list. | The CEO's request, already working. |
| 5 | **סידור עבודה** (roster), this week | The same layout as the sheet: projects as rows grouped by client, days as columns, names as blocks, "זמני" chips. | Familiar to the managers on day one. |
| 6 | Roster, **שבוע הבא** | Next week's drafts: the dashed "ריק" places are the "?" cells from the sheet. | Gaps show before publishing, not after. |
| 6a | **הרצות שיבוץ** (schedule runs): **הרצה חדשה**, "השבוע הבא", **כל פרויקטי האבטחה**, **הפקה** | In a few seconds every place is filled (68/68 on the demo data), keeping temporary workers on their own projects and dates. Show the proposal as a roster week, then accept or discard. | A whole company's week, drafted in seconds; the manager reviews and accepts. |
| 7 | On a phone (or a narrow window) | The roster one day at a time with a day strip; the overview and staff as cards. | The same tool in the field. |
| 8 | **צוות** (staff) | Hours this week and this month, the permanent/temporary filter, and the "זמני · עד …" chip. Then open a temporary worker's card: type, dates (3 months at most) and projects. | Staff control in one table. A temporary worker stays on their projects and dates; the auto-scheduler never places them outside. |
| 8a | On a phone, sign in as `emp001@demo.hds.test` (the employee with the most shifts) | The guard's app: the next shift, the availability deadline, my shifts, and the expected gross pay for the month by rate. | Every guard sees their own week and pay on their phone. |
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
