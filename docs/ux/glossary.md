# HDS glossary (Hebrew and English)

The one name for every domain term, in both languages (specs/007-ux-redesign FR-075). Copy in
`apps/client/src/locales/` uses these words and no synonyms. Where a row names a locale key, the
test `apps/client/tests/unit/locales.parity.test.ts` checks that the key, once it exists, carries
exactly these words. Add a row before adding a new domain term to the app.

How to read it: **Term** is the English, **עברית** the Hebrew, **Avoid** the words that must not be
used for it. The Hebrew is gender-neutral (the voice guide, [voice-and-tone.md](voice-and-tone.md)):
nouns rather than gendered verbs or adjectives about the reader.

## Navigation and screens

| Key | Term | עברית | Avoid |
| --- | --- | --- | --- |
| `nav.roster` | Roster | סידור עבודה | Schedule (for the weekly view), מי עבד, לוח זמנים |
| `nav.work_log` | Work log | יומן עבודה | Who worked, מי עבד |
| `reports.work_log` | Work log | יומן עבודה | Who worked, מי עבד |
| `nav.hours` | Hours report | דוח שעות | Timesheet |
| `nav.availability` | Availability | זמינות | Constraints, אילוצים (on screens) |
| `availability.title` | My availability | הזמינות שלי | |
| `nav.shifts` | My shifts | המשמרות שלי | |
| `shifts.title` | My shifts | המשמרות שלי | |
| `nav.pay` | My pay | השכר שלי | Salary, משכורת |
| `pay.title` | My pay | השכר שלי | |
| `nav.runs` | Schedule runs | הרצות שיבוץ | Auto-schedule, אלגוריתם |
| `nav.account` | Account | חשבון | Profile (for the screen), פרופיל |
| `nav.help` | Help | עזרה | |
| `nav.contact` | Contact | יצירת קשר | Contact us, צרו קשר |
| `nav.accessibility` | Accessibility statement | הצהרת נגישות | |
| `auth.sign_in` | Sign in | כניסה | Log in, התחברות |
| `auth.sign_out` | Sign out | יציאה | Log out, Sign Out, התנתקות |

Navigation groups: **My work** / **העבודה שלי**, **Manage** / **ניהול**, **Admin** / **ניהול מערכת**.
Client shell: **Projects** / **פרויקטים**, **Billing** / **חיוב**, **Requests** / **פניות**,
**Contact** / **יצירת קשר**.

## Session and connection

| Key | Term | עברית | Avoid |
| --- | --- | --- | --- |
| `unreachable.title` | Can't reach HDS | אין חיבור ל-HDS | Offline, Network error, שגיאת רשת |
| `unreachable.sign_in` | Sign in again | התחברות מחדש | Log in, Login, לוגין |
| `common.retry` | Try again | ניסיון נוסף | Retry, Reload, רענון |
| `unreachable.body` | Your device is signed in, but HDS did not answer. Check the connection and try again; signing in again also works. | החשבון עדיין מחובר במכשיר הזה, אבל HDS לא ענה. כדאי לבדוק את החיבור לרשת ולנסות שוב; אפשר גם להתחבר מחדש. | המכשיר מחובר (reads as "connected to the network") |
| `account.session_renewed` | The session on the device you chose was just renewed. Press Sign out again. | החיבור במכשיר שבחרת התחדש הרגע. אפשר ללחוץ שוב על 'יציאה'. | המכשיר הזה (the chip on the caller's own row) |

## Availability

| Key | Term | עברית | Avoid |
| --- | --- | --- | --- |
| `availability.kind.blocked` | Can't work | אין אפשרות לעבוד | Unavailable, לא זמין, לא זמינה |
| `availability.kind.open` | Would like to work | עדיפות לעבוד | Prefer, רוצה, רוצה לעבוד |
| `availability.kind.close` | Would rather not work | עדיפות לא לעבוד | Dislike, לא רוצה |
| `availability.kind.absence` | Absence | היעדרות | Time off, חופש (for the kind) |
| `availability.kind.unmarked` | No mark (available) | ללא סימון (זמינות) | Empty |
| `availability.status.not_submitted` | Not submitted | לא הוגש | Missing, חסר |
| `availability.status.draft` | Draft | טיוטה | Unsaved |
| `availability.status.submitted` | Submitted | הוגש | Sent, נשלח |
| `availability.status.locked` | Locked | נעול | Closed, סגור |
| `availability.submit` | Submit | הגשה | Send, שליחה, שלח |
| `availability.review` | Review | סקירה | Confirm, אישור |
| | Availability window | חלון הגשה | Period, תקופה |
| | Deadline | מועד אחרון | Due date |
| | Counted as unavailable | נחשב כלא זמין לשיבוץ | Blocked (for this case) |

Absence types: **Leave** / **חופשה**, **Reserve duty** / **מילואים**, **Sick** / **מחלה**.

## Roster and scheduling

| Key | Term | עברית | Avoid |
| --- | --- | --- | --- |
| | Shift | משמרת | Slot (on screens) |
| | Shift template | תבנית משמרת | Shift type (on screens), סוג משמרת |
| | Assignment | שיבוץ | Allocation |
| | Assign | שיבוץ | Book, הוסף |
| | Unfilled | לא מאויש | Empty, ריק |
| | Coverage | איוש | Staffing level |
| | Staffing need | דרישת איוש | Requirement (on screens) |
| | Publish | פרסום | Release, שחרור |
| | Draft (shift) | טיוטה | Unpublished |
| | Changed since publishing | השתנה מאז הפרסום | Dirty |
| | Warning | אזהרה | Error (for a warning) |
| | Override (with a reason) | עקיפה (עם סיבה) | Ignore, התעלמות |
| | Hard stop | חסימה | |
| | Weekly cap | תקרה שבועית | Limit |
| | Rest between shifts | מנוחה בין משמרות | Break |
| | Proposal | הצעה | Suggestion, Result |
| | Optimal | אופטימלי | Perfect |
| | Best found in N s | הטוב ביותר שנמצא תוך N שניות | |
| | Week template | תבנית שבוע | Preset |
| | Submission tracker | מעקב הגשות | |

## Organisation

| Key | Term | עברית | Avoid |
| --- | --- | --- | --- |
| | Staff (the list) | צוות | Employees (as a title), עובדים |
| | Client | לקוח | Customer (on screens) |
| | Project | פרויקט | Job |
| | Site | אתר | Location, מיקום |
| | Group | קבוצה | Team (for this entity) |
| | Special day | יום מיוחד | Holiday (for the entity), חג |
| | Pay settings | הגדרות שכר | Payroll |

## Pay

| Key | Term | עברית | Avoid |
| --- | --- | --- | --- |
| | Pay period | תקופת שכר | Month (when the period is meant) |
| | Estimate, before tax | הערכה, לפני מס | Net, נטו |
| | Gross pay | שכר ברוטו | Salary |
| | Hourly rate | תעריף שעתי | Wage |
| | Overtime | שעות נוספות | Extra hours (for pay) |
| | Weekly rest | מנוחה שבועית | Weekend, סופ"ש |
| | Night shift | משמרת לילה | |

## Dates

| Key | Term | עברית | Avoid |
| --- | --- | --- | --- |
| `date.preset.custom` | Custom | מותאם אישית | Adjusted, מותאם (alone it reads as "adjusted") |

## Phrases never used

- "You are not allowed to do this." and any refusal that names no way forward (FR-007).
- "מי עבד" as a screen name (FR-025).
- Gendered imperatives on buttons: בחר, שמור, הוסף; use בחירה, שמירה, הוספה (D13).
