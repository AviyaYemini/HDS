# Voice and tone

How HDS speaks, in Hebrew and English (specs/007-ux-redesign FR-075). Use it with the
[glossary](glossary.md): the glossary fixes the words, this guide fixes how they are put together.

## Voice

- **Plain.** The words a guard or a shift manager uses, never system words: "Can't work", not
  "Blocked constraint".
- **Short.** One idea per sentence. Labels are one to three words. Buttons are one or two.
- **Calm and exact.** Say what happened and what to do next. No exclamation marks, no blame, no
  jokes in errors.
- **Neutral Hebrew.** Hebrew second person is gendered, so copy never addresses the reader with a
  gendered verb or adjective:
  - buttons and links are nouns or infinitives: שמירה, ביטול, הגשה, לבחירת תאריך;
  - sentences that speak to the reader use the plural, or are rewritten around the thing:
    "הזמינות לשבוע הבא עדיין לא הוגשה", not "לא הגשת";
  - roles are named by their area (ניהול, צוות) rather than a gendered noun where a label allows it.
- **English sentence case** everywhere: "Sign out", "My availability", never "Sign Out".

## Formats

| What | Format | Example (en) | Example (he) |
| --- | --- | --- | --- |
| Date, numeric | dd/MM/yyyy | 07/11/2026 | 07/11/2026 |
| Date, in a header | weekday, day month | Saturday, 7 November | שבת, 7 בנובמבר |
| Time | 24-hour HH:mm, isolated left to right inside Hebrew | 06:00–14:00 | 06:00–14:00 |
| Overnight end | "+1" after the end | 22:00–06:00 +1 | 22:00–06:00 ‎+1 |
| Duration | h and m | 8h 30m | 8 ש׳ 30 ד׳ |
| Money | the locale's currency format from the exact amount | ₪1,234.50 | ‏1,234.50 ₪ |
| Counts | filled over required | 2/3 | 2/3 |

Never place ₪ by hand, never round money for display without saying so, and never mirror numbers,
times or phone numbers (FR-056 to FR-058).

## Patterns

- **Empty states**: what this place is for, then one action named by a verb or noun.
  "No shifts this week. They show here once the roster is published." / "אין משמרות השבוע. הן
  יופיעו כאן כשהסידור יפורסם." Promise only what the product does: no deadline the spec doesn't set.
- **Errors**: what happened, then what to do. Never a code, never "something went wrong" alone.
  "The roster changed while you were editing. Showing the latest version." / "הסידור השתנה בזמן
  העריכה. מוצגת הגרסה העדכנית."
- **Refusals**: "You don't have access to this page" with Go home and Contact your manager, and no
  record named (FR-007). / "אין גישה לדף הזה" עם "לדף הבית" ו"יצירת קשר עם ההנהלה".
- **Confirmations for money**: name the change, its scope and its date. "Change the hourly rate for
  14 people from 45.00 to 48.00 from 1 Dec?" (FR-046).
- **Warnings**: who, which rule, by how much. "Dana: 11 h rest required, 8 h scheduled."
- **Loading**: say nothing for under a second; then the loading bar; beyond ten seconds, say what is
  happening ("Checking rest rules…").
- **Success**: a short toast in the past tense about the thing: "Availability submitted" /
  "הזמינות הוגשה".
