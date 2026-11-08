/**
 * Calendar dates without a time of day (specs/007-ux-redesign FR-051, FR-054, FR-055).
 *
 * A date is an ISO string, `YYYY-MM-DD`. Arithmetic goes through a day number (days since
 * 1970-01-01 in UTC), so no step crosses a daylight-saving change and no device zone leaks in. Weeks
 * start on Sunday everywhere (D6). "Today" is asked for a named zone, the site's or Asia/Jerusalem,
 * never the device's.
 */

/** A calendar date as its three numbers; `month` runs 1 to 12. */
export interface DateParts {
  year: number;
  month: number;
  day: number;
}

const ISO = /^(\d{4})-(\d{2})-(\d{2})$/;
const DAY_MS = 86_400_000;

/** The parts of an ISO date; throws on anything else. */
export function parts(iso: string): DateParts {
  const match = ISO.exec(iso);
  if (!match) throw new Error(`not a calendar date: ${iso}`);
  return { year: Number(match[1]), month: Number(match[2]), day: Number(match[3]) };
}

/** An ISO date from its parts, zero-padded; the parts are not checked. */
export function iso(year: number, month: number, day: number): string {
  return `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

/** The number of days in `month` of `year`. */
export function daysInMonth(year: number, month: number): number {
  return new Date(Date.UTC(year, month, 0)).getUTCDate();
}

/** Whether the three numbers name a real date (29 February only in a leap year, and so on). */
export function isValidDate(year: number, month: number, day: number): boolean {
  return (
    Number.isInteger(year) &&
    Number.isInteger(month) &&
    Number.isInteger(day) &&
    year >= 1 &&
    month >= 1 &&
    month <= 12 &&
    day >= 1 &&
    day <= daysInMonth(year, month)
  );
}

/** Days since 1970-01-01. */
export function toDayNumber(date: string): number {
  const { year, month, day } = parts(date);
  return Math.round(Date.UTC(year, month - 1, day) / DAY_MS);
}

/** The ISO date `days` days after 1970-01-01. */
export function fromDayNumber(days: number): string {
  const d = new Date(days * DAY_MS);
  return iso(d.getUTCFullYear(), d.getUTCMonth() + 1, d.getUTCDate());
}

/** `date` moved by `days` (negative goes back). */
export function addDays(date: string, days: number): string {
  return fromDayNumber(toDayNumber(date) + days);
}

/** Whole days from `from` to `to`; negative when `to` is earlier. */
export function daysBetween(from: string, to: string): number {
  return toDayNumber(to) - toDayNumber(from);
}

/** Negative, zero or positive as `a` is before, on or after `b`. */
export function compareDates(a: string, b: string): number {
  return toDayNumber(a) - toDayNumber(b);
}

/** The weekday of `date`: 0 Sunday to 6 Saturday. */
export function weekday(date: string): number {
  // 1970-01-01 was a Thursday (4).
  return (((toDayNumber(date) + 4) % 7) + 7) % 7;
}

/** The Sunday that starts the week holding `date`. */
export function startOfWeek(date: string): string {
  return addDays(date, -weekday(date));
}

/** The Saturday that ends the week holding `date`. */
export function endOfWeek(date: string): string {
  return addDays(startOfWeek(date), 6);
}

/** The first day of the month holding `date`. */
export function startOfMonth(date: string): string {
  const { year, month } = parts(date);
  return iso(year, month, 1);
}

/** The last day of the month holding `date`. */
export function endOfMonth(date: string): string {
  const { year, month } = parts(date);
  return iso(year, month, daysInMonth(year, month));
}

/** The first day of the month `count` months after the month holding `date`; the day is dropped. */
export function addMonths(date: string, count: number): string {
  const { year, month } = parts(date);
  const index = year * 12 + (month - 1) + count;
  return iso(Math.floor(index / 12), (index % 12) + 1, 1);
}

/** Whether `date` lies in `[start, end]`, inclusive. */
export function isWithin(date: string, start: string, end: string): boolean {
  const n = toDayNumber(date);
  return n >= toDayNumber(start) && n <= toDayNumber(end);
}

/**
 * The calendar date it is now in `timeZone`.
 *
 * Read from `Intl`, so the site's zone decides the date even on a device set to another zone. The
 * instant defaults to now, and tests pass a fixed one.
 */
export function todayIn(timeZone: string, now: Date = new Date()): string {
  return calendarDateIn(now, timeZone);
}

const dateParts = new Map<string, Intl.DateTimeFormat>();

/**
 * The calendar date of `instant` in `timeZone`, read part by part, so it does not depend on how any
 * locale lays a date out (the Intl polyfill on Hermes carries only the app's two languages).
 */
export function calendarDateIn(instant: Date, timeZone: string): string {
  let formatter = dateParts.get(timeZone);
  if (formatter === undefined) {
    formatter = new Intl.DateTimeFormat("en", { timeZone, year: "numeric", month: "numeric", day: "numeric" });
    dateParts.set(timeZone, formatter);
  }
  const found: Record<string, string> = {};
  for (const part of formatter.formatToParts(instant)) found[part.type] = part.value;
  const year = Number(found.year);
  const month = Number(found.month);
  const day = Number(found.day);
  if (!isValidDate(year, month, day)) throw new Error(`could not read the date in ${timeZone}`);
  return iso(year, month, day);
}

const TYPED = /^\s*(\d{1,2})\s*[/.\-\s]\s*(\d{1,2})\s*[/.\-\s]\s*(\d{2}|\d{4})\s*$/;

/**
 * A date someone typed as day, month, year with any separator (`7/11/2026`, `7.11.2026`,
 * `7-11-2026`, `07 11 26`): the ISO date, or null when it is not a real date (FR-051, D1).
 *
 * The day always comes first (O13: dd/MM/yyyy), so `7/11` is 7 November, never 11 July. A two-digit
 * year means 20xx.
 */
export function parseTypedDate(text: string): string | null {
  const match = TYPED.exec(text);
  if (!match) return null;
  const day = Number(match[1]);
  const month = Number(match[2]);
  const year = match[3]!.length === 2 ? 2000 + Number(match[3]) : Number(match[3]);
  return isValidDate(year, month, day) ? iso(year, month, day) : null;
}

/** `date` as dd/MM/yyyy (FR-055, O13). */
export function formatNumericDate(date: string): string {
  const { year, month, day } = parts(date);
  return `${String(day).padStart(2, "0")}/${String(month).padStart(2, "0")}/${String(year).padStart(4, "0")}`;
}
