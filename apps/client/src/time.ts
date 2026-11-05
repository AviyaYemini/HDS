/**
 * Dates and times as people read them. Shift times are shown in the site's IANA zone, not the
 * device's, so a night shift reads the same wherever the phone is; `Intl` applies the zone's rules,
 * including the days the clocks change.
 */

const DAY: Intl.DateTimeFormatOptions = { weekday: "short", month: "short", day: "numeric" };
const CLOCK: Intl.DateTimeFormatOptions = { hour: "2-digit", minute: "2-digit", hourCycle: "h23" };

/** A shift's day, start and end at the site; `endDay` names the end's day when it differs. */
export interface ShiftTimes {
  day: string;
  start: string;
  end: string;
  endDay: string | null;
}

const formatters = new Map<string, Intl.DateTimeFormat>();

/** A formatter for `locale`, `timeZone` and `options`, built once: building one is costly. */
function formatter(locale: string, timeZone: string | undefined, options: Intl.DateTimeFormatOptions) {
  const key = JSON.stringify([locale, timeZone ?? "", options]);
  let found = formatters.get(key);
  if (found === undefined) {
    found = new Intl.DateTimeFormat(locale, { ...options, timeZone });
    formatters.set(key, found);
  }
  return found;
}

function format(instant: Date, locale: string, timeZone: string, options: Intl.DateTimeFormatOptions) {
  return formatter(locale, timeZone, options).format(instant);
}

/** The times of a shift from `startsAt` to `endsAt` (ISO instants) in `timeZone`. */
export function shiftTimes(startsAt: string, endsAt: string, timeZone: string, locale: string): ShiftTimes {
  const start = new Date(startsAt);
  const end = new Date(endsAt);
  const day = format(start, locale, timeZone, DAY);
  const lastDay = format(end, locale, timeZone, DAY);
  return {
    day,
    start: format(start, locale, timeZone, CLOCK),
    end: format(end, locale, timeZone, CLOCK),
    endDay: lastDay === day ? null : lastDay,
  };
}

/** A calendar date (`YYYY-MM-DD`) as that same date, whatever the device's zone. */
export function formatDay(date: string, locale: string): string {
  return format(new Date(`${date}T00:00:00Z`), locale, "UTC", DAY);
}

/** An instant with its date and time, in `timeZone` or the device's zone. */
export function formatInstant(instant: string, locale: string, timeZone?: string): string {
  return formatter(locale, timeZone, { dateStyle: "medium", timeStyle: "short" }).format(new Date(instant));
}

/** The name of weekday `day` (0 Sunday to 6 Saturday). */
export function formatWeekday(day: number, locale: string): string {
  // 2027-06-06 is a Sunday.
  return format(new Date(Date.UTC(2027, 5, 6 + day)), locale, "UTC", { weekday: "long" });
}

/** The calendar date after `date` (`YYYY-MM-DD`). */
export function nextDay(date: string): string {
  const next = new Date(`${date}T00:00:00Z`);
  next.setUTCDate(next.getUTCDate() + 1);
  return next.toISOString().slice(0, 10);
}

/** Today's calendar date on the device. */
export function today(): string {
  const now = new Date();
  const month = String(now.getMonth() + 1).padStart(2, "0");
  const day = String(now.getDate()).padStart(2, "0");
  return `${now.getFullYear()}-${month}-${day}`;
}

/** The device's current month, `YYYY-MM`. */
export function currentMonth(): string {
  return today().slice(0, 7);
}

/** The month `count` months after `month` (`YYYY-MM`); a negative count goes back. */
export function addMonths(month: string, count: number): string {
  const [year, number] = month.split("-").map(Number) as [number, number];
  const moved = new Date(Date.UTC(year, number - 1 + count, 1));
  return moved.toISOString().slice(0, 7);
}

/** A month (`YYYY-MM`) as people read it, such as "October 2026". */
export function formatMonth(month: string, locale: string): string {
  return format(new Date(`${month}-01T00:00:00Z`), locale, "UTC", { month: "long", year: "numeric" });
}

/** The first and last day (`YYYY-MM-DD`) of a month (`YYYY-MM`). */
export function monthBounds(month: string): { first: string; last: string } {
  const next = new Date(`${addMonths(month, 1)}-01T00:00:00Z`);
  next.setUTCDate(0);
  return { first: `${month}-01`, last: next.toISOString().slice(0, 10) };
}
