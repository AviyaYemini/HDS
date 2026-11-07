/**
 * Clock times and spans as people read them (specs/007-ux-redesign FR-053, D5; O13).
 *
 * Times are 24-hour `HH:mm` in the site's zone. A span whose end falls on a later calendar day
 * carries "+1" (or "+2"). Every time or span is isolated left to right, so it reads the same inside
 * Hebrew (D8). The clock icon that goes with a time is never mirrored (FR-057); that is the icon's
 * business, not this module's.
 */
import { isolateLtr } from "./bidi";
import { daysBetween } from "./plainDate";

/** An en dash, the range separator in both languages. */
export const RANGE_DASH = "–";

const clocks = new Map<string, Intl.DateTimeFormat>();
const dates = new Map<string, Intl.DateTimeFormat>();

function clock(timeZone: string): Intl.DateTimeFormat {
  let found = clocks.get(timeZone);
  if (found === undefined) {
    found = new Intl.DateTimeFormat("en-GB", { hour: "2-digit", minute: "2-digit", hourCycle: "h23", timeZone });
    clocks.set(timeZone, found);
  }
  return found;
}

function dateIn(timeZone: string): Intl.DateTimeFormat {
  let found = dates.get(timeZone);
  if (found === undefined) {
    found = new Intl.DateTimeFormat("en-CA", { year: "numeric", month: "2-digit", day: "2-digit", timeZone });
    dates.set(timeZone, found);
  }
  return found;
}

/** `instant` (an ISO instant or Date) as `HH:mm` in `timeZone`, isolated left to right. */
export function formatTime(instant: string | Date, timeZone: string): string {
  return isolateLtr(clockText(instant, timeZone));
}

function clockText(instant: string | Date, timeZone: string): string {
  const text = clock(timeZone).format(typeof instant === "string" ? new Date(instant) : instant);
  const match = /(\d{2}):(\d{2})/.exec(text);
  return match ? `${match[1]}:${match[2]}` : text;
}

/** The calendar date of `instant` in `timeZone`, `YYYY-MM-DD`. */
export function dateOf(instant: string | Date, timeZone: string): string {
  const text = dateIn(timeZone).format(typeof instant === "string" ? new Date(instant) : instant);
  const match = /(\d{4})-(\d{2})-(\d{2})/.exec(text);
  return match ? `${match[1]}-${match[2]}-${match[3]}` : text;
}

/** How many calendar days after the start's date the end falls, in `timeZone`: 0 the same day. */
export function dayOffset(start: string | Date, end: string | Date, timeZone: string): number {
  return daysBetween(dateOf(start, timeZone), dateOf(end, timeZone));
}

/** The span from `start` to `end` as `HH:mm–HH:mm`, with ` +1` when the end is on the next day. */
export function formatTimeRange(start: string | Date, end: string | Date, timeZone: string): string {
  const offset = dayOffset(start, end, timeZone);
  const suffix = offset > 0 ? ` +${offset}` : "";
  return isolateLtr(`${clockText(start, timeZone)}${RANGE_DASH}${clockText(end, timeZone)}${suffix}`);
}

/** Whole minutes from `start` to `end`. */
export function minutesBetween(start: string | Date, end: string | Date): number {
  const a = typeof start === "string" ? new Date(start) : start;
  const b = typeof end === "string" ? new Date(end) : end;
  return Math.round((b.getTime() - a.getTime()) / 60_000);
}

/** `minutes` split into hours and minutes, for a duration such as "8h 30m". */
export function durationParts(minutes: number): { hours: number; minutes: number } {
  const whole = Math.max(0, Math.round(minutes));
  return { hours: Math.floor(whole / 60), minutes: whole % 60 };
}

/**
 * Whether the hours between `start` and `end` differ from the clock-face hours because the clocks
 * changed in `timeZone` during the span (FR-053: a 7- or 9-hour night).
 */
export function crossesClockChange(start: string | Date, end: string | Date, timeZone: string): boolean {
  const a = typeof start === "string" ? new Date(start) : start;
  const b = typeof end === "string" ? new Date(end) : end;
  const offsetAt = (instant: Date): number => {
    const text = new Intl.DateTimeFormat("en-US", { timeZone, timeZoneName: "longOffset" }).format(instant);
    const match = /GMT([+-]\d{2}):?(\d{2})?/.exec(text);
    if (!match) return 0;
    const sign = match[1]!.startsWith("-") ? -1 : 1;
    return sign * (Math.abs(Number(match[1])) * 60 + Number(match[2] ?? 0));
  };
  return offsetAt(a) !== offsetAt(b);
}
