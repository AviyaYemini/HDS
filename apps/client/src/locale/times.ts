/**
 * Clock times and spans as people read them (specs/007-ux-redesign FR-053, D5; O13).
 *
 * Times are 24-hour `HH:mm` in the site's zone. A span whose end falls on a later calendar day
 * carries "+1" (or "+2"). Every time or span is isolated left to right, so it reads the same inside
 * Hebrew (D8). The clock icon that goes with a time is never mirrored (FR-057); that is the icon's
 * business, not this module's.
 */
import { isolateLtr } from "./bidi";
import { calendarDateIn, daysBetween } from "./plainDate";

/** An en dash, the range separator in both languages. */
export const RANGE_DASH = "–";

const clocks = new Map<string, Intl.DateTimeFormat>();

// Every formatter here reads its parts, never a locale's layout: the Intl polyfill on Hermes
// carries only the app's two languages (research R13).
function clock(timeZone: string): Intl.DateTimeFormat {
  let found = clocks.get(timeZone);
  if (found === undefined) {
    found = new Intl.DateTimeFormat("en", {
      timeZone,
      year: "numeric",
      month: "numeric",
      day: "numeric",
      hour: "numeric",
      minute: "numeric",
      second: "numeric",
      hourCycle: "h23",
    });
    clocks.set(timeZone, found);
  }
  return found;
}

function wallClock(instant: string | Date, timeZone: string): Record<string, number> {
  const out: Record<string, number> = {};
  for (const part of clock(timeZone).formatToParts(typeof instant === "string" ? new Date(instant) : instant)) {
    if (part.type !== "literal") out[part.type] = Number(part.value);
  }
  // Some engines give 24 for midnight under h23 as well.
  if (out.hour === 24) out.hour = 0;
  return out;
}

/** `instant` (an ISO instant or Date) as `HH:mm` in `timeZone`, isolated left to right. */
export function formatTime(instant: string | Date, timeZone: string): string {
  return isolateLtr(clockText(instant, timeZone));
}

function clockText(instant: string | Date, timeZone: string): string {
  const { hour, minute } = wallClock(instant, timeZone);
  return `${String(hour ?? 0).padStart(2, "0")}:${String(minute ?? 0).padStart(2, "0")}`;
}

/** The calendar date of `instant` in `timeZone`, `YYYY-MM-DD`. */
export function dateOf(instant: string | Date, timeZone: string): string {
  return calendarDateIn(typeof instant === "string" ? new Date(instant) : instant, timeZone);
}

/** Minutes the zone is ahead of UTC at `instant` (negative when behind). */
export function zoneOffsetMinutes(instant: string | Date, timeZone: string): number {
  const date = typeof instant === "string" ? new Date(instant) : instant;
  const wall = wallClock(date, timeZone);
  const asUtc = Date.UTC(wall.year!, wall.month! - 1, wall.day!, wall.hour!, wall.minute!, wall.second ?? 0);
  return Math.round((asUtc - date.getTime() + (date.getTime() % 1000)) / 60_000);
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
  return zoneOffsetMinutes(start, timeZone) !== zoneOffsetMinutes(end, timeZone);
}
