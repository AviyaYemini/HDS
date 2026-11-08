/**
 * The date-range presets every range picker offers (specs/007-ux-redesign US5, FR-051; D1, D4).
 *
 * Each preset is a pure function of "today" in the site's zone, with Sunday weeks. Reports add the
 * pay-period presets; the pay period is the calendar month (005).
 */
import { addDays, addMonths, endOfMonth, endOfWeek, startOfMonth, startOfWeek, compareDates } from "./plainDate";

/** The presets of User Story 5, plus the two pay-period presets reports add. */
export const RANGE_PRESETS = [
  "today",
  "yesterday",
  "this-week",
  "last-week",
  "last-7-days",
  "last-30-days",
  "this-month",
  "last-month",
] as const;
export const PAY_PERIOD_PRESETS = ["this-pay-period", "last-pay-period"] as const;
/** Ranges that start today or later, for what is planned ahead (a schedule run, FR-042). */
export const FORWARD_PRESETS = ["next-week", "next-7-days", "next-30-days"] as const;

export type RangePreset =
  | (typeof RANGE_PRESETS)[number]
  | (typeof PAY_PERIOD_PRESETS)[number]
  | (typeof FORWARD_PRESETS)[number];
/** A preset key, or `custom` when the dates were set by hand. */
export type PresetKey = RangePreset | "custom";

/** A closed range of ISO dates. */
export interface DateRange {
  start: string;
  end: string;
}

/** The range `preset` stands for on `today`. */
export function presetRange(preset: RangePreset, today: string): DateRange {
  switch (preset) {
    case "today":
      return { start: today, end: today };
    case "yesterday": {
      const day = addDays(today, -1);
      return { start: day, end: day };
    }
    case "this-week":
      return { start: startOfWeek(today), end: endOfWeek(today) };
    case "last-week": {
      const start = addDays(startOfWeek(today), -7);
      return { start, end: addDays(start, 6) };
    }
    case "last-7-days":
      return { start: addDays(today, -6), end: today };
    case "last-30-days":
      return { start: addDays(today, -29), end: today };
    case "this-month":
    case "this-pay-period":
      return { start: startOfMonth(today), end: endOfMonth(today) };
    case "next-week": {
      const start = addDays(startOfWeek(today), 7);
      return { start, end: addDays(start, 6) };
    }
    case "next-7-days":
      return { start: today, end: addDays(today, 6) };
    case "next-30-days":
      return { start: today, end: addDays(today, 29) };
    case "last-month":
    case "last-pay-period": {
      const start = addMonths(today, -1);
      return { start, end: endOfMonth(start) };
    }
  }
}

/** The preset among `candidates` that stands for exactly `range` on `today`, else `custom`. */
export function presetFor(
  range: DateRange,
  today: string,
  candidates: readonly RangePreset[] = RANGE_PRESETS,
): PresetKey {
  for (const preset of candidates) {
    const expected = presetRange(preset, today);
    if (compareDates(expected.start, range.start) === 0 && compareDates(expected.end, range.end) === 0) return preset;
  }
  return "custom";
}
