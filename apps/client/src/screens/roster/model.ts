/**
 * The roster week's pure parts (specs/007-ux-redesign FR-025 to FR-041, T070, T075): which day and
 * row a shift sits in, an overnight shift's continuation, each person's bar against the block, and
 * coverage. Times are instants from the API; days are the site's local dates.
 */
import type { components } from "@hds/api-client";

import { addDays, dateOf } from "../../locale";

export type Shift = components["schemas"]["ShiftRead"];
export type Assignment = components["schemas"]["AssignmentRead"];
export type Warning = components["schemas"]["WarningRead"];

/** The seven site dates of the week starting on Sunday `week`. */
export function weekDays(week: string): string[] {
  return Array.from({ length: 7 }, (_, i) => addDays(week, i));
}

/** The site date a shift starts on, and the one it ends on. */
export function shiftDays(shift: Pick<Shift, "starts_at" | "ends_at" | "timezone">): { start: string; end: string } {
  // An end exactly at midnight belongs to the day before.
  const end = dateOf(new Date(new Date(shift.ends_at).getTime() - 1).toISOString(), shift.timezone);
  return { start: dateOf(shift.starts_at, shift.timezone), end };
}

/** Whether the shift runs past its first day (drawn with a continuation on the next). */
export function isOvernight(shift: Pick<Shift, "starts_at" | "ends_at" | "timezone">): boolean {
  const { start, end } = shiftDays(shift);
  return end > start;
}

/**
 * A person's bar inside a block, in percent of the block's own length: where it starts and how long
 * it runs. A half shift is narrower than the block; extra hours run past its edge (above 100).
 */
export function barGeometry(
  shift: Pick<Shift, "starts_at" | "ends_at">,
  assignment: Pick<Assignment, "starts_at" | "ends_at">,
): { offset: number; width: number } {
  const start = new Date(shift.starts_at).getTime();
  const length = new Date(shift.ends_at).getTime() - start;
  if (length <= 0) return { offset: 0, width: 100 };
  const own = new Date(assignment.starts_at).getTime();
  const ownEnd = new Date(assignment.ends_at).getTime();
  return { offset: ((own - start) / length) * 100, width: ((ownEnd - own) / length) * 100 };
}

/** Filled over required, as the block shows it ("2/3"). */
export function coverage(shift: Pick<Shift, "assignments" | "headcount">): { filled: number; required: number; open: number } {
  const filled = shift.assignments.length;
  return { filled, required: shift.headcount, open: Math.max(0, shift.headcount - filled) };
}

/** The shifts of each day and shift type, by start time. */
export function placed(shifts: readonly Shift[]): Map<string, Shift[]> {
  const out = new Map<string, Shift[]>();
  for (const shift of [...shifts].sort((a, b) => a.starts_at.localeCompare(b.starts_at))) {
    const key = cellOf(shiftDays(shift).start, shift.shift_type_id);
    out.set(key, [...(out.get(key) ?? []), shift]);
  }
  return out;
}

/** The key of one day's row for a shift type. */
export function cellOf(day: string, shiftTypeId: string): string {
  return `${day}|${shiftTypeId}`;
}

/** One step the roster's Undo can take back (FR-037). */
export type RosterChange =
  | { kind: "assigned"; assignmentId: string; shiftId: string; userId: string }
  // A transfer or a swap (008 T042f): undone by moving the assignment back, swapping again.
  | { kind: "moved"; assignmentId: string; shiftId: string; fromShiftId: string; swapWith: string | null }
  | { kind: "unassigned"; shiftId: string; userId: string; starts_at: string | null; ends_at: string | null };

/** One rule a person breaks, with every distinct set of numbers it was reported with. */
export interface WarningGroup<T> {
  first: T;
  values: Record<string, string>[];
}

/**
 * Warnings grouped by person and rule (owner, 2026-11-09): the rules report one warning per
 * clashing pair of shifts, so someone on two overlapping shifts would read the same words twice,
 * each with its own "Keep despite this", though one override covers the rule. Each group keeps the
 * numbers that differ (a rest of 0 h and one of 8 h), each once, in the order first seen.
 */
export function groupWarnings<T extends { code: string; values?: Record<string, string> | null }>(
  warnings: readonly T[],
  person: (w: T) => string | null,
): WarningGroup<T>[] {
  const groups = new Map<string, WarningGroup<T>>();
  for (const w of warnings) {
    const key = `${person(w) ?? ""}|${w.code}`;
    const values = w.values ?? {};
    const group = groups.get(key);
    if (!group) {
      groups.set(key, { first: w, values: [values] });
    } else if (!group.values.some((v) => JSON.stringify(v) === JSON.stringify(values))) {
      group.values.push(values);
    }
  }
  return [...groups.values()];
}
