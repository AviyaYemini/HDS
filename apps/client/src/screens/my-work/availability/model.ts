/**
 * The availability week's logic (specs/007-ux-redesign FR-013 to FR-020), kept apart from the
 * screen so the cycle, the grid's reading of the entries, the review and Copy last week can be
 * tested on their own. Dates are `YYYY-MM-DD` at the site; nothing here touches the clock.
 */
import type { components } from "@hds/api-client";

import { addDays, endOfWeek, startOfWeek, weekday } from "../../../locale";

export type Entry = components["schemas"]["EntryRead"];
export type Window = components["schemas"]["WindowRead"];
export type Submission = components["schemas"]["SubmissionRead"];
export type ShiftType = components["schemas"]["ShiftTypeRead"];

/** What a cell shows; a tap moves to the next one, round and round (FR-013). */
export type CellState = "unmarked" | "open" | "close" | "blocked";
export const CYCLE: readonly CellState[] = ["unmarked", "open", "close", "blocked"];

export function nextState(state: CellState): CellState {
  return CYCLE[(CYCLE.indexOf(state) + 1) % CYCLE.length]!;
}

/** The all-day column's key in the grid. */
export const ALL_DAY = "all-day";

/** A cell's address: a date and a shift type's id, or the all-day column. */
export interface CellKey {
  date: string;
  shiftType: string;
}

/** Whether `entry` applies on `date`: within its dates, and on its weekday when it has one. */
export function appliesOn(entry: Pick<Entry, "starts_on" | "ends_on" | "weekday">, date: string): boolean {
  if (date < entry.starts_on || date > entry.ends_on) return false;
  return entry.weekday === null || entry.weekday === weekday(date);
}

/** The entry a cell shows: its own shift's, else the day's all-day entry, which covers every shift. */
export function cellEntry(entries: readonly Entry[], key: CellKey): Entry | undefined {
  const marks = entries.filter((entry) => entry.kind !== "absence" && appliesOn(entry, key.date));
  if (key.shiftType !== ALL_DAY) {
    const own = marks.find((entry) => entry.shift_type_id === key.shiftType);
    if (own) return own;
  }
  return marks.find((entry) => entry.shift_type_id === null);
}

export function cellState(entries: readonly Entry[], key: CellKey): CellState {
  const entry = cellEntry(entries, key);
  return entry === undefined ? "unmarked" : (entry.kind as CellState);
}

/** The absence entries covering `date` (FR-020). */
export function absencesOn(entries: readonly Entry[], date: string): Entry[] {
  return entries.filter((entry) => entry.kind === "absence" && appliesOn(entry, date));
}

/** The Sunday-to-Saturday weeks that touch `window`, each as its Sunday (FR-013). */
export function weeksOf(window: Pick<Window, "starts_on" | "ends_on">): string[] {
  const weeks: string[] = [];
  const last = endOfWeek(window.ends_on);
  for (let sunday = startOfWeek(window.starts_on); sunday <= last; sunday = addDays(sunday, 7)) weeks.push(sunday);
  return weeks;
}

/** The seven dates of the week that starts on `sunday`. */
export function daysOfWeek(sunday: string): string[] {
  return Array.from({ length: 7 }, (_, index) => addDays(sunday, index));
}

/** The week to open first: the one holding `today` when the window does, else the window's first. */
export function initialWeek(window: Pick<Window, "starts_on" | "ends_on">, today: string): string {
  const weeks = weeksOf(window);
  return weeks.find((sunday) => sunday <= today && today <= addDays(sunday, 6)) ?? weeks[0]!;
}

/** The window to open: the one asked for, else the first open one, else the latest. */
export function pickWindow(windows: readonly Window[], asked: string | undefined): Window | null {
  if (windows.length === 0) return null;
  const wanted = asked ? windows.find((window) => window.id === asked) : undefined;
  if (wanted) return wanted;
  const open = windows.filter((window) => window.is_open).sort((a, b) => a.starts_on.localeCompare(b.starts_on));
  if (open.length > 0) return open[0]!;
  return [...windows].sort((a, b) => b.starts_on.localeCompare(a.starts_on))[0]!;
}

/** A state the API stores as an entry: every one but unmarked. */
export type Mark = Exclude<CellState, "unmarked">;

/** What to send to get a cell to `to` from what is there. */
export type Operation = { op: "create"; kind: Mark } | { op: "update"; id: string; kind: Mark } | { op: "delete"; id: string } | { op: "none" };

export function operationFor(current: Entry | undefined, to: CellState): Operation {
  if (to === "unmarked") return current ? { op: "delete", id: current.id } : { op: "none" };
  if (current === undefined) return { op: "create", kind: to };
  if (current.kind === to) return { op: "none" };
  return { op: "update", id: current.id, kind: to };
}

/**
 * The cell a change really lands on: a shift cell that only inherits the day's all-day mark changes
 * that all-day entry, which covers it (FR-018: Undo then puts the all-day mark back, R55).
 */
export function targetKey(entries: readonly Entry[], key: CellKey): CellKey {
  const current = cellEntry(entries, key);
  return current && current.shift_type_id === null && key.shiftType !== ALL_DAY ? { date: key.date, shiftType: ALL_DAY } : key;
}

/** One API call of a change plan. */
export type PlannedCall =
  | { call: "create"; kind: Mark; starts_on: string; ends_on: string; shift_type_id: string | null; weekday: number | null }
  | { call: "update"; id: string; kind?: Mark; starts_on?: string; ends_on?: string }
  | { call: "delete"; id: string };

/** Whether any date from `first` to `last` falls on `weekday` (null: any date does). */
function hasDay(first: string, last: string, day: number | null): boolean {
  if (first > last) return false;
  if (day === null) return true;
  for (let date = first; date <= last; date = addDays(date, 1)) if (weekday(date) === day) return true;
  return false;
}

/**
 * The calls that put one cell into `to` and touch no other date or shift (R48). A one-day entry of
 * the cell's own is updated or deleted. An entry that covers more dates (several days, or a weekday
 * rule) is shortened to the dates before the cell's, the dates after it are added back as an entry
 * of their own inside `window`, and the cell gets a one-day entry of its own.
 */
export function planCellChange(entries: readonly Entry[], key: CellKey, to: CellState, window: Pick<Window, "starts_on" | "ends_on">): PlannedCall[] {
  const target = targetKey(entries, key);
  const current = cellEntry(entries, target);
  const shiftTypeId = target.shiftType === ALL_DAY ? null : target.shiftType;
  const own = (kind: Mark): PlannedCall => ({ call: "create", kind, starts_on: target.date, ends_on: target.date, shift_type_id: shiftTypeId, weekday: null });
  if (current === undefined) return to === "unmarked" ? [] : [own(to)];
  const oneDay = current.starts_on === current.ends_on && current.weekday === null;
  if (oneDay) {
    if (to === "unmarked") return [{ call: "delete", id: current.id }];
    return current.kind === to ? [] : [{ call: "update", id: current.id, kind: to }];
  }
  const calls: PlannedCall[] = [];
  const before = { first: current.starts_on, last: addDays(target.date, -1) };
  const after = { first: addDays(target.date, 1), last: current.ends_on };
  const keepBefore = hasDay(before.first, before.last, current.weekday);
  // The dates after the cell come back as their own entry, only inside the window (R48).
  const afterFirst = after.first < window.starts_on ? window.starts_on : after.first;
  const afterLast = after.last > window.ends_on ? window.ends_on : after.last;
  const keepAfter = hasDay(afterFirst, afterLast, current.weekday);
  if (keepBefore) calls.push({ call: "update", id: current.id, ends_on: before.last });
  else if (keepAfter) calls.push({ call: "update", id: current.id, starts_on: afterFirst, ends_on: afterLast });
  else calls.push({ call: "delete", id: current.id });
  if (keepBefore && keepAfter) {
    calls.push({ call: "create", kind: current.kind as Mark, starts_on: afterFirst, ends_on: afterLast, shift_type_id: current.shift_type_id, weekday: current.weekday });
  }
  if (to !== "unmarked") calls.push(own(to));
  return calls;
}

/** One change a person made, kept so it can be undone (FR-018). */
export interface Change {
  key: CellKey;
  before: CellState;
  after: CellState;
}

/** A row of the review (FR-015): a day and its marks, days without a mark left out. */
export interface ReviewRow {
  date: string;
  marks: { shiftType: string; state: CellState }[];
}

export function reviewRows(entries: readonly Entry[], window: Pick<Window, "starts_on" | "ends_on">, shiftTypes: readonly string[]): ReviewRow[] {
  const rows: ReviewRow[] = [];
  for (let date = window.starts_on; date <= window.ends_on; date = addDays(date, 1)) {
    const marks: ReviewRow["marks"] = [];
    const allDay = cellEntry(entries, { date, shiftType: ALL_DAY });
    if (allDay) marks.push({ shiftType: ALL_DAY, state: allDay.kind as CellState });
    for (const shiftType of shiftTypes) {
      const own = entries.find((entry) => entry.kind !== "absence" && entry.shift_type_id === shiftType && appliesOn(entry, date));
      if (own) marks.push({ shiftType, state: own.kind as CellState });
    }
    if (marks.length > 0) rows.push({ date, marks });
  }
  return rows;
}

/** Copy last week (FR-017): the marks of each day a week earlier, for the cells still unmarked. */
export function copyPlan(entries: readonly Entry[], sunday: string, shiftTypes: readonly string[]): { key: CellKey; state: CellState }[] {
  const plan: { key: CellKey; state: CellState }[] = [];
  for (const date of daysOfWeek(sunday)) {
    const source = addDays(date, -7);
    for (const shiftType of [ALL_DAY, ...shiftTypes]) {
      const from = cellEntry(entries, { date: source, shiftType });
      if (!from) continue;
      // A shift cell that only inherits the all-day mark is copied once, through the all-day cell.
      if (shiftType !== ALL_DAY && from.shift_type_id === null) continue;
      const target: CellKey = { date, shiftType };
      if (cellEntry(entries, target) !== undefined) continue;
      plan.push({ key: target, state: from.kind as CellState });
    }
  }
  return plan;
}

/** Whether the person may still change the week: before the deadline, and the window open. */
export function editable(submission: Pick<Submission, "status"> | null, window: Pick<Window, "is_open"> | null): boolean {
  if (window === null) return false;
  if (submission?.status === "locked") return false;
  return window.is_open;
}

/** The status the banner shows right after a change, before the server has answered. */
export function statusAfterChange(status: Submission["status"]): Submission["status"] {
  return status === "locked" ? "locked" : "draft";
}
