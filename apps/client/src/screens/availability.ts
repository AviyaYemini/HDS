/** What an employee may change on My availability, as the API decides it (FR-105, FR-109). */
import type { components } from "@hds/api-client";

import { nextDay } from "../time";

export type Entry = components["schemas"]["EntryRead"];
export type Window = components["schemas"]["WindowRead"];
export type Kind = Entry["kind"];
export type AbsenceKind = NonNullable<Entry["absence_kind"]>;

export const KINDS: Kind[] = ["blocked", "open", "close", "absence"];
export const ABSENCE_KINDS: AbsenceKind[] = ["leave", "reserve_duty", "sick"];

/** Whether every date from `first` to `last` lies in one of the open `windows`. */
export function inOpenWindows(first: string, last: string, windows: Window[]): boolean {
  const open = windows.filter((w) => w.is_open);
  let day = first;
  while (day <= last) {
    const holding = open.find((w) => w.starts_on <= day && day <= w.ends_on);
    if (holding === undefined) return false;
    day = nextDay(holding.ends_on);
  }
  return true;
}

/** Whether the employee may still change `entry`: an absence any time, others in an open window. */
export function canChange(entry: Pick<Entry, "kind" | "starts_on" | "ends_on">, windows: Window[]): boolean {
  return entry.kind === "absence" || inOpenWindows(entry.starts_on, entry.ends_on, windows);
}

/** The kinds an employee may add: all while a window is open, else only an absence. */
export function kindsOffered(windows: Window[]): Kind[] {
  return windows.some((w) => w.is_open) ? KINDS : ["absence"];
}
