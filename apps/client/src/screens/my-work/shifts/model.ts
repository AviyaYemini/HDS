/**
 * My shifts' logic (specs/007-ux-redesign FR-021 to FR-023): the split into upcoming and past, the
 * groups, the next shift and its countdown, "+1" for an overnight shift, and the calendar file.
 */
import type { components } from "@hds/api-client";

import { addDays, dateOf, dayOffset, minutesBetween, startOfWeek } from "../../../locale";

export type Shift = components["schemas"]["MyShift"];
export type ShiftDetail = components["schemas"]["MyShiftDetail"];

/** The shift's local date at its site. */
export function shiftDate(shift: Pick<Shift, "starts_at" | "timezone">): string {
  return dateOf(shift.starts_at, shift.timezone);
}

/** Whether the shift ends on a later local day than it starts (FR-022's "+1"). */
export function endsNextDay(shift: Pick<Shift, "starts_at" | "ends_at" | "timezone">): boolean {
  return dayOffset(shift.starts_at, shift.ends_at, shift.timezone) > 0;
}

/** Shifts that have not ended yet, soonest first. */
export function upcoming(shifts: readonly Shift[], now: Date): Shift[] {
  return shifts.filter((shift) => new Date(shift.ends_at).getTime() > now.getTime()).sort((a, b) => a.starts_at.localeCompare(b.starts_at));
}

/** Shifts that have ended, latest first. */
export function past(shifts: readonly Shift[], now: Date): Shift[] {
  return shifts.filter((shift) => new Date(shift.ends_at).getTime() <= now.getTime()).sort((a, b) => b.starts_at.localeCompare(a.starts_at));
}

export interface WeekGroup {
  weekStart: string;
  items: Shift[];
}

/** Upcoming shifts by their week, Sunday first (FR-021). */
export function byWeek(shifts: readonly Shift[]): WeekGroup[] {
  const groups = new Map<string, Shift[]>();
  for (const shift of shifts) {
    const weekStart = startOfWeek(shiftDate(shift));
    groups.set(weekStart, [...(groups.get(weekStart) ?? []), shift]);
  }
  return [...groups.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([weekStart, items]) => ({ weekStart, items }));
}

export interface PeriodGroup {
  /** The pay period, a calendar month `YYYY-MM`. */
  period: string;
  items: Shift[];
  /** Whole minutes worked, summed exactly. */
  minutes: number;
}

/** Past shifts by pay period, latest first, with the minutes worked (FR-021). */
export function byPeriod(shifts: readonly Shift[]): PeriodGroup[] {
  const groups = new Map<string, Shift[]>();
  for (const shift of shifts) {
    const period = shiftDate(shift).slice(0, 7);
    groups.set(period, [...(groups.get(period) ?? []), shift]);
  }
  return [...groups.entries()]
    .sort(([a], [b]) => b.localeCompare(a))
    .map(([period, items]) => ({ period, items, minutes: items.reduce((sum, shift) => sum + minutesBetween(shift.starts_at, shift.ends_at), 0) }));
}

/** The next shift: the first upcoming one, or none. */
export function nextShift(shifts: readonly Shift[], now: Date): Shift | null {
  return upcoming(shifts, now)[0] ?? null;
}

/** How far off a shift is, in whole days and hours, or `started` once it has begun. */
export function countdown(shift: Pick<Shift, "starts_at">, now: Date): { days: number; hours: number } | "started" {
  const minutes = minutesBetween(now, shift.starts_at);
  if (minutes <= 0) return "started";
  return { days: Math.floor(minutes / 1440), hours: Math.floor((minutes % 1440) / 60) };
}

/** The range of dates Past loads: the last three pay periods up to yesterday. */
export function pastRange(today: string): { from: string; to: string } {
  const [year, month] = today.split("-").map(Number) as [number, number, number];
  const start = new Date(Date.UTC(year, month - 1 - 3, 1));
  return { from: start.toISOString().slice(0, 10), to: addDays(today, -1) };
}

/** Links that open the address in a maps app or site (FR-023). */
export function mapLinks(address: string): { google: string; apple: string; waze: string } {
  const query = encodeURIComponent(address);
  return {
    google: `https://www.google.com/maps/search/?api=1&query=${query}`,
    apple: `https://maps.apple.com/?q=${query}`,
    waze: `https://waze.com/ul?q=${query}`,
  };
}

function icsStamp(instant: string): string {
  return new Date(instant).toISOString().replace(/[-:]/g, "").replace(/\.\d{3}/, "");
}

function icsText(text: string): string {
  return text.replace(/\\/g, "\\\\").replace(/\n/g, "\\n").replace(/[,;]/g, (match) => `\\${match}`);
}

/** An iCalendar file with the one shift, for the web's "Add to calendar" (FR-023). */
export function icsFor(shift: ShiftDetail, title: string, now: Date = new Date()): string {
  const lines = [
    "BEGIN:VCALENDAR",
    "VERSION:2.0",
    "PRODID:-//HDS//Shifts//EN",
    "BEGIN:VEVENT",
    `UID:${shift.id}@hds`,
    `DTSTAMP:${icsStamp(now.toISOString())}`,
    `DTSTART:${icsStamp(shift.starts_at)}`,
    `DTEND:${icsStamp(shift.ends_at)}`,
    `SUMMARY:${icsText(title)}`,
    ...(shift.site_address ? [`LOCATION:${icsText(shift.site_address)}`] : []),
    `DESCRIPTION:${icsText(`${shift.project.name} · ${shift.site.name}`)}`,
    "END:VEVENT",
    "END:VCALENDAR",
  ];
  return `${lines.join("\r\n")}\r\n`;
}
