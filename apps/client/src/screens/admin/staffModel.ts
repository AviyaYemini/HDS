/**
 * Staff control's pure parts (specs/demo-ceo/plan.md, screen 3): a person's employment as the API
 * gives it (permanent when a field is missing, as in older fixtures), rostered hours, the
 * permanent/temporary filter, and the 3-month cap on a temporary period.
 */
import { addDays } from "../../locale";

export type EmploymentType = "permanent" | "temporary";
export type TypeFilter = "all" | EmploymentType;

/** The demo's employee fields (`EmployeeRead`, demo plan §1). */
export interface Employment {
  employment_type: EmploymentType;
  engaged_from: string | null;
  engaged_until: string | null;
  project_ids: string[];
}

/** A person's employment; permanent with no dates when the API does not say otherwise. */
export function employmentOf(person: object): Employment {
  const p = person as Partial<Employment>;
  return {
    employment_type: p.employment_type === "temporary" ? "temporary" : "permanent",
    engaged_from: p.engaged_from ?? null,
    engaged_until: p.engaged_until ?? null,
    project_ids: p.project_ids ?? [],
  };
}

/** Whether `person` passes the type filter. */
export function passesType(person: object, filter: TypeFilter): boolean {
  return filter === "all" || employmentOf(person).employment_type === filter;
}

/** The filter from the address (`?type=temporary`), all by default. */
export function typeFilterOf(value: string | undefined): TypeFilter {
  return value === "temporary" || value === "permanent" ? value : "all";
}

/**
 * Each person's scheduled hours over `shifts` (drafts and published), by their own times on each
 * assignment.
 */
export function scheduledHours(shifts: readonly { assignments: readonly { user_id: string; starts_at: string; ends_at: string }[] }[]): Map<string, number> {
  const out = new Map<string, number>();
  for (const s of shifts)
    for (const a of s.assignments) {
      const hours = (Date.parse(a.ends_at) - Date.parse(a.starts_at)) / 3_600_000;
      if (hours > 0) out.set(a.user_id, (out.get(a.user_id) ?? 0) + hours);
    }
  return out;
}

/** "2027-02-12" → "12.02", as the chip writes the end date. */
export function dayMonth(date: string): string {
  return `${date.slice(8, 10)}.${date.slice(5, 7)}`;
}

/**
 * The last day a temporary period starting on `from` may run to: three calendar months later,
 * on the same day of the month, or the month's last day when it has no such day (30 Nov → 28 Feb).
 */
export function maxEngagedUntil(from: string): string {
  const year = Number(from.slice(0, 4));
  const month = Number(from.slice(5, 7)) + 3;
  const day = Number(from.slice(8, 10));
  const y = year + Math.floor((month - 1) / 12);
  const m = ((month - 1) % 12) + 1;
  const firstOfNext = m === 12 ? `${y + 1}-01-01` : `${y}-${String(m + 1).padStart(2, "0")}-01`;
  const lastDay = Number(addDays(firstOfNext, -1).slice(8, 10));
  return `${y}-${String(m).padStart(2, "0")}-${String(Math.min(day, lastDay)).padStart(2, "0")}`;
}

/** Days left until `until`, from `today` (0 on the last day; negative once it passed). */
export function daysLeft(today: string, until: string): number {
  return Math.round((Date.parse(`${until}T00:00:00Z`) - Date.parse(`${today}T00:00:00Z`)) / 86_400_000);
}
