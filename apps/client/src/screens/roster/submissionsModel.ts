/**
 * The submission tracker's arithmetic (the owner, 2026-11-13): how many submitted of how many, the
 * share, the days left to the deadline, and the rows in the order a manager acts on them: not
 * submitted first, then drafts, then the submitted, each by name; a search narrows them.
 */
export type Status = "not_submitted" | "draft" | "submitted" | "locked";

export interface Row {
  user_id: string;
  full_name: string;
  status: Status;
}

/** Submitted (or locked after submitting) of everyone listed, and the share in whole percent. */
export function submittedOf(rows: readonly Pick<Row, "status">[]): { submitted: number; drafts: number; total: number; pct: number } {
  const submitted = rows.filter((r) => r.status === "submitted" || r.status === "locked").length;
  const drafts = rows.filter((r) => r.status === "draft").length;
  const total = rows.length;
  return { submitted, drafts, total, pct: total > 0 ? Math.round((submitted / total) * 100) : 0 };
}

const ORDER: Record<Status, number> = { not_submitted: 0, draft: 1, submitted: 2, locked: 3 };

/** The rows matching `query` (any part of the name, case and accents aside), not submitted first. */
export function orderedRows<T extends Row>(rows: readonly T[], query: string, locale = "he"): T[] {
  const wanted = query.trim().toLocaleLowerCase(locale);
  return rows
    .filter((r) => wanted === "" || r.full_name.toLocaleLowerCase(locale).includes(wanted))
    .sort((a, b) => ORDER[a.status] - ORDER[b.status] || a.full_name.localeCompare(b.full_name, locale));
}

/** Whole days from `today` to `deadlineDate` (both site dates); 0 on the day, negative once past. */
export function daysLeft(today: string, deadlineDate: string): number {
  return Math.round((Date.parse(`${deadlineDate}T00:00:00Z`) - Date.parse(`${today}T00:00:00Z`)) / 86_400_000);
}
