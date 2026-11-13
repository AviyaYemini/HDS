/**
 * The snapshot dashboard's data (specs/demo-ceo/plan.md, "API changes" §2): the generated shape of
 * `GET /api/v1/reports/overview`, and the pure parts the screen needs: the period's range and how a project's status reads. Money is
 * an `Amount` and hours are `HoursOut`, strings with two decimals; the client never adds them up.
 */
import type { components } from "@hds/api-client";

import { addDays, isolateLtr } from "../../locale";

/** The answer of `GET /api/v1/reports/overview`; the money fields are absent where pay is hidden. */
export type Overview = components["schemas"]["OverviewRead"];
export type OverviewProject = components["schemas"]["OverviewProjectRead"];
export type OverviewWeek = components["schemas"]["OverviewWeekRead"];
export type ProjectHealth = OverviewProject["status"];

export type Period = "month" | "quarter" | "year";

/** The period holding `today`: its first and last dates. */
export function periodRange(period: Period, today: string): { from: string; to: string } {
  const year = Number(today.slice(0, 4));
  const month = Number(today.slice(5, 7));
  const first = (y: number, m: number) => `${y}-${String(m).padStart(2, "0")}-01`;
  const lastOf = (y: number, m: number) => addDays(m === 12 ? first(y + 1, 1) : first(y, m + 1), -1);
  if (period === "month") return { from: first(year, month), to: lastOf(year, month) };
  if (period === "quarter") {
    const start = Math.floor((month - 1) / 3) * 3 + 1;
    return { from: first(year, start), to: lastOf(year, start + 2) };
  }
  return { from: first(year, 1), to: lastOf(year, 12) };
}

/** This week's coverage as a whole percent, or null when nothing is needed. */
export function coveragePercent(coverage: Overview["coverage"]): number | null {
  if (coverage.needed <= 0) return null;
  return Math.round((Math.min(coverage.filled, coverage.needed) / coverage.needed) * 100);
}

/** A "12.50" percent as the table shows it: "12.5%", a loss with a true minus sign. */
export function formatPercent(value: string | null | undefined): string {
  if (value == null) return "—";
  const n = Number(value);
  const text = new Intl.NumberFormat("en-GB", { maximumFractionDigits: 1 }).format(Math.abs(n));
  // Isolated left to right, so a loss's minus stays in front of the number inside Hebrew.
  return isolateLtr(`${n < 0 ? "−" : ""}${text}%`);
}

/** The chip a project's status shows. */
export function healthChip(status: ProjectHealth): "success" | "warning" | "danger" | "neutral" {
  return status === "ok" ? "success" : status === "gaps" ? "warning" : status === "loss" ? "danger" : "neutral";
}

/** "1.11": a week's label, day and month. */
export function weekLabel(weekStart: string): string {
  return `${Number(weekStart.slice(8))}.${Number(weekStart.slice(5, 7))}`;
}
