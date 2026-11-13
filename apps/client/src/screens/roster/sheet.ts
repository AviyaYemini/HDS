/**
 * The roster week in the sheet's layout (specs/demo-ceo/plan.md, screen 2; slide 7): projects as
 * rows grouped by client, the seven days as columns, and in each cell the day's shifts with their
 * people and empty places. Pure, so the grouping and the counts are tested on their own.
 */
import { shiftDays, type Assignment, type Shift } from "./model";

/** What the sheet needs of a project and a client. */
export interface SheetProject {
  id: string;
  name: string;
  client_id: string;
  status?: string;
  /** The task's managers, named (GET /projects gives them to staff readers). */
  managers?: readonly { id: string; full_name: string }[];
}

/** "מנהל משימה" as the sheet shows it: the managers' names, or a dash. */
export function managersText(project: Pick<SheetProject, "managers">): string {
  const names = (project.managers ?? []).map((m) => m.full_name).filter(Boolean);
  return names.length > 0 ? names.join(", ") : "—";
}
export interface SheetClient {
  id: string;
  name: string;
}

/** One project's row: its shifts per day, Sunday first. */
export interface SheetRow {
  project: SheetProject;
  cells: Shift[][];
}

export interface ClientGroup {
  clientId: string;
  clientName: string;
  rows: SheetRow[];
}

/** The week's counts, for the line under the title. */
export interface SheetTotals {
  places: number;
  filled: number;
  empty: number;
  temporary: number;
  published: number;
  shifts: number;
}

/** A temporary worker's assignment (`AssignmentRead.temporary`, demo plan §1; filled on the shifts list). */
export function isTemporary(assignment: Pick<Assignment, "temporary">): boolean {
  return assignment.temporary === true;
}

/** "Dana Levi" → "Dana L.", as the managers' sheet writes names; one word stays whole. */
export function shortName(full: string): string {
  const parts = full.trim().split(/\s+/).filter(Boolean);
  if (parts.length < 2) return parts[0] ?? "";
  return `${parts[0]} ${parts[parts.length - 1]![0]}.`;
}

/**
 * Projects grouped by client, both by name. An active project shows even with no shifts this
 * week (an empty row is news); an ended one only when it has shifts.
 */
export function buildSheet(projects: readonly SheetProject[], clients: readonly SheetClient[], shifts: readonly Shift[], days: readonly string[], locale = "he"): ClientGroup[] {
  const byProject = new Map<string, Shift[]>();
  for (const s of shifts) byProject.set(s.project_id, [...(byProject.get(s.project_id) ?? []), s]);
  const clientName = new Map(clients.map((c) => [c.id, c.name]));
  const compare = (a: string, b: string) => a.localeCompare(b, locale);
  const groups = new Map<string, ClientGroup>();
  for (const project of projects) {
    const own = byProject.get(project.id) ?? [];
    if (project.status === "ended" && own.length === 0) continue;
    const cells = days.map((day) => own.filter((s) => shiftDays(s).start === day).sort((a, b) => a.starts_at.localeCompare(b.starts_at)));
    const group = groups.get(project.client_id) ?? { clientId: project.client_id, clientName: clientName.get(project.client_id) ?? "", rows: [] };
    group.rows.push({ project, cells });
    groups.set(project.client_id, group);
  }
  const out = [...groups.values()].sort((a, b) => compare(a.clientName, b.clientName));
  for (const g of out) g.rows.sort((a, b) => compare(a.project.name, b.project.name));
  return out;
}

/** Places, filled and empty over the shifts shown; an over-filled shift never hides another's gap. */
export function sheetTotals(groups: readonly ClientGroup[]): SheetTotals {
  const totals: SheetTotals = { places: 0, filled: 0, empty: 0, temporary: 0, published: 0, shifts: 0 };
  const temporary = new Set<string>();
  for (const g of groups)
    for (const r of g.rows)
      for (const cell of r.cells)
        for (const s of cell) {
          totals.shifts += 1;
          if (s.status === "published") totals.published += 1;
          totals.places += s.headcount;
          totals.filled += Math.min(s.headcount, s.assignments.length);
          totals.empty += Math.max(0, s.headcount - s.assignments.length);
          for (const a of s.assignments) if (isTemporary(a)) temporary.add(a.user_id);
        }
  totals.temporary = temporary.size;
  return totals;
}

/** A row's empty places this week. */
export function rowEmpty(row: SheetRow): number {
  return row.cells.flat().reduce((n, s) => n + Math.max(0, s.headcount - s.assignments.length), 0);
}

/** Only the projects with shifts this week, and how many were left out (the sheet's default view). */
export function withShifts(groups: readonly ClientGroup[]): { groups: ClientGroup[]; hidden: number } {
  let hidden = 0;
  const out: ClientGroup[] = [];
  for (const g of groups) {
    const rows = g.rows.filter((r) => r.cells.some((c) => c.length > 0));
    hidden += g.rows.length - rows.length;
    if (rows.length > 0) out.push({ ...g, rows });
  }
  return { groups: out, hidden };
}
