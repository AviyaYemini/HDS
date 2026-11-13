/**
 * A finished run at a glance (the owner, 2026-11-13, "the deck treatment"): its scope in one line,
 * the people it places, how long it searched, and a proposal's slots laid out as the roster sheet,
 * projects as rows by client and days as columns. Pure, so each is tested on its own.
 */
import type { ReviewSlot } from "./review";

export interface ScopeProject {
  id: string;
  name: string;
  type: string;
}

/**
 * The run's scope: how many projects, of which type (one type, or mixed), and their names when there
 * are few enough to read in a line (two or fewer). No list means every active project.
 */
export function scopeOf(projectIds: readonly string[], projects: readonly ScopeProject[]): { all: boolean; count: number; type: "security" | "training" | "mixed"; names: string[] | null } {
  if (projectIds.length === 0) return { all: true, count: projects.length, type: "mixed", names: null };
  const chosen = projectIds.map((id) => projects.find((p) => p.id === id)).filter((p): p is ScopeProject => p !== undefined);
  const types = new Set(chosen.map((p) => p.type));
  const type = types.size === 1 ? ([...types][0] === "training" ? "training" : "security") : "mixed";
  // Names only when every chosen project is known; otherwise the count says it (CodeRabbit, PR 28).
  return { all: false, count: projectIds.length, type, names: projectIds.length <= 2 && chosen.length === projectIds.length ? chosen.map((p) => p.name) : null };
}

/** Seconds the run searched, to one decimal (at least 0.1). */
export function solveSeconds(job: { started_at: string | null; finished_at: string | null }): number | null {
  if (!job.started_at || !job.finished_at) return null;
  const seconds = (Date.parse(job.finished_at) - Date.parse(job.started_at)) / 1000;
  return Math.max(0.1, Math.round(seconds * 10) / 10);
}

/** How many different people the proposal places: anyone on a slot, not taken off it, and anyone it keeps. */
export function peoplePlaced(slots: readonly Pick<ReviewSlot, "marks" | "kept">[]): number {
  const placed = new Set<string>();
  for (const slot of slots) {
    for (const mark of slot.marks) if (mark.mark !== "removed") placed.add(mark.user_id);
    for (const id of slot.kept) placed.add(id);
  }
  return placed.size;
}

/** One project's row of a proposal week: its slots per day of `days`. */
export interface ProposalRow {
  projectId: string;
  name: string;
  cells: ReviewSlot[][];
}

/** The proposal's slots as the roster sheet: rows by project, grouped by client, both by name. */
export function proposalSheet(
  slots: readonly ReviewSlot[],
  days: readonly string[],
  projects: readonly { id: string; name: string; client_id: string }[],
  clients: readonly { id: string; name: string }[],
  locale = "he",
): { clientId: string; clientName: string; rows: ProposalRow[] }[] {
  const inWeek = slots.filter((s) => days.includes(s.date));
  const byProject = new Map<string, ReviewSlot[]>();
  for (const s of inWeek) byProject.set(s.project_id, [...(byProject.get(s.project_id) ?? []), s]);
  const groups = new Map<string, { clientId: string; clientName: string; rows: ProposalRow[] }>();
  for (const [projectId, own] of byProject) {
    const project = projects.find((p) => p.id === projectId);
    const clientId = project?.client_id ?? "";
    const group = groups.get(clientId) ?? { clientId, clientName: clients.find((c) => c.id === clientId)?.name ?? "", rows: [] };
    group.rows.push({ projectId, name: project?.name ?? "", cells: days.map((day) => own.filter((s) => s.date === day).sort((a, b) => a.starts_at.localeCompare(b.starts_at))) });
    groups.set(clientId, group);
  }
  const compare = (a: string, b: string) => a.localeCompare(b, locale);
  const out = [...groups.values()].sort((a, b) => compare(a.clientName, b.clientName));
  for (const g of out) g.rows.sort((a, b) => compare(a.name, b.name));
  return out;
}
