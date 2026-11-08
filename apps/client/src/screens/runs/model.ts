/**
 * Schedule runs' pure parts (specs/007-ux-redesign FR-042, FR-074, T079): how far the bar is, how a
 * proposal is labelled, and how many places it fills. The solver streams no interim result, so the
 * bar is elapsed time against the time limit ("up to N s"); "best so far" comes with feature 008.
 */
import type { components } from "@hds/api-client";

import { addDays, startOfWeek } from "../../locale";

export type Job = components["schemas"]["JobRead"];
export type JobSummary = components["schemas"]["JobSummary"];

/** Whether the run is still going. */
export function isActive(job: Pick<JobSummary, "status">): boolean {
  return job.status === "queued" || job.status === "running";
}

/** A status's chip tone, in the list and on the run's page. */
export const STATUS_TONE = { queued: "neutral", running: "info", succeeded: "success", failed: "danger", cancelled: "neutral" } as const;

/**
 * Seconds searched by the server's clock, `now` milliseconds after the epoch: the job's
 * `elapsed_seconds` as read at `readAt`, plus the time since while it runs. The device's clock only
 * measures the time since the answer, so a clock that is off never moves the bar.
 */
export function elapsedNow(
  job: Pick<JobSummary, "status" | "elapsed_seconds">,
  readAt: number,
  now: number,
): number | null {
  if (job.elapsed_seconds === null) return null;
  return job.elapsed_seconds + (isActive(job) ? Math.max(0, now - readAt) / 1000 : 0);
}

/**
 * The bar's value, 0 to 1: `elapsed` seconds over the limit, never below `floor` (the last value
 * shown, so it only rises), and 1 at once when the run has ended. Null (the indeterminate bar) for a
 * greedy run or one without a limit, which have nothing to measure against.
 */
export function runProgress(
  job: Pick<JobSummary, "status" | "solver" | "time_limit_seconds">,
  elapsed: number | null,
  floor = 0,
): number | null {
  if (job.solver === "greedy" || job.time_limit_seconds <= 0) return null;
  if (!isActive(job)) return 1;
  if (elapsed === null) return floor;
  return Math.max(floor, Math.min(1, elapsed / job.time_limit_seconds));
}

/** The seconds a finished run searched, rounded, at least 1. */
export function searchedSeconds(job: Pick<JobSummary, "started_at" | "finished_at">): number | null {
  if (job.started_at === null || job.finished_at === null) return null;
  const seconds = (new Date(job.finished_at).getTime() - new Date(job.started_at).getTime()) / 1000;
  return Math.max(1, Math.round(seconds));
}

/** How good the proposal is: proven optimal, or the best found in N seconds (FR-042). */
export function honesty(
  job: Pick<JobSummary, "status" | "optimal" | "started_at" | "finished_at">,
): { kind: "optimal" } | { kind: "best"; seconds: number } | null {
  if (job.status !== "succeeded") return null;
  if (job.optimal === true) return { kind: "optimal" };
  return { kind: "best", seconds: searchedSeconds(job) ?? 0 };
}

/** Places filled over places needed across the proposal's slots. */
export function filled(proposal: Pick<NonNullable<Job["proposal"]>, "slots">): { filled: number; required: number } {
  let done = 0;
  let required = 0;
  for (const slot of proposal.slots) {
    required += slot.headcount;
    done += Math.min(slot.headcount, slot.assigned.length);
  }
  return { filled: done, required };
}

export type Proposal = NonNullable<Job["proposal"]>;

/** Project, shift type and people's names, to read a proposal by. */
export interface Names {
  projects: Map<string, string>;
  types: Map<string, string>;
  people: Map<string, string>;
}

/** Every Sunday-started week from `first` to `last`, as their seven dates. */
export function weeksOf(first: string, last: string): string[][] {
  const weeks: string[][] = [];
  for (let start = startOfWeek(first); start <= last; start = addDays(start, 7)) {
    weeks.push(Array.from({ length: 7 }, (_, i) => addDays(start, i)));
  }
  return weeks;
}

/** The projects a run covers: its stored list, or (an older run) those its slots name. */
export function projectsOf(job: Pick<Job, "project_ids" | "proposal">): string[] {
  if (job.project_ids.length > 0) return job.project_ids;
  return [...new Set((job.proposal?.slots ?? []).map((slot) => slot.project_id))];
}

/** The projects a run covers, by name; an older run without a list covered every active one. */
export function coveredText(projectIds: readonly string[], names: Map<string, string>, everyProject: string): string {
  if (projectIds.length === 0) return everyProject;
  return projectIds.map((id) => names.get(id) ?? "").filter(Boolean).join(", ");
}

/** A short, stable digest of `text` (FNV-1a), for an idempotency key that follows the request. */
export function digest(text: string): string {
  let hash = 0x811c9dc5;
  for (let i = 0; i < text.length; i += 1) {
    hash ^= text.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash.toString(36);
}

/** The project types among `projects`, in a fixed order (security, then training). */
export function typesOf(projects: readonly { type: string }[]): string[] {
  return [...new Set(projects.map((p) => p.type))].sort();
}
