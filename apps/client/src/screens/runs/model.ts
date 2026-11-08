/**
 * Schedule runs' pure parts (specs/007-ux-redesign FR-042, FR-074, T079): how far the bar is, how a
 * proposal is labelled, and how many places it fills. The solver streams no interim result, so the
 * bar is elapsed time against the time limit ("up to N s"); "best so far" comes with feature 008.
 */
import type { components } from "@hds/api-client";

export type Job = components["schemas"]["JobRead"];
export type JobSummary = components["schemas"]["JobSummary"];

/** Whether the run is still going. */
export function isActive(job: Pick<JobSummary, "status">): boolean {
  return job.status === "queued" || job.status === "running";
}

/**
 * The bar's value, 0 to 1, `now` milliseconds after the epoch: elapsed over the limit, never below
 * `floor` (the last value shown, so it only rises), and 1 at once when the run has ended. Null (the
 * indeterminate bar) for a greedy run, which has no limit to measure against.
 */
export function runProgress(
  job: Pick<JobSummary, "status" | "solver" | "started_at" | "time_limit_seconds">,
  now: number,
  floor = 0,
): number | null {
  if (job.solver === "greedy") return null;
  if (!isActive(job)) return 1;
  if (job.started_at === null || job.time_limit_seconds <= 0) return floor;
  const elapsed = (now - new Date(job.started_at).getTime()) / 1000;
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
