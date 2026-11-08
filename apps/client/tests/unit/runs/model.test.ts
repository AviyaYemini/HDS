/** Schedule runs' model (specs/007-ux-redesign FR-042, FR-074): the bar, the label, the count. */
import { filled, honesty, runProgress } from "../../../src/screens/runs/model";

const START = "2026-11-08T10:00:00Z";
const at = (seconds: number) => new Date(START).getTime() + seconds * 1000;
const running = { status: "running" as const, solver: "cpsat", started_at: START, time_limit_seconds: 20 };

test("the bar is elapsed time over the limit, rising only, and full once the run ends", () => {
  expect(runProgress(running, at(5))).toBe(0.25);
  expect(runProgress(running, at(40))).toBe(1);
  // A clock that steps back never moves the bar back.
  expect(runProgress(running, at(2), 0.25)).toBe(0.25);
  expect(runProgress({ ...running, status: "queued", started_at: null }, at(5))).toBe(0);
  // Ending early completes it at once.
  expect(runProgress({ ...running, status: "succeeded" }, at(3))).toBe(1);
  expect(runProgress({ ...running, status: "failed" }, at(3))).toBe(1);
});

test("a greedy run has no limit to measure against, so its bar is indeterminate", () => {
  expect(runProgress({ ...running, solver: "greedy" }, at(5))).toBeNull();
});

test("a proposal is labelled optimal, or the best found in the seconds it searched", () => {
  const done = { status: "succeeded" as const, started_at: START, finished_at: "2026-11-08T10:00:12.400Z" };
  expect(honesty({ ...done, optimal: true })).toEqual({ kind: "optimal" });
  expect(honesty({ ...done, optimal: false })).toEqual({ kind: "best", seconds: 12 });
  expect(honesty({ ...done, optimal: null, finished_at: "2026-11-08T10:00:00.100Z" })).toEqual({ kind: "best", seconds: 1 });
  expect(honesty({ ...done, status: "running", optimal: null })).toBeNull();
});

test("filled counts each slot's people up to its headcount", () => {
  const slot = (headcount: number, assigned: number) => ({ headcount, assigned: Array.from({ length: assigned }, (_, i) => `u${i}`) });
  expect(filled({ slots: [slot(2, 1), slot(1, 1), slot(1, 3)] as never })).toEqual({ filled: 3, required: 4 });
});
