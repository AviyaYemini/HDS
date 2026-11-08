/** Schedule runs' model (specs/007-ux-redesign FR-042, FR-074): the bar, the label, the count. */
import { elapsedNow, filled, honesty, peopleOf, projectsOf, runProgress, slotsByDay, weeksOf } from "../../../src/screens/runs/model";

const START = "2026-11-08T10:00:00Z";
const running = { status: "running" as const, solver: "cpsat", time_limit_seconds: 20 };

test("the bar is elapsed time over the limit, rising only, and full once the run ends", () => {
  expect(runProgress(running, 5)).toBe(0.25);
  expect(runProgress(running, 40)).toBe(1);
  // An earlier reading never moves the bar back.
  expect(runProgress(running, 2, 0.25)).toBe(0.25);
  expect(runProgress({ ...running, status: "queued" }, null)).toBe(0);
  // Ending early completes it at once.
  expect(runProgress({ ...running, status: "succeeded" }, 3)).toBe(1);
  expect(runProgress({ ...running, status: "failed" }, 3)).toBe(1);
});

test("a greedy run, or one without a limit, has nothing to measure against: indeterminate", () => {
  expect(runProgress({ ...running, solver: "greedy" }, 5)).toBeNull();
  expect(runProgress({ ...running, time_limit_seconds: 0 }, 5)).toBeNull();
});

test("elapsed is the server's reading plus the time since it, whatever the device's clock says", () => {
  const readAt = 1_000_000;
  expect(elapsedNow({ status: "running", elapsed_seconds: 4 }, readAt, readAt + 1500)).toBe(5.5);
  // A device clock far off changes nothing: only the time since the answer counts.
  expect(elapsedNow({ status: "running", elapsed_seconds: 4 }, readAt - 3_600_000, readAt - 3_600_000 + 1000)).toBe(5);
  expect(elapsedNow({ status: "queued", elapsed_seconds: null }, readAt, readAt + 1000)).toBeNull();
  expect(elapsedNow({ status: "succeeded", elapsed_seconds: 7 }, readAt, readAt + 9000)).toBe(7);
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

test("a run's weeks start on Sundays and cover its whole range", () => {
  const weeks = weeksOf("2026-11-10", "2026-11-17");
  expect(weeks.map((w) => w[0])).toEqual(["2026-11-08", "2026-11-15"]);
  expect(weeks[0]).toHaveLength(7);
});

test("a slot's people say what the run does to each against the drafts", () => {
  const changes = { added: [{ key: "k1", user_id: "new" }], removed: [{ key: "k1", user_id: "gone" }, { key: "k2", user_id: "x" }] };
  expect(peopleOf({ key: "k1", assigned: ["kept", "new"] }, changes)).toEqual([
    { userId: "kept", change: "kept" },
    { userId: "new", change: "added" },
    { userId: "gone", change: "removed" },
  ]);
});

test("slots sort by start within their day, and an older run's projects come from its slots", () => {
  const slot = (key: string, date: string, starts_at: string, project_id = "p") => ({ key, date, starts_at, project_id }) as never;
  const byDay = slotsByDay({ slots: [slot("b", "2026-11-08", "2026-11-08T12:00:00Z"), slot("a", "2026-11-08", "2026-11-08T05:00:00Z")] });
  expect(byDay.get("2026-11-08")!.map((s: { key: string }) => s.key)).toEqual(["a", "b"]);
  expect(projectsOf({ project_ids: [], proposal: { slots: [slot("a", "d", "t", "p1"), slot("b", "d", "t", "p1")] } as never })).toEqual(["p1"]);
  expect(projectsOf({ project_ids: ["p2"], proposal: null })).toEqual(["p2"]);
});
