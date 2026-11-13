/**
 * A finished run at a glance (the owner, 2026-11-13): the scope in a line, the solve time, the
 * people placed, and the proposal laid out as the roster sheet. Made-up names only.
 */
import { peoplePlaced, proposalSheet, scopeOf, solveSeconds } from "../../../src/screens/runs/runSummary";
import type { ReviewSlot } from "../../../src/screens/runs/review";

const projects = [
  { id: "p1", name: "Harbour gate", type: "security", client_id: "c1" },
  { id: "p2", name: "City museum", type: "security", client_id: "c2" },
  { id: "p3", name: "Course 7", type: "training", client_id: "c1" },
];
const clients = [
  { id: "c1", name: "Alpha Security" },
  { id: "c2", name: "Beta Guarding" },
];

test("the scope: names when two or fewer, else a count by type; none means every project", () => {
  expect(scopeOf(["p1"], projects)).toEqual({ all: false, count: 1, type: "security", names: ["Harbour gate"] });
  expect(scopeOf(["p1", "p2", "p3"], projects)).toEqual({ all: false, count: 3, type: "mixed", names: null });
  expect(scopeOf(["p1", "p2"], projects)).toEqual({ all: false, count: 2, type: "security", names: ["Harbour gate", "City museum"] });
  expect(scopeOf([], projects).all).toBe(true);
});

test("solve time to a tenth, and the people placed", () => {
  expect(solveSeconds({ started_at: "2026-11-13T10:00:00.000Z", finished_at: "2026-11-13T10:00:02.340Z" })).toBe(2.3);
  expect(solveSeconds({ started_at: "2026-11-13T10:00:00.000Z", finished_at: "2026-11-13T10:00:00.010Z" })).toBe(0.1);
  expect(solveSeconds({ started_at: null, finished_at: null })).toBeNull();
  const marks = (...m: [string, string][]) => ({ marks: m.map(([user_id, mark]) => ({ user_id, mark, accepted: false, replaces: null })) }) as unknown as Pick<ReviewSlot, "marks">;
  expect(peoplePlaced([marks(["u1", "added"], ["u2", "removed"]), marks(["u1", "unchanged"], ["u3", "changed"])])).toBe(2);
});

test("the proposal as the sheet: rows by project under their client, a slot in its day", () => {
  const slot = (key: string, project: string, date: string, starts = `${date}T05:00:00Z`) => ({ key, project_id: project, date, starts_at: starts }) as unknown as ReviewSlot;
  const days = ["2026-11-15", "2026-11-16", "2026-11-17", "2026-11-18", "2026-11-19", "2026-11-20", "2026-11-21"];
  const sheet = proposalSheet([slot("a", "p2", "2026-11-16"), slot("b", "p1", "2026-11-15"), slot("c", "p1", "2026-11-15", "2026-11-15T03:00:00Z"), slot("x", "p3", "2026-11-25")], days, projects, clients, "en");
  expect(sheet.map((g) => g.clientName)).toEqual(["Alpha Security", "Beta Guarding"]);
  expect(sheet[0]!.rows.map((r) => r.name)).toEqual(["Harbour gate"]);
  expect(sheet[0]!.rows[0]!.cells[0]!.map((s) => s.key)).toEqual(["c", "b"]);
  expect(sheet[1]!.rows[0]!.cells[1]!.map((s) => s.key)).toEqual(["a"]);
});
