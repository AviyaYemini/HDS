/**
 * The roster sheet's grouping and counts (specs/demo-ceo/plan.md, screen 2): projects by client,
 * shifts by day, empty places that an over-filled shift never hides, temporary workers counted
 * once, and names written the way the managers' sheet writes them. Made-up names only.
 */
import type { Assignment, Shift } from "../../../src/screens/roster/model";
import { buildSheet, isTemporary, keptWarnings, rowEmpty, sheetTotals, shortName } from "../../../src/screens/roster/sheet";
import { weekDays } from "../../../src/screens/roster/model";

const ZONE = "Asia/Jerusalem";
const WEEK = weekDays("2026-12-06");

function person(user: string, extra: Partial<Assignment> & { temporary?: boolean } = {}): Assignment {
  return { id: `a-${user}-${Math.random()}`, user_id: user, full_name: null, starts_at: "", ends_at: "", locked: false, overrides: [], own_times: false, shift_id: "s", ...extra } as Assignment;
}

function shift(id: string, project: string, startsUtc: string, headcount: number, people: Assignment[], status: "draft" | "published" = "draft"): Shift {
  const end = new Date(new Date(startsUtc).getTime() + 8 * 3600_000).toISOString();
  return { id, project_id: project, starts_at: startsUtc, ends_at: end, timezone: ZONE, headcount, assignments: people, status, etag: "e", pay_rate: null, published_at: null, shift_type_id: "t" } as Shift;
}

const clients = [
  { id: "c2", name: "Zeta Guarding" },
  { id: "c1", name: "Alpha Security" },
];
const projects = [
  { id: "p1", name: "Harbour gate", client_id: "c1", status: "active" },
  { id: "p2", name: "City museum", client_id: "c2", status: "active" },
  { id: "p3", name: "Airport fence", client_id: "c1", status: "active" },
  { id: "p4", name: "Old mall", client_id: "c2", status: "ended" },
];

test("projects group under their client, both sorted by name; an ended project without shifts is left out", () => {
  const groups = buildSheet(projects, clients, [], WEEK, "en");
  expect(groups.map((g) => g.clientName)).toEqual(["Alpha Security", "Zeta Guarding"]);
  expect(groups[0]!.rows.map((r) => r.project.name)).toEqual(["Airport fence", "Harbour gate"]);
  expect(groups[1]!.rows.map((r) => r.project.id)).toEqual(["p2"]);
});

test("a shift sits in the site day it starts on, with the day's shifts by start time", () => {
  // Monday 7 December, 23:00 and 07:00 in Israel (UTC+2): both on Monday, the morning first.
  const night = shift("n", "p1", "2026-12-07T21:00:00Z", 1, []);
  const morning = shift("m", "p1", "2026-12-07T05:00:00Z", 2, []);
  const [group] = buildSheet([projects[0]!], clients, [night, morning], WEEK, "en");
  const row = group!.rows[0]!;
  expect(row.cells[1]!.map((s) => s.id)).toEqual(["m", "n"]);
  expect(row.cells.filter((c) => c.length > 0)).toHaveLength(1);
});

test("an ended project still shows when it has shifts this week", () => {
  const groups = buildSheet(projects, clients, [shift("x", "p4", "2026-12-08T05:00:00Z", 1, [])], WEEK, "en");
  expect(groups.flatMap((g) => g.rows.map((r) => r.project.id))).toContain("p4");
});

test("totals: an over-filled shift never hides another's gap; temporary workers count once", () => {
  const over = shift("o", "p1", "2026-12-06T05:00:00Z", 1, [person("u1"), person("u2", { temporary: true })], "published");
  const short = shift("s", "p1", "2026-12-07T05:00:00Z", 3, [person("u2", { temporary: true })]);
  const groups = buildSheet([projects[0]!], clients, [over, short], WEEK, "en");
  expect(sheetTotals(groups)).toEqual({ places: 4, filled: 2, empty: 2, temporary: 1, published: 1, shifts: 2 });
  expect(rowEmpty(groups[0]!.rows[0]!)).toBe(2);
});

test("temporary is read from the assignment and false when the field is missing", () => {
  expect(isTemporary(person("u", { temporary: true }))).toBe(true);
  expect(isTemporary(person("u"))).toBe(false);
});

test("names as the sheet writes them", () => {
  expect(shortName("Dana Levi")).toBe("Dana L.");
  expect(shortName("  Noa  Ben David ")).toBe("Noa D.");
  expect(shortName("Omer")).toBe("Omer");
  expect(shortName("")).toBe("");
});

test("kept warnings: one line per person, rule and shift", () => {
  const override = { warning_code: "rest_too_short", reason: "phone ok", created_at: "", created_by: "" };
  const s = shift("k", "p1", "2026-12-07T05:00:00Z", 1, [person("u1", { overrides: [override, override] })]);
  expect(keptWarnings([s]).map((w) => [w.userId, w.code, w.reason])).toEqual([["u1", "rest_too_short", "phone ok"]]);
});
