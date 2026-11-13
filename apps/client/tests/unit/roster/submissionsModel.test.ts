/** The submission tracker's arithmetic (the owner, 2026-11-13). Made-up names only. */
import { daysLeft, orderedRows, submittedOf } from "../../../src/screens/roster/submissionsModel";

const rows = [
  { user_id: "1", full_name: "Noa Peretz", status: "submitted" as const },
  { user_id: "2", full_name: "Omer Katz", status: "not_submitted" as const },
  { user_id: "3", full_name: "Dana Levi", status: "draft" as const },
  { user_id: "4", full_name: "Ami Cohen", status: "not_submitted" as const },
  { user_id: "5", full_name: "Tal Shani", status: "locked" as const },
];

test("submitted of the total, with drafts and the share", () => {
  expect(submittedOf(rows)).toEqual({ submitted: 2, drafts: 1, total: 5, pct: 40 });
  expect(submittedOf([])).toEqual({ submitted: 0, drafts: 0, total: 0, pct: 0 });
});

test("not submitted first, then drafts, then the submitted; a search narrows by name", () => {
  expect(orderedRows(rows, "", "en").map((r) => r.full_name)).toEqual(["Ami Cohen", "Omer Katz", "Dana Levi", "Noa Peretz", "Tal Shani"]);
  expect(orderedRows(rows, "  KATZ", "en").map((r) => r.user_id)).toEqual(["2"]);
});

test("days left to the deadline", () => {
  expect(daysLeft("2026-11-13", "2026-11-18")).toBe(5);
  expect(daysLeft("2026-11-18", "2026-11-18")).toBe(0);
  expect(daysLeft("2026-11-19", "2026-11-18")).toBe(-1);
});
