/**
 * A proposal under review, its pure parts (specs/008-auto-scheduler-review FR-001 to FR-012,
 * T016): slots by day, open pairs, the days and projects still open, a changed mark's two pairs,
 * what a person's menu may offer, and the breaks an edit was refused for.
 */
import { breaksOf, hasOpen, hours, menuFor, numbersOf, openDaysAndProjects, openMarks, pairsOf, partOfPairs, reasonKey, slotsByDay, type Review, type ReviewSlot } from "../../../src/screens/runs/review";

const A = "00000000-0000-0000-0000-00000000000a";
const B = "00000000-0000-0000-0000-00000000000b";

function slot(key: string, date: string, project: string, marks: ReviewSlot["marks"], start = "04:00"): ReviewSlot {
  return {
    key,
    shift_id: null,
    project_id: project,
    shift_type_id: "t",
    date,
    starts_at: `${date}T${start}:00Z`,
    ends_at: `${date}T12:00:00Z`,
    headcount: 1,
    kept: [],
    people: marks.filter((m) => m.mark !== "removed").map((m) => ({ user_id: m.user_id, by: "solver", locked: false, override: null })),
    places: 0,
    marks,
    part_accepted: marks.some((m) => m.mark !== "unchanged" && m.accepted),
  };
}

const review: Pick<Review, "slots"> = {
  slots: [
    slot("late", "2027-06-06", "p1", [{ mark: "added", user_id: A, replaces: null, accepted: false }], "14:00"),
    slot("early", "2027-06-06", "p1", [{ mark: "changed", user_id: A, replaces: B, accepted: false }]),
    slot("done", "2027-06-07", "p2", [{ mark: "added", user_id: B, replaces: null, accepted: true }]),
    slot("same", "2027-06-08", "p2", [{ mark: "unchanged", user_id: B, replaces: null, accepted: true }]),
  ],
};

test("a day lists its slots by start time", () => {
  expect(slotsByDay(review).get("2027-06-06")!.map((s) => s.key)).toEqual(["early", "late"]);
});

test("only marks with something to accept, not yet accepted, are open", () => {
  expect(openMarks(review.slots[2]!)).toEqual([]);
  expect(openMarks(review.slots[3]!)).toEqual([]);
  expect(openMarks(review.slots[1]!)).toHaveLength(1);
  expect(hasOpen(review)).toBe(true);
  expect(openDaysAndProjects(review)).toEqual({ days: ["2027-06-06"], projects: ["p1"] });
});

test("a changed mark accepts both the person added and the person replaced", () => {
  const early = review.slots[1]!;
  expect(pairsOf(early, early.marks[0]!)).toEqual([
    { key: "early", userId: A },
    { key: "early", userId: B },
  ]);
  expect(partOfPairs(pairsOf(early, early.marks[0]!))).toEqual({
    assignments: [
      { key: "early", user_id: A },
      { key: "early", user_id: B },
    ],
  });
});

test("a slot with a part accepted is not edited in the proposal, nor offered as a target", () => {
  expect(menuFor(review, review.slots[2]!).canEdit).toBe(false);
  expect(menuFor(review, review.slots[0]!).targets.map((s) => s.key)).toEqual(["early", "same"]);
});

test("the breaks an edit was refused for come from the problem's errors", () => {
  const problem = { code: "schedule.override_reason_required", errors: [{ key: "late", user_id: A, code: "warnings.blocked", values: {} }, { nonsense: true }] };
  expect(breaksOf(problem)).toEqual([{ key: "late", user_id: A, code: "warnings.blocked", values: {}, excused: false }]);
  expect(breaksOf({ code: "x" })).toEqual([]);
  expect(breaksOf(null)).toEqual([]);
});

describe("a rule's numbers in words (008 FR-013)", () => {
  test("rest gives the rest they would have and the rest needed, in hours", () => {
    expect(numbersOf({ code: "schedule.minimum_rest", have: 540, need: 660 }, "en")).toEqual({ key: "runs.numbers.rest", values: { have: "9", need: "11" } });
  });

  test("the weekly maximum gives the week's hours, the slot's and the cap", () => {
    expect(numbersOf({ code: "schedule.weekly_maximum", week_minutes: 2880, slot_minutes: 450, limit: 2880 }, "en")).toEqual({
      key: "runs.numbers.weekly",
      values: { worked: "48", more: "7.5", limit: "48" },
    });
  });

  test("other rules, or missing numbers, give none", () => {
    expect(numbersOf({ code: "schedule.overlap" }, "en")).toBeNull();
    expect(numbersOf({ code: "schedule.minimum_rest", have: null, need: 660 }, "en")).toBeNull();
  });

  test("hours keep one decimal place at most", () => {
    expect(hours(500, "en")).toBe("8.3");
    expect(hours(0, "en")).toBe("0");
  });

  test("a code this build does not know reads as another rule", () => {
    const known = new Set(["runs.reasons.overlap"]);
    expect(reasonKey("schedule.overlap", (k) => known.has(k))).toBe("runs.reasons.overlap");
    expect(reasonKey("schedule.new_rule", (k) => known.has(k))).toBe("runs.reasons.other");
  });
});
