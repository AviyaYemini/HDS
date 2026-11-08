/**
 * A proposal under review, its pure parts (specs/008-auto-scheduler-review FR-001 to FR-012,
 * T016): slots by day, open pairs, the days and projects still open, a changed mark's two pairs,
 * what a person's menu may offer, and the breaks an edit was refused for.
 */
import { breaksOf, hasOpen, menuFor, openDaysAndProjects, openMarks, pairsOf, partOfPairs, slotsByDay, type Review, type ReviewSlot } from "../../../src/screens/runs/review";

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
