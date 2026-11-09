/**
 * The roster week's geometry (specs/007-ux-redesign T075, FR-026, FR-027): each person's bar
 * against the block (a half shift, extra hours), an overnight shift's two days, and coverage. The
 * bars are in percent of the block, so they mirror with the layout in Hebrew.
 */
import { barGeometry, cellOf, coverage, groupWarnings, isOvernight, placed, shiftDays, weekDays, type Shift } from "../../../src/screens/roster/model";

const ZONE = "Asia/Jerusalem";

function shift(id: string, starts: string, ends: string, extra: Partial<Shift> = {}): Shift {
  return {
    id,
    starts_at: starts,
    ends_at: ends,
    timezone: ZONE,
    shift_type_id: "morning",
    headcount: 3,
    assignments: [],
    status: "draft",
    etag: "e",
    pay_rate: null,
    project_id: "p",
    published_at: null,
    ...extra,
  } as Shift;
}

// Monday 7 December 2026, 06:00 to 14:00 in Israel (UTC+2).
const morning = shift("m", "2026-12-07T04:00:00Z", "2026-12-07T12:00:00Z");

test("a half shift is half as wide, and extra hours run past the block's edge", () => {
  expect(barGeometry(morning, { starts_at: "2026-12-07T04:00:00Z", ends_at: "2026-12-07T08:00:00Z" })).toEqual({ offset: 0, width: 50 });
  const late = barGeometry(morning, { starts_at: "2026-12-07T08:00:00Z", ends_at: "2026-12-07T14:00:00Z" });
  expect(late.offset).toBe(50);
  expect(late.width).toBe(75); // 10:00 to 16:00: two hours past the block's 14:00
  expect(late.offset + late.width).toBeGreaterThan(100);
});

test("a person on the shift's own times fills the block exactly", () => {
  expect(barGeometry(morning, { starts_at: morning.starts_at, ends_at: morning.ends_at })).toEqual({ offset: 0, width: 100 });
});

test("an overnight shift starts on one site date and continues on the next", () => {
  // Monday 22:00 to Tuesday 06:00 in Israel.
  const night = shift("n", "2026-12-07T20:00:00Z", "2026-12-08T04:00:00Z", { shift_type_id: "night" });
  expect(isOvernight(night)).toBe(true);
  expect(shiftDays(night)).toEqual({ start: "2026-12-07", end: "2026-12-08" });
  // An end exactly at midnight stays on the day it starts.
  const evening = shift("e", "2026-12-07T13:00:00Z", "2026-12-07T22:00:00Z");
  expect(isOvernight(evening)).toBe(false);
});

test("coverage counts filled against required, and placement groups by day and type", () => {
  const two = { ...morning, assignments: [{ id: "a1" }, { id: "a2" }] } as Shift;
  expect(coverage(two)).toEqual({ filled: 2, required: 3, open: 1 });
  const grouped = placed([two]);
  expect(grouped.get(cellOf("2026-12-07", "morning"))?.map((s) => s.id)).toEqual(["m"]);
  expect(weekDays("2026-12-06")).toHaveLength(7);
  expect(weekDays("2026-12-06")[6]).toBe("2026-12-12");
});

describe("warnings grouped by person and rule (owner, 2026-11-09)", () => {
  const w = (code: string, employee_id: string | null, values: Record<string, string> = {}) => ({ code, employee_id, values });

  test("the same rule for one person reads once, keeping each distinct set of numbers", () => {
    const groups = groupWarnings(
      [
        w("warnings.overlap", "ann"),
        w("warnings.overlap", "ann"),
        w("warnings.rest_too_short", "ann", { rest_hours: "0", minimum_hours: "11" }),
        w("warnings.rest_too_short", "ann", { rest_hours: "8", minimum_hours: "11" }),
        w("warnings.overlap", "ben"),
      ],
      (x) => x.employee_id,
    );

    expect(groups.map((g) => [g.first.employee_id, g.first.code, g.values.length])).toEqual([
      ["ann", "warnings.overlap", 1],
      ["ann", "warnings.rest_too_short", 2],
      ["ben", "warnings.overlap", 1],
    ]);
  });
});
