/** The availability week's logic (specs/007-ux-redesign FR-013, FR-015, FR-017, task T060). */
import {
  ALL_DAY,
  absencesOn,
  cellState,
  copyPlan,
  editable,
  initialWeek,
  nextState,
  operationFor,
  pickWindow,
  reviewRows,
  statusAfterChange,
  weeksOf,
  type Entry,
} from "../../../src/screens/my-work/availability/model";

const entry = (over: Partial<Entry>): Entry => ({
  id: "e",
  user_id: "u",
  kind: "open",
  shift_type_id: null,
  starts_on: "2026-11-17",
  ends_on: "2026-11-17",
  weekday: null,
  absence_kind: null,
  created_at: "",
  updated_at: "",
  ...over,
});

describe("the tap cycle (FR-013)", () => {
  it("goes unmarked, open, close, blocked and round", () => {
    expect(nextState("unmarked")).toBe("open");
    expect(nextState("open")).toBe("close");
    expect(nextState("close")).toBe("blocked");
    expect(nextState("blocked")).toBe("unmarked");
  });
});

describe("what a cell shows", () => {
  const entries = [
    entry({ id: "a", kind: "open", shift_type_id: null, starts_on: "2026-11-16", ends_on: "2026-11-16" }),
    entry({ id: "b", kind: "blocked", shift_type_id: "night", starts_on: "2026-11-16", ends_on: "2026-11-16" }),
    entry({ id: "c", kind: "close", shift_type_id: "morning", starts_on: "2026-11-15", ends_on: "2026-11-21", weekday: 3 }),
    entry({ id: "d", kind: "absence", absence_kind: "sick", starts_on: "2026-11-19", ends_on: "2026-11-20" }),
  ];
  it("shows its own mark, else inherits the all-day one, else unmarked", () => {
    expect(cellState(entries, { date: "2026-11-16", shiftType: "night" })).toBe("blocked");
    expect(cellState(entries, { date: "2026-11-16", shiftType: "morning" })).toBe("open");
    expect(cellState(entries, { date: "2026-11-16", shiftType: ALL_DAY })).toBe("open");
    expect(cellState(entries, { date: "2026-11-17", shiftType: "morning" })).toBe("unmarked");
  });
  it("honours a weekday rule and keeps absences apart", () => {
    expect(cellState(entries, { date: "2026-11-18", shiftType: "morning" })).toBe("close");
    expect(cellState(entries, { date: "2026-11-19", shiftType: "morning" })).toBe("unmarked");
    expect(absencesOn(entries, "2026-11-20").map((e) => e.id)).toEqual(["d"]);
    expect(absencesOn(entries, "2026-11-21")).toEqual([]);
    expect(cellState(entries, { date: "2026-11-19", shiftType: ALL_DAY })).toBe("unmarked");
  });
});

describe("the operation a change needs", () => {
  it("creates, updates, deletes or does nothing", () => {
    const current = entry({ id: "x", kind: "open" });
    expect(operationFor(undefined, "open")).toEqual({ op: "create", kind: "open" });
    expect(operationFor(current, "close")).toEqual({ op: "update", id: "x", kind: "close" });
    expect(operationFor(current, "unmarked")).toEqual({ op: "delete", id: "x" });
    expect(operationFor(current, "open")).toEqual({ op: "none" });
    expect(operationFor(undefined, "unmarked")).toEqual({ op: "none" });
  });
});

describe("weeks and windows", () => {
  it("lists the Sundays a window touches and starts on today's week when it can", () => {
    expect(weeksOf({ starts_on: "2026-11-17", ends_on: "2026-11-30" })).toEqual(["2026-11-15", "2026-11-22", "2026-11-29"]);
    expect(initialWeek({ starts_on: "2026-11-15", ends_on: "2026-11-28" }, "2026-11-25")).toBe("2026-11-22");
    expect(initialWeek({ starts_on: "2026-11-15", ends_on: "2026-11-28" }, "2026-11-07")).toBe("2026-11-15");
  });
  it("picks the window asked for, else the first open one, else the latest", () => {
    const w = (id: string, starts_on: string, is_open: boolean) => ({ id, starts_on, ends_on: starts_on, deadline: "", is_open });
    const windows = [w("late", "2026-12-06", false), w("open2", "2026-11-22", true), w("open1", "2026-11-15", true)];
    expect(pickWindow(windows, "late")?.id).toBe("late");
    expect(pickWindow(windows, undefined)?.id).toBe("open1");
    expect(pickWindow(windows, "nope")?.id).toBe("open1");
    expect(pickWindow([w("a", "2026-10-01", false), w("b", "2026-11-01", false)], undefined)?.id).toBe("b");
    expect(pickWindow([], undefined)).toBeNull();
  });
});

describe("review and Copy last week", () => {
  const window = { starts_on: "2026-11-15", ends_on: "2026-11-21" };
  it("lists one row per day with a mark (FR-015)", () => {
    const entries = [
      entry({ id: "a", kind: "open", shift_type_id: "morning", starts_on: "2026-11-17", ends_on: "2026-11-17" }),
      entry({ id: "b", kind: "open", shift_type_id: null, starts_on: "2026-11-16", ends_on: "2026-11-16" }),
      entry({ id: "c", kind: "absence", absence_kind: "leave", starts_on: "2026-11-18", ends_on: "2026-11-18" }),
      entry({ id: "d", kind: "blocked", shift_type_id: "night", starts_on: "2026-11-30", ends_on: "2026-11-30" }),
    ];
    const rows = reviewRows(entries, window, ["morning", "night"]);
    expect(rows.map((row) => row.date)).toEqual(["2026-11-16", "2026-11-17"]);
    expect(rows[0]!.marks).toEqual([{ shiftType: ALL_DAY, state: "open" }]);
    expect(rows[1]!.marks).toEqual([{ shiftType: "morning", state: "open" }]);
  });
  it("copies last week's marks into the cells still unmarked, once each (FR-017)", () => {
    const entries = [
      entry({ id: "a", kind: "open", shift_type_id: null, starts_on: "2026-11-09", ends_on: "2026-11-09" }),
      entry({ id: "b", kind: "blocked", shift_type_id: "night", starts_on: "2026-11-10", ends_on: "2026-11-10" }),
      entry({ id: "c", kind: "close", shift_type_id: "night", starts_on: "2026-11-17", ends_on: "2026-11-17" }),
    ];
    expect(copyPlan(entries, "2026-11-15", ["morning", "night"])).toEqual([{ key: { date: "2026-11-16", shiftType: ALL_DAY }, state: "open" }]);
  });
});

describe("editable and the banner", () => {
  it("allows changes only in an open window that is not locked", () => {
    expect(editable({ status: "not_submitted" }, { is_open: true })).toBe(true);
    expect(editable({ status: "submitted" }, { is_open: true })).toBe(true);
    expect(editable({ status: "locked" }, { is_open: true })).toBe(false);
    expect(editable(null, { is_open: false })).toBe(false);
    expect(editable(null, null)).toBe(false);
  });
  it("turns a change into a draft, unless locked", () => {
    expect(statusAfterChange("submitted")).toBe("draft");
    expect(statusAfterChange("not_submitted")).toBe("draft");
    expect(statusAfterChange("locked")).toBe("locked");
  });
});
