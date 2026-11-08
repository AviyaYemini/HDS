/** The device draft and the offline shift list (specs/007-ux-redesign FR-019). */
import AsyncStorage from "@react-native-async-storage/async-storage";

import { loadPending, savePending, withChange } from "../../../src/screens/my-work/availability/draftQueue";
import { loadCached, saveCached } from "../../../src/screens/my-work/shifts/shiftsCache";

beforeEach(async () => AsyncStorage.clear());

it("keeps one pending change per cell, the latest, in order", () => {
  const a = { key: { date: "2026-11-17", shiftType: "morning" }, state: "open" as const };
  const b = { key: { date: "2026-11-18", shiftType: "all-day" }, state: "blocked" as const };
  const a2 = { ...a, state: "close" as const };
  expect(withChange(withChange(withChange([], a), b), a2)).toEqual([b, a2]);
});

it("stores the pending changes per person and window and reads them back", async () => {
  const change = { key: { date: "2026-11-17", shiftType: "morning" }, state: "open" as const };
  await savePending("me", "w1", [change]);
  expect(await loadPending("me", "w1")).toEqual([change]);
  expect(await loadPending("me", "w2")).toEqual([]);
  await savePending("me", "w1", []);
  expect(await loadPending("me", "w1")).toEqual([]);
});

it("keeps the last shift list with the time it was loaded", async () => {
  const shift = { id: "s", starts_at: "2026-11-09T06:00:00Z", ends_at: "2026-11-09T14:00:00Z", timezone: "Asia/Jerusalem", project: { id: "p", name: "P" }, site: { id: "x", name: "X" }, shift_type: "morning" };
  await saveCached("me", "upcoming", [shift], new Date("2026-11-07T08:00:00Z"));
  expect(await loadCached("me", "upcoming")).toEqual({ items: [shift], updatedAt: "2026-11-07T08:00:00.000Z" });
  expect(await loadCached("me", "past")).toBeNull();
});
