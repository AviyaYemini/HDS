/** My shifts' logic (specs/007-ux-redesign FR-021 to FR-023). */
import { byPeriod, byWeek, countdown, endsNextDay, icsFor, mapLinks, nextShift, past, pastRange, upcoming, type Shift } from "../../../src/screens/my-work/shifts/model";

const shift = (id: string, starts_at: string, ends_at: string): Shift => ({
  id,
  starts_at,
  ends_at,
  timezone: "Asia/Jerusalem",
  project: { id: "p", name: "Night Watch" },
  site: { id: "s", name: "Gate 3" },
  shift_type: "night",
});
const now = new Date("2026-11-07T08:00:00Z");
const shifts = [
  shift("past", "2026-11-03T06:00:00Z", "2026-11-03T14:00:00Z"),
  shift("older", "2026-10-28T06:00:00Z", "2026-10-28T14:00:00Z"),
  shift("night", "2026-11-11T20:00:00Z", "2026-11-12T04:00:00Z"),
  shift("soon", "2026-11-09T06:00:00Z", "2026-11-09T14:00:00Z"),
  shift("later", "2026-11-17T06:00:00Z", "2026-11-17T14:00:00Z"),
];

it("splits upcoming from past and orders each (FR-021)", () => {
  expect(upcoming(shifts, now).map((s) => s.id)).toEqual(["soon", "night", "later"]);
  expect(past(shifts, now).map((s) => s.id)).toEqual(["past", "older"]);
});

it("groups upcoming by week and past by pay period with the minutes", () => {
  expect(byWeek(upcoming(shifts, now)).map((g) => [g.weekStart, g.items.map((s) => s.id)])).toEqual([
    ["2026-11-08", ["soon", "night"]],
    ["2026-11-15", ["later"]],
  ]);
  expect(byPeriod(past(shifts, now)).map((g) => [g.period, g.items.length, g.minutes])).toEqual([
    ["2026-11", 1, 480],
    ["2026-10", 1, 480],
  ]);
});

it("marks an overnight shift and counts down to the next one (FR-022)", () => {
  expect(endsNextDay(shifts[2]!)).toBe(true);
  expect(endsNextDay(shifts[3]!)).toBe(false);
  expect(nextShift(shifts, now)?.id).toBe("soon");
  expect(countdown(shifts[3]!, now)).toEqual({ days: 1, hours: 22 });
  expect(countdown(shifts[0]!, now)).toBe("started");
});

it("loads three periods of the past and links to maps", () => {
  expect(pastRange("2026-11-07")).toEqual({ from: "2026-08-01", to: "2026-11-06" });
  expect(mapLinks("Rothschild 1, Tel Aviv").google).toContain(encodeURIComponent("Rothschild 1, Tel Aviv"));
});

it("writes a calendar file for the shift (FR-023)", () => {
  const ics = icsFor({ ...shifts[2]!, site_address: "Rothschild 1, Tel Aviv", contacts: [] }, "Night shift, Gate 3", new Date("2026-11-07T08:00:00Z"));
  expect(ics).toContain("BEGIN:VEVENT");
  expect(ics).toContain("DTSTART:20261111T200000Z");
  expect(ics).toContain("DTEND:20261112T040000Z");
  expect(ics).toContain("SUMMARY:Night shift\\, Gate 3");
  expect(ics).toContain("LOCATION:Rothschild 1\\, Tel Aviv");
  expect(ics.endsWith("END:VCALENDAR\r\n")).toBe(true);
});
