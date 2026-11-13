/**
 * Staff control (specs/demo-ceo/plan.md, screen 3): this week's and this month's hours from the
 * hours report by employee, the near-the-weekly-limit flag, the temporary chip with its end date and
 * the permanent/temporary filter from the address; plus the 3-month cap. Made-up names only.
 */
import { screen } from "@testing-library/react-native";
import * as Router from "expo-router";

import type { Me } from "../../../src/api/session";
import { setupI18n } from "../../../src/i18n";
import { StaffScreen } from "../../../src/screens/admin";
import { daysLeft, dayMonth, employmentOf, maxEngagedUntil, nearWeeklyLimit, passesType, scheduledHours, typeFilterOf } from "../../../src/screens/admin/staffModel";
import { FakeApi, ME, json, renderWith, signedIn } from "../fakeApi";

const router = Router as unknown as { __setSearchParams: (p: Record<string, string>) => void };

const ADMIN: Me = { ...(ME as Me), role: "admin", capabilities: ["staff.manage", "staff.delete", "reports.read", "account.self"], managed_project_ids: [] };
const base = { organisation_id: "o", role: "employee", is_active: true, phone: null, default_hourly_rate: null, created_at: "2026-01-01T00:00:00Z", updated_at: "2026-01-01T00:00:00Z" };
const DANA = { ...base, id: "u1", full_name: "Dana Levi", email: "dana@hds.test" };
const OMER = { ...base, id: "u2", full_name: "Omer Katz", email: "omer@hds.test", employment_type: "temporary", engaged_from: "2026-11-13", engaged_until: "2027-02-12", project_ids: ["p1"] };

function api() {
  return new FakeApi()
    .on("GET", "/me", json(200, ADMIN))
    .on("GET", "/me/saved-views", json(200, { items: [] }))
    .on("GET", "/api/v1/employees", json(200, { items: [DANA, OMER] }))
    .on("GET", "/api/v1/organisation/scheduling", json(200, { max_weekly_hours: 48, min_rest_hours: 8 }))
    // This week Dana is rostered for five 9-hour shifts, 45 h (near 48), drafts included; Omer for 8 h.
    .on("GET", "/api/v1/shifts", json(200, { items: [
      { id: "s1", assignments: [0, 1, 2, 3, 4].map((d) => ({ user_id: "u1", starts_at: `2026-11-1${d}T05:00:00Z`, ends_at: `2026-11-1${d}T14:00:00Z` })).concat([{ user_id: "u2", starts_at: "2026-11-10T05:00:00Z", ends_at: "2026-11-10T13:00:00Z" }]) },
    ], next_cursor: null }))
    // The month's worked hours come from the report.
    .on("GET", "/api/v1/reports/hours", json(200, { from: "x", to: "y", group_by: "employee", hours: "0", shifts: 0, shifts_missing_rate: 0, groups: [{ id: "u1", name: "Dana Levi", hours: "120.00", shifts: 15, shifts_missing_rate: 0, rows: [] }] }));
}

beforeEach(async () => {
  await setupI18n("en");
});

test("hours, the near-limit flag and the temporary chip", async () => {
  router.__setSearchParams({});
  await renderWith(await signedIn(api()), <StaffScreen />);
  // At phone width (the tests' default) each record is a card whose second line carries the columns as text.
  expect(await screen.findByText(/Active · Temporary · until 12\.02/, {}, { timeout: 5000 })).toBeTruthy();
  expect(await screen.findByText(/45 h · Near 48 h/, {}, { timeout: 5000 })).toBeTruthy();
  expect(screen.getByText(/120 h/)).toBeTruthy();
  expect(screen.getAllByText(/Near 48 h/)).toHaveLength(1);
  expect(screen.getAllByText(/Temporary · until/)).toHaveLength(1);
});

test("?type=temporary shows only temporary workers", async () => {
  router.__setSearchParams({ type: "temporary" });
  await renderWith(await signedIn(api()), <StaffScreen />);
  expect(await screen.findAllByText("Omer Katz", {}, { timeout: 5000 })).not.toHaveLength(0);
  expect(screen.queryAllByText("Dana Levi")).toHaveLength(0);
});

test("the model: employment, filter, limit, dates", () => {
  expect(employmentOf(DANA)).toEqual({ employment_type: "permanent", engaged_from: null, engaged_until: null, project_ids: [] });
  expect(employmentOf(OMER).employment_type).toBe("temporary");
  expect(passesType(OMER, "temporary")).toBe(true);
  expect(passesType(DANA, "temporary")).toBe(false);
  expect(passesType(DANA, "all")).toBe(true);
  expect(typeFilterOf("temporary")).toBe("temporary");
  expect(typeFilterOf("nonsense")).toBe("all");
  expect(nearWeeklyLimit(44, 48)).toBe(true);
  expect(nearWeeklyLimit(43.5, 48)).toBe(false);
  expect(nearWeeklyLimit(50, 48)).toBe(true);
  expect(nearWeeklyLimit(0, 4)).toBe(false);
  expect(nearWeeklyLimit(47, null)).toBe(false);
  expect(dayMonth("2027-02-12")).toBe("12.02");
  expect(maxEngagedUntil("2026-11-13")).toBe("2027-02-13");
  expect(maxEngagedUntil("2026-11-30")).toBe("2027-02-28");
  expect(maxEngagedUntil("2027-11-30")).toBe("2028-02-29");
  expect(maxEngagedUntil("2026-10-31")).toBe("2027-01-31");
  expect(daysLeft("2026-11-13", "2026-11-27")).toBe(14);
  const twice = { user_id: "u1", starts_at: "2026-11-10T05:00:00Z", ends_at: "2026-11-10T09:30:00Z" };
  expect(scheduledHours([{ assignments: [twice] }, { assignments: [twice, { ...twice, user_id: "u2" }] }])).toEqual(new Map([["u1", 9], ["u2", 4.5]]));
});
