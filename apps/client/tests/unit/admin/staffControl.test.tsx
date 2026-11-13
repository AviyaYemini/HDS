/**
 * Staff control (specs/demo-ceo/plan.md, screen 3): this week's and this month's hours from the
 * hours report by employee (no weekly-limit flag), the temporary chip with its end date and
 * the permanent/temporary filter from the address; plus the 3-month cap. Made-up names only.
 */
import { fireEvent, screen, waitFor, within } from "@testing-library/react-native";
import * as Router from "expo-router";

import type { Me } from "../../../src/api/session";
import { setupI18n } from "../../../src/i18n";
import { StaffScreen } from "../../../src/screens/admin";
import { daysLeft, dayMonth, employmentOf, endingSoon, initials, maxEngagedUntil, passesType, scheduledHours, typeFilterOf } from "../../../src/screens/admin/staffModel";
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
    // This week Dana is rostered for five 9-hour shifts, 45 h, drafts included; Omer for 8 h.
    .on("GET", "/api/v1/shifts", json(200, { items: [
      { id: "s1", assignments: [0, 1, 2, 3, 4].map((d) => ({ user_id: "u1", starts_at: `2026-11-1${d}T05:00:00Z`, ends_at: `2026-11-1${d}T14:00:00Z` })).concat([{ user_id: "u2", starts_at: "2026-11-10T05:00:00Z", ends_at: "2026-11-10T13:00:00Z" }]) },
    ], next_cursor: null }))
    // The month's worked hours come from the report.
    .on("GET", "/api/v1/reports/hours", json(200, { from: "x", to: "y", group_by: "employee", hours: "0", shifts: 0, shifts_missing_rate: 0, groups: [{ id: "u1", name: "Dana Levi", hours: "120.00", shifts: 15, shifts_missing_rate: 0, rows: [] }] }));
}

beforeEach(async () => {
  await setupI18n("en");
});

test("hours and the temporary chip, with no weekly-limit flag", async () => {
  router.__setSearchParams({});
  await renderWith(await signedIn(api()), <StaffScreen />);
  // At phone width (the tests' default) each record is a card: the hours, the rate and the chips.
  expect(await screen.findByTestId("card-temporary-u2", {}, { timeout: 5000 })).toHaveTextContent("Temporary · until 12.02");
  expect(screen.queryByTestId("card-temporary-u1")).toBeNull();
  expect(await screen.findByText("45 h", {}, { timeout: 5000 })).toBeTruthy();
  expect(screen.getByText("120 h")).toBeTruthy();
  expect(screen.getAllByText("This week").length).toBeGreaterThan(0);
  // No weekly-limit flag: warnings are out of the app's scope (the owner, 2026-11-13).
  expect(screen.queryByText(/Near 48/)).toBeNull();
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
  expect(dayMonth("2027-02-12")).toBe("12.02");
  expect(maxEngagedUntil("2026-11-13")).toBe("2027-02-13");
  expect(maxEngagedUntil("2026-11-30")).toBe("2027-02-28");
  expect(maxEngagedUntil("2027-11-30")).toBe("2028-02-29");
  expect(maxEngagedUntil("2026-10-31")).toBe("2027-01-31");
  expect(daysLeft("2026-11-13", "2026-11-27")).toBe(14);
  const twice = { user_id: "u1", starts_at: "2026-11-10T05:00:00Z", ends_at: "2026-11-10T09:30:00Z" };
  expect(scheduledHours([{ assignments: [twice] }, { assignments: [twice, { ...twice, user_id: "u2" }] }])).toEqual(new Map([["u1", 9], ["u2", 4.5]]));
});

test("initials and a temporary worker ending soon", () => {
  expect(initials("Dana Levi")).toBe("DL");
  expect(initials("Noa Ben David")).toBe("ND");
  expect(initials("Omer")).toBe("O");
  expect(initials("")).toBe("");
  expect(initials("דנה לוי")).toBe("דל");
  const temp = (until: string) => ({ ...OMER, engaged_until: until });
  expect(endingSoon(temp("2026-11-27"), "2026-11-13")).toBe(14);
  expect(endingSoon(temp("2026-11-28"), "2026-11-13")).toBeNull();
  expect(endingSoon(temp("2026-11-13"), "2026-11-13")).toBe(0);
  expect(endingSoon(temp("2026-11-12"), "2026-11-13")).toBeNull();
  expect(endingSoon(DANA, "2026-11-13")).toBeNull();
});

test("a temporary worker within 14 days of the end gets an 'ending soon' chip", async () => {
  router.__setSearchParams({});
  const soon = new Date(Date.now() + 5 * 86_400_000).toISOString().slice(0, 10);
  const fake = api().on("GET", "/api/v1/employees", json(200, { items: [DANA, { ...OMER, engaged_until: soon }] }));
  await renderWith(await signedIn(fake), <StaffScreen />);
  expect(await screen.findByTestId("card-ending-u2", {}, { timeout: 5000 })).toHaveTextContent(/Ends in [45] days/);
  expect(screen.queryByTestId("card-ending-u1")).toBeNull();
});

test("staff open sorted by this month's hours, the most first", async () => {
  router.__setSearchParams({});
  const fake = api().on("GET", "/api/v1/reports/hours", json(200, { from: "x", to: "y", group_by: "employee", hours: "0", shifts: 0, shifts_missing_rate: 0, groups: [
    { id: "u1", name: "Dana Levi", hours: "120.00", shifts: 15, shifts_missing_rate: 0, rows: [] },
    { id: "u2", name: "Omer Katz", hours: "200.00", shifts: 25, shifts_missing_rate: 0, rows: [] },
  ] }));
  await renderWith(await signedIn(fake), <StaffScreen />);
  await waitFor(() => expect(screen.getAllByTestId("table-row")[0]).toHaveTextContent(/Omer Katz/));
  expect(screen.getAllByTestId("table-row")[1]).toHaveTextContent(/Dana Levi/);
});

test("an invitation is regular unless 'temporary' is chosen, which asks for dates and projects", async () => {
  router.__setSearchParams({});
  const fake = api()
    .on("GET", "/api/v1/projects", json(200, { items: [{ id: "p1", name: "Harbour gate", status: "active" }] }))
    .on("POST", "/auth/invite", json(202, {}));
  await renderWith(await signedIn(fake), <StaffScreen />);
  await fireEvent.press(await screen.findByTestId("action-invite", {}, { timeout: 5000 }));
  const form = await screen.findByTestId("invite-form");
  // Regular by default: no dates or projects asked.
  expect(screen.queryByTestId("field-engaged_from")).toBeNull();
  await fireEvent.changeText(within(form).getByTestId("field-email"), "new.guard@hds.test");
  await fireEvent.press(within(form).getByTestId("action-save"));
  await waitFor(() => expect(fake.calls("POST", "/auth/invite")).toHaveLength(1));
  expect(fake.calls("POST", "/auth/invite")[0]!.body).toEqual({ email: "new.guard@hds.test", role: "employee" });

  // Temporary: the dates and the projects appear.
  await fireEvent.press(await screen.findByTestId("action-invite"));
  const again = await screen.findByTestId("invite-form");
  await fireEvent.press(within(again).getByTestId("field-employment_type-temporary"));
  expect(within(again).getByTestId("field-engaged_from")).toBeTruthy();
  expect(within(again).getByTestId("field-project_ids-p1")).toBeTruthy();
});

test("with no temporary workers, the temporary filter says how one is marked", async () => {
  router.__setSearchParams({ type: "temporary" });
  const fake = api().on("GET", "/api/v1/employees", json(200, { items: [DANA] }));
  await renderWith(await signedIn(fake), <StaffScreen />);
  expect(await screen.findByText(/No temporary workers\. Mark someone temporary/, {}, { timeout: 5000 })).toBeTruthy();
});
