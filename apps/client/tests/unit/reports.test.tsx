/**
 * Reports in 007's design (specs/007-ux-redesign FR-050, FR-025): the work log and the hours report
 * open on this pay period and never ask past today, their key figures are the API's own totals,
 * a bar opens its rows, a scope asks for its choice, and pay shows only where the API sends it.
 */
import { fireEvent, screen } from "@testing-library/react-native";

import { setupI18n } from "../../src/i18n";
import { DEFAULT_TIME_ZONE, todayIn } from "../../src/locale";
import { HoursReportScreen, WorkLogScreen } from "../../src/screens/reports/ReportScreens";
import { FakeApi, json, renderWith, signedIn } from "./fakeApi";

const ROW = { user_id: "u1", full_name: "Dana Levi", project_id: "p1", project_name: "Gate", shifts: 2, hours: "16.00", first_date: "2026-10-05", last_date: "2026-10-06" };

beforeEach(async () => {
  await setupI18n("en");
});

test("the work log asks for the whole company this pay period, up to today, and shows the API's figures", async () => {
  const today = todayIn(DEFAULT_TIME_ZONE);
  const api = new FakeApi()
    .on("GET", "/api/v1/projects", json(200, { items: [{ id: "p1", name: "Gate" }] }))
    .on("GET", "/api/v1/reports/work-log", json(200, { from: `${today.slice(0, 8)}01`, to: today, people: 3, shifts: 5, hours: "41.50", rows: [ROW] }));
  await renderWith(await signedIn(api), <WorkLogScreen />);
  expect(await screen.findByText("Work log")).toBeTruthy();
  const asked = new URLSearchParams(api.calls("GET", "/api/v1/reports/work-log")[0]!.query);
  expect(asked.get("from")).toBe(`${today.slice(0, 8)}01`);
  expect(asked.get("to")).toBe(today);
  // The whole company by default: no project, site or group (the owner, PR 5).
  expect(asked.has("project_id") || asked.has("site_id") || asked.has("group_id")).toBe(false);
  // The API's own totals, shown as they came, never the rows added up (16.00 h, 1 person here):
  // data-value is web only, checked by g-reports.
  expect(screen.getByTestId("figure-hours")).toHaveTextContent(/41\.50/);
  expect(screen.getByTestId("figure-people")).toHaveTextContent(/People\s*3$/);
  expect(screen.getByTestId("figure-shifts")).toHaveTextContent(/Person-shifts\s*5$/);
  await fireEvent.press(screen.getByTestId("report-bar"));
  expect(await screen.findByTestId("report-rows")).toBeTruthy();
  expect(screen.getAllByText(/Dana Levi/).length).toBeGreaterThan(0);
});

test("a site waits for its choice before asking the API", async () => {
  const api = new FakeApi()
    .on("GET", "/api/v1/projects", json(200, { items: [{ id: "p1", name: "Gate" }] }))
    .on("GET", "/api/v1/sites", json(200, { items: [{ id: "s1", name: "HQ" }] }))
    .on("GET", "/api/v1/reports/work-log", json(200, { from: "2026-11-01", to: "2026-11-08", people: 0, shifts: 0, hours: "0", rows: [] }));
  await renderWith(await signedIn(api), <WorkLogScreen />);
  await screen.findByTestId("figure-people");
  const before = api.calls("GET", "/api/v1/reports/work-log").length;
  await fireEvent.press(screen.getByTestId("scope-site"));
  expect(await screen.findByText("Choose a site.")).toBeTruthy();
  expect(api.calls("GET", "/api/v1/reports/work-log")).toHaveLength(before);
});

test("the hours report shows pay only where the API sends it, and its missing rates", async () => {
  const group = { id: "u1", name: "Dana Levi", hours: "8.00", shifts: 1, pay: null, shifts_missing_rate: 1, rows: [{ id: "p1", name: "Gate", hours: "8.00", shifts: 1, pay: null, shifts_missing_rate: 1 }] };
  const api = new FakeApi().on("GET", "/api/v1/reports/hours", json(200, { from: "2026-11-01", to: "2026-11-08", group_by: "employee", hours: "8.00", shifts: 1, pay: null, shifts_missing_rate: 1, groups: [group] }));
  await renderWith(await signedIn(api), <HoursReportScreen />);
  await screen.findByTestId("figure-hours");
  expect(screen.queryByTestId("figure-pay")).toBeNull();
  expect(screen.getAllByText(/no pay rate|without a rate|missing/i).length).toBeGreaterThan(0);
  await fireEvent.press(screen.getByTestId("group-project"));
  const asked = api.calls("GET", "/api/v1/reports/hours").map((c) => new URLSearchParams(c.query).get("group_by"));
  expect(asked).toContain("project");
});
