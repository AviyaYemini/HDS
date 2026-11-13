/**
 * Hours and costs (specs/demo-ceo/plan.md, screen 4): numbered key figures that are the API's own
 * totals; billing and pay shown only when the API sends them (never as zero); a group without a
 * rate says so; projects first, and the bars name each group. Made-up names only.
 */
import { screen } from "@testing-library/react-native";

import { setupI18n } from "../../src/i18n";
import { HoursCostsScreen, formatHoursFigure, topGroups } from "../../src/screens/reports/HoursCostsScreen";
import { FakeApi, json, renderWith, signedIn } from "./fakeApi";

const group = (id: string, name: string, hours: string, extra: Record<string, unknown> = {}) => ({ id, name, hours, shifts: 2, pay: null, shifts_missing_rate: 0, rows: [], ...extra });

beforeEach(async () => {
  await setupI18n("en");
});

test("without pay or billing from the API, neither figure nor column shows", async () => {
  const api = new FakeApi()
    .on("GET", "/api/v1/projects", json(200, { items: [{ id: "p1", name: "Harbour gate", type: "security" }] }))
    .on("GET", "/api/v1/reports/hours", json(200, { from: "2026-11-01", to: "2026-11-12", group_by: "project", hours: "1984.00", shifts: 248, pay: null, shifts_missing_rate: 0, groups: [group("p1", "Harbour gate", "1984.00")] }));
  await renderWith(await signedIn(api), <HoursCostsScreen />);
  expect(await screen.findByTestId("figure-hours", {}, { timeout: 5000 })).toHaveTextContent(/01\s*Hours\s*1,984/);
  expect(screen.queryByTestId("figure-pay")).toBeNull();
  expect(screen.queryByTestId("figure-billing")).toBeNull();
  expect(screen.queryByText("Gross pay")).toBeNull();
  expect(new URLSearchParams(api.calls("GET", "/api/v1/reports/hours")[0]!.query).get("group_by")).toBe("project");
});

test("billing and pay show as the API sends them, and a group without a rate says so", async () => {
  const api = new FakeApi()
    .on("GET", "/api/v1/projects", json(200, { items: [] }))
    .on("GET", "/api/v1/reports/hours", json(200, {
      from: "2026-11-01", to: "2026-11-12", group_by: "project", hours: "20.00", shifts: 3, pay: "1500.00", billing: "2200.00", shifts_missing_rate: 1,
      groups: [group("p1", "Harbour gate", "12.00", { pay: "1500.00", billing: "1400.00" }), group("p2", "City museum", "8.00", { pay: null, billing: "800.00", shifts_missing_rate: 1 })],
    }));
  await renderWith(await signedIn(api), <HoursCostsScreen />);
  expect(await screen.findByTestId("figure-billing", {}, { timeout: 5000 })).toHaveTextContent(/02\s*Billing/);
  expect(screen.getByTestId("figure-pay")).toHaveTextContent(/03\s*Gross pay/);
  expect(screen.getByText("No rate · 1")).toBeTruthy();
});

test("hours figures and the bars' order", () => {
  expect(formatHoursFigure("1984.00", "en")).toBe("1,984");
  expect(formatHoursFigure("12.50", "en")).toBe("12.5");
  expect(topGroups([{ hours: "1" }, { hours: "30" }, { hours: "7" }], 2).map((g) => g.hours)).toEqual(["30", "7"]);
});
