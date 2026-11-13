/**
 * The snapshot dashboard (specs/demo-ceo/plan.md, screen 1): the period's range asked of the API,
 * the four numbered key figures as the API sent them, the temporary-workers panel, the project
 * table's chips, the estimated-rates note, and no pay where the API hides it. Made-up names only.
 */
import { fireEvent, screen, waitFor } from "@testing-library/react-native";

import { setupI18n } from "../../../src/i18n";
import { DEFAULT_TIME_ZONE, todayIn } from "../../../src/locale";
import { coveragePercent, formatPercent, healthChip, periodRange } from "../../../src/screens/overview/model";
import { OverviewScreen } from "../../../src/screens/overview/OverviewScreen";
import { FakeApi, json, renderWith, signedIn } from "../fakeApi";
import { OVERVIEW } from "./fixture";

beforeEach(async () => {
  await setupI18n("en");
});

test("periods: the month, quarter and year holding today", () => {
  expect(periodRange("month", "2026-11-13")).toEqual({ from: "2026-11-01", to: "2026-11-30" });
  expect(periodRange("month", "2028-02-10")).toEqual({ from: "2028-02-01", to: "2028-02-29" });
  expect(periodRange("quarter", "2026-11-13")).toEqual({ from: "2026-10-01", to: "2026-12-31" });
  expect(periodRange("quarter", "2027-01-01")).toEqual({ from: "2027-01-01", to: "2027-03-31" });
  expect(periodRange("year", "2026-11-13")).toEqual({ from: "2026-01-01", to: "2026-12-31" });
});

test("percents, coverage and chips", () => {
  expect(formatPercent("12.50")).toBe("12.5%");
  expect(formatPercent("-4.00")).toBe("−4%");
  expect(formatPercent(null)).toBe("—");
  expect(coveragePercent({ ...OVERVIEW.coverage, filled: 96, needed: 100 })).toBe(96);
  expect(coveragePercent({ ...OVERVIEW.coverage, filled: 3, needed: 0 })).toBeNull();
  expect(coveragePercent({ ...OVERVIEW.coverage, filled: 12, needed: 10 })).toBe(100);
  expect([healthChip("ok"), healthChip("gaps"), healthChip("loss"), healthChip("no_rate")]).toEqual(["success", "warning", "danger", "neutral"]);
});

test("the dashboard asks this month and shows the API's figures, the panel, the chips and the note", async () => {
  const api = new FakeApi().on("GET", "/api/v1/reports/overview", json(200, OVERVIEW));
  await renderWith(await signedIn(api), <OverviewScreen />);
  expect(await screen.findByTestId("figure-billing", {}, { timeout: 5000 })).toHaveTextContent(/01\s*Billing · so far/);
  const asked = new URLSearchParams(api.calls("GET", "/api/v1/reports/overview")[0]!.query);
  expect(asked.get("from")).toBe(periodRange("month", todayIn(DEFAULT_TIME_ZONE)).from);
  expect(screen.getByTestId("figure-forecast")).toHaveTextContent(/margin 18%/);
  expect(screen.getByTestId("figure-coverage")).toHaveTextContent(/96%.*4 empty places in 2 projects/);
  expect(screen.getByTestId("figure-people")).toHaveTextContent(/11\s*\/ 14 active/);
  expect(screen.getByTestId("panel-temporary")).toHaveTextContent(/3.*1 ends in the next two weeks/);
  expect(screen.getByTestId("status-p2")).toHaveTextContent("2 empty");
  expect(screen.getByTestId("status-p3")).toHaveTextContent("Loss");
  expect(screen.getByText("Seaside mall *")).toBeTruthy();
  expect(screen.getByTestId("note-estimated")).toBeTruthy();
  expect(screen.getAllByTestId("bar-column")).toHaveLength(2);
});

test("where pay is hidden the API sends no money: hours stand in, with no margin or money anywhere", async () => {
  const { billing_actual, billing_forecast, pay_actual, pay_forecast, margin_pct, ...rest } = OVERVIEW;
  void [billing_actual, billing_forecast, pay_actual, pay_forecast, margin_pct];
  const hidden = { ...rest, weeks: OVERVIEW.weeks.map(({ week_start, hours, forecast }) => ({ week_start, hours, forecast })), projects: OVERVIEW.projects.map(({ billing, pay, margin_pct: m, ...p }) => (void [billing, pay, m], p)) };
  const api = new FakeApi().on("GET", "/api/v1/reports/overview", json(200, hidden));
  await renderWith(await signedIn(api), <OverviewScreen />);
  expect(await screen.findByTestId("figure-hours", {}, { timeout: 5000 })).toHaveTextContent(/01\s*Hours · so far\s*1,984/);
  expect(screen.getByTestId("figure-forecast")).toHaveTextContent(/4,310/);
  expect(screen.queryByTestId("figure-billing")).toBeNull();
  expect(screen.queryByText(/margin/i)).toBeNull();
  expect(screen.queryByText(/₪/)).toBeNull();
  expect(screen.getByText("Hours · by week")).toBeTruthy();
});

test("switching to the quarter asks the quarter", async () => {
  const api = new FakeApi().on("GET", "/api/v1/reports/overview", json(200, OVERVIEW));
  await renderWith(await signedIn(api), <OverviewScreen />);
  await fireEvent.press(await screen.findByTestId("period-quarter", {}, { timeout: 5000 }));
  await waitFor(() => expect(api.calls("GET", "/api/v1/reports/overview").length).toBeGreaterThan(1));
  const asked = new URLSearchParams(api.calls("GET", "/api/v1/reports/overview").at(-1)!.query);
  expect(asked.get("from")).toBe(periodRange("quarter", todayIn(DEFAULT_TIME_ZONE)).from);
});
