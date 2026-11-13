/** My pay on screen (FR-024; R56): the period's own last day bounds the scheduled hours, and the rows meet the total. */
import { fireEvent, screen, waitFor } from "@testing-library/react-native";
import * as Router from "expo-router";

import type { Me } from "../../../src/api/session";
import { MyPayScreen } from "../../../src/screens/my-work/pay/MyPayScreen";
import { units } from "../../../src/screens/my-work/pay/model";
import { FakeApi, ME, json, renderWith, signedIn } from "../fakeApi";

const EMPLOYEE: Me = { ...(ME as Me), id: "me", capabilities: ["my_work.read", "account.self"], managed_project_ids: [] };
const setSearchParams = (Router as unknown as { __setSearchParams: (params: Record<string, string>) => void }).__setSearchParams;

const line = (base_rate: string, parts: { hours: string; rate_pct: string; pay: string }[], pay: string) => ({
  base_rate, date: "2026-10-03", starts_at: "2026-10-03T06:00:00Z", ends_at: "2026-10-03T14:00:00Z", hours: "8", parts, pay, pay_enabled: true, project_id: "p", project_name: "Night Watch", rate_missing: false, shift_id: "s", timezone: "Asia/Jerusalem",
});

it("asks for the month's shifts to its last day and shows rows that add up to the total", async () => {
  setSearchParams({ period: "2026-10" });
  const api = new FakeApi()
    .on("GET", "/me", json(200, EMPLOYEE))
    .on("GET", "/me/pay", (seen) =>
      seen.query.includes("month=2026-10")
        ? json(200, { month: "2026-10", hours: "16", pay: "937.50", shifts_missing_rate: 0, lines: [line("50.00", [{ hours: "8", rate_pct: "100", pay: "400.00" }], "400.00"), line("50.00", [{ hours: "6", rate_pct: "100", pay: "300.00" }, { hours: "2", rate_pct: "137.5", pay: "137.50" }], "437.50"), line("50.00", [{ hours: "2", rate_pct: "100", pay: "100.00" }], "100.00")] })
        : json(200, { month: "2026-09", hours: "8", pay: "400.00", shifts_missing_rate: 0, lines: [] }),
    )
    .on("GET", "/me/shifts", json(200, { items: [{ id: "x", starts_at: "2026-10-31T22:30:00Z", ends_at: "2026-11-01T06:00:00Z", timezone: "Asia/Jerusalem", project: { id: "p", name: "P" }, site: { id: "s", name: "S" }, shift_type: "night" }] }));
  await renderWith(await signedIn(api), <MyPayScreen />);
  // The data attributes exist on the web only; here the shown money and the model's exact rows are checked.
  const total = await screen.findByTestId("pay-total");
  expect(total).toHaveTextContent(/937\.50/);
  const rows = screen.getAllByTestId("pay-breakdown-row");
  expect(rows).toHaveLength(2);
  expect(rows[0]).toHaveTextContent(/100%/);
  expect(rows[1]).toHaveTextContent(/137\.5%/);
  expect(units("937.50")).toBe(9375000n);
  expect(screen.getByTestId("pay-estimate-label")).toBeTruthy();
  // A finished month says last month's total plainly, in agorot, never a "drop".
  expect(screen.getByTestId("pay-change")).toHaveTextContent(/^Last month: \u2066₪400\.00\u2069$/);
  await waitFor(() => expect(api.calls("GET", "/me/shifts")).toHaveLength(1));
  expect(api.calls("GET", "/me/shifts")[0]?.query).toBe("?from=2026-10-01&to=2026-10-31");
  await fireEvent.press(screen.getByTestId("view-hours"));
  expect(await screen.findByTestId("hours-scheduled")).toHaveTextContent(/7:30/);
  setSearchParams({});
});
