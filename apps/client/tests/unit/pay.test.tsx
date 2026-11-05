// My pay (005 US1): the month's gross pay so far, a line per ended shift, hidden and missing pay.
import { act, fireEvent, screen } from "@testing-library/react-native";

import { MyPayScreen } from "../../src/screens/MyPayScreen";
import { FakeApi, json, problem, renderWith, signedIn } from "./fakeApi";

function line(id: string, extra: Record<string, unknown> = {}) {
  return {
    shift_id: id,
    project_id: "p1",
    project_name: "Night Watch",
    date: "2026-10-09",
    starts_at: "2026-10-09T19:00:00Z",
    ends_at: "2026-10-10T03:00:00Z",
    timezone: "Asia/Jerusalem",
    hours: "8.00",
    base_rate: "50.0000",
    rate_missing: false,
    pay_enabled: true,
    parts: [
      { rate_pct: "150.00", hours: "7.00" },
      { rate_pct: "175.00", hours: "1.00" },
    ],
    pay: "612.50",
    ...extra,
  };
}

const OCTOBER = {
  month: "2026-10",
  lines: [
    line("a"),
    line("b", { pay_enabled: false, pay: undefined, base_rate: null, parts: [{ rate_pct: "100.00", hours: "8.00" }] }),
    line("c", { rate_missing: true, pay: undefined, base_rate: null }),
  ],
  hours: "24.00",
  pay: "612.50",
  shifts_missing_rate: 1,
};

test("shows each shift's hours at each rate, its pay, and the month's total", async () => {
  const api = new FakeApi().on("GET", "/me/pay", json(200, OCTOBER));
  await renderWith(await signedIn(api), <MyPayScreen />);

  const paid = await screen.findByTestId("pay-line-a");
  expect(paid).toHaveTextContent(/Night Watch/);
  expect(paid).toHaveTextContent(/7\.00 h at 150%/);
  expect(paid).toHaveTextContent(/1\.00 h at 175%/);
  expect(paid).toHaveTextContent(/612\.50/);
  expect(screen.getByTestId("pay-total")).toHaveTextContent(/24\.00 h/);
  expect(screen.getByTestId("pay-total")).toHaveTextContent(/612\.50/);
});

test("a project without pay and a missing rate are said, never shown as zero", async () => {
  const api = new FakeApi().on("GET", "/me/pay", json(200, OCTOBER));
  await renderWith(await signedIn(api), <MyPayScreen />);

  expect(await screen.findByTestId("pay-line-b")).toHaveTextContent(/No pay on this project/);
  expect(screen.getByTestId("pay-line-c")).toHaveTextContent(/No pay rate set/);
  expect(screen.getByTestId("pay-line-c")).not.toHaveTextContent(/0\.00/);
  expect(screen.getByTestId("pay-missing")).toHaveTextContent(/1 shift/);
});

test("each shift shows its start and end at the site and the rate it was paid at", async () => {
  const api = new FakeApi().on("GET", "/me/pay", json(200, OCTOBER));
  await renderWith(await signedIn(api), <MyPayScreen />);

  const paid = await screen.findByTestId("pay-line-a");
  // 19:00 to 03:00 UTC is 22:00 to 06:00 in Israel, ending the next day (FR-018).
  expect(paid).toHaveTextContent(/22:00/);
  expect(paid).toHaveTextContent(/06:00/);
  expect(paid).toHaveTextContent(/50\.0000 an hour/);
  expect(screen.getByTestId("pay-line-c")).not.toHaveTextContent(/an hour/);
});

test("a month whose rates are all missing shows no total pay, never zero", async () => {
  const missing = {
    ...OCTOBER,
    lines: [line("c", { rate_missing: true, pay: undefined, base_rate: null })],
    hours: "8.00",
    pay: undefined,
    shifts_missing_rate: 1,
  };
  const api = new FakeApi().on("GET", "/me/pay", json(200, missing));
  await renderWith(await signedIn(api), <MyPayScreen />);

  const total = await screen.findByTestId("pay-total");
  expect(total).toHaveTextContent(/8\.00 h/);
  expect(total).not.toHaveTextContent(/Total/);
  expect(screen.getByTestId("pay-missing")).toHaveTextContent(/1 shift/);
});

test("the month can be moved back and forward", async () => {
  jest.useFakeTimers({ now: new Date("2026-11-05T10:00:00"), doNotFake: ["nextTick", "setImmediate", "queueMicrotask", "setTimeout", "setInterval"] });
  try {
    const api = new FakeApi().on("GET", "/me/pay", json(200, { ...OCTOBER, lines: [], hours: "0.00", pay: "0.00", shifts_missing_rate: 0 }));
    await renderWith(await signedIn(api), <MyPayScreen />);
    await screen.findByTestId("pay-total");
    expect(api.calls("GET", "/me/pay")[0]!.query).toBe("?month=2026-11");

    await fireEvent.press(screen.getByTestId("pay-prev"));
    await screen.findByTestId("pay-total");
    await fireEvent.press(screen.getByTestId("pay-prev"));
    await fireEvent.press(screen.getByTestId("pay-next"));
    await screen.findByTestId("pay-total");
    expect(api.calls("GET", "/me/pay").map((c) => c.query)).toContain("?month=2026-09");
    expect(api.calls("GET", "/me/pay").at(-1)!.query).toBe("?month=2026-10");
    expect(await screen.findByText("No shifts worked yet this month.")).toBeTruthy();
  } finally {
    jest.useRealTimers();
  }
});

test("an earlier month's late answer never replaces the month shown", async () => {
  jest.useFakeTimers({ now: new Date("2026-11-05T10:00:00"), doNotFake: ["nextTick", "setImmediate", "queueMicrotask", "setTimeout", "setInterval"] });
  try {
    let release: (() => void) | undefined;
    const slow = new Promise<void>((resolve) => (release = resolve));
    const api = new FakeApi().on("GET", "/me/pay", async (seen) => {
      if (seen.query === "?month=2026-11") {
        await slow;
        return json(200, { ...OCTOBER, month: "2026-11", lines: [line("late")] });
      }
      return json(200, { ...OCTOBER, month: "2026-10", lines: [] });
    });
    await renderWith(await signedIn(api), <MyPayScreen />);
    await fireEvent.press(screen.getByTestId("pay-prev"));
    expect(await screen.findByText("No shifts worked yet this month.")).toBeTruthy();
    await act(async () => {
      release!();
      await new Promise((resolve) => setTimeout(resolve, 50));
    });
    expect(screen.getByText("October 2026")).toBeTruthy();
    expect(screen.queryByTestId("pay-line-late")).toBeNull();
    expect(api.calls("GET", "/me/pay")).toHaveLength(2);
  } finally {
    jest.useRealTimers();
  }
});

test("a failed load is shown translated and can be tried again", async () => {
  const api = new FakeApi().on("GET", "/me/pay", problem(500, "unknown.code"), json(200, OCTOBER));
  await renderWith(await signedIn(api), <MyPayScreen />);

  expect(await screen.findByTestId("error-message")).toHaveTextContent("Something went wrong. Please try again.");
  await fireEvent.press(screen.getByTestId("retry"));
  expect(await screen.findByTestId("pay-line-a")).toBeTruthy();
});
