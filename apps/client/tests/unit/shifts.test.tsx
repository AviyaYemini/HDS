// My shifts (US7): the caller's published shifts in the site's zone, and an empty state.
import { fireEvent, screen } from "@testing-library/react-native";

import { MyShiftsScreen } from "../../src/screens/MyShiftsScreen";
import { FakeApi, json, problem, renderWith, signedIn } from "./fakeApi";

function shift(id: string, starts_at: string, ends_at: string, timezone = "Asia/Jerusalem") {
  return {
    id,
    starts_at,
    ends_at,
    timezone,
    shift_type: "night",
    project: { id: "p1", name: "Night Watch" },
    site: { id: "s1", name: "HQ" },
  };
}

test("lists the shifts with times at the site, naming the day a night shift ends", async () => {
  const api = new FakeApi().on(
    "GET",
    "/me/shifts",
    json(200, {
      items: [
        shift("a", "2027-06-07T04:00:00Z", "2027-06-07T12:00:00Z"),
        shift("b", "2027-06-07T19:00:00Z", "2027-06-08T03:00:00Z"),
      ],
    }),
  );
  await renderWith(await signedIn(api), <MyShiftsScreen />);
  const day = await screen.findByTestId("shift-a");
  expect(day).toHaveTextContent(/Night Watch/);
  expect(day).toHaveTextContent(/HQ/);
  expect(day).toHaveTextContent(/Mon, Jun 7/);
  expect(day).toHaveTextContent(/07:00 – 15:00/);
  expect(day).toHaveTextContent(/Asia\/Jerusalem/);
  expect(screen.getByTestId("shift-b")).toHaveTextContent(/22:00 – 06:00 \(Tue, Jun 8\)/);
});

test("says so when there are no shifts", async () => {
  const api = new FakeApi().on("GET", "/me/shifts", json(200, { items: [] }));
  await renderWith(await signedIn(api), <MyShiftsScreen />);
  expect(await screen.findByText("You have no published shifts.")).toBeTruthy();
});

test("a failed load is shown translated and can be tried again", async () => {
  const api = new FakeApi().on(
    "GET",
    "/me/shifts",
    problem(500, "unknown.code"),
    json(200, { items: [shift("a", "2027-06-07T04:00:00Z", "2027-06-07T12:00:00Z")] }),
  );
  await renderWith(await signedIn(api), <MyShiftsScreen />);
  expect(await screen.findByTestId("error-message")).toHaveTextContent("Something went wrong. Please try again.");
  await fireEvent.press(screen.getByTestId("retry"));
  expect(await screen.findByTestId("shift-a")).toBeTruthy();
});

test("only shifts from today on are asked for", async () => {
  jest.useFakeTimers({ now: new Date("2027-06-03T10:00:00"), doNotFake: ["nextTick", "setImmediate", "queueMicrotask", "setTimeout", "setInterval"] });
  try {
    const api = new FakeApi().on("GET", "/me/shifts", json(200, { items: [] }));
    await renderWith(await signedIn(api), <MyShiftsScreen />);
    await screen.findByText("You have no published shifts.");
    expect(api.calls("GET", "/me/shifts")[0]!.query).toBe("?from=2027-06-03");
  } finally {
    jest.useRealTimers();
  }
});
