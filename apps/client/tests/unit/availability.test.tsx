// My availability (US4): the open window, the employee's entries, adding, changing and removing
// them, the API's refusals translated, and read-only entries once their window has closed.
import { fireEvent, screen, waitFor, within } from "@testing-library/react-native";

import { AvailabilityScreen } from "../../src/screens/AvailabilityScreen";
import { FakeApi, ME, json, problem, renderWith, signedIn } from "./fakeApi";

const ENTRIES = `/api/v1/employees/${ME.id}/constraints`;
const MORNING = "33333333-3333-4333-8333-333333333333";

const OPEN = {
  id: "w1",
  starts_on: "2027-06-06",
  ends_on: "2027-06-12",
  deadline: "2027-06-01T09:30:00Z",
  is_open: true,
};
const CLOSED = { ...OPEN, id: "w0", starts_on: "2027-05-30", ends_on: "2027-06-05", is_open: false };

function entry(id: string, extra: Record<string, unknown> = {}) {
  return {
    id,
    user_id: ME.id,
    kind: "blocked",
    absence_kind: null,
    starts_on: "2027-06-07",
    ends_on: "2027-06-07",
    shift_type_id: null,
    weekday: null,
    created_at: "2027-05-01T00:00:00Z",
    updated_at: "2027-05-01T00:00:00Z",
    ...extra,
  };
}

const TYPES = {
  items: [
    {
      id: MORNING,
      name: "morning",
      starts_at_local: "07:00",
      ends_at_local: "15:00",
      crosses_midnight: false,
      is_active: true,
      created_at: "2027-01-01T00:00:00Z",
      updated_at: "2027-01-01T00:00:00Z",
    },
  ],
};

async function show(api: FakeApi, windows: unknown[], entries: unknown[]) {
  api
    .on("GET", "/api/v1/availability-windows", json(200, { items: windows }))
    .on("GET", ENTRIES, json(200, { items: entries }))
    .on("GET", "/api/v1/shift-types", json(200, TYPES));
  const session = await signedIn(api);
  await renderWith(session, <AvailabilityScreen />);
  await screen.findByTestId("entries");
}

test("shows the open window with its deadline and the employee's entries", async () => {
  const api = new FakeApi();
  await show(api, [OPEN], [entry("e1"), entry("e2", { kind: "open", shift_type_id: MORNING })]);
  const window = screen.getByTestId("window-w1");
  expect(window).toHaveTextContent(/Sun, Jun 6 to Sat, Jun 12/);
  expect(window).toHaveTextContent(/Submit by Jun 1, 2027/);
  expect(screen.getByTestId("entry-e1")).toHaveTextContent(/Can't work.*Every shift/);
  expect(screen.getByTestId("entry-e2")).toHaveTextContent(/Would like to work.*morning/);
});

test("says so when there are no entries and no open window", async () => {
  const api = new FakeApi();
  await show(api, [CLOSED], []);
  expect(screen.getByText("You have no entries yet.")).toBeTruthy();
  expect(screen.getByText(/No submission window is open/)).toBeTruthy();
});

test("adds an entry for one shift type", async () => {
  const api = new FakeApi().on("POST", ENTRIES, json(201, entry("e9", { kind: "open" })));
  await show(api, [OPEN], []);
  await fireEvent.press(screen.getByTestId("add-entry"));
  await fireEvent.press(screen.getByTestId("kind-open"));
  await fireEvent.changeText(screen.getByTestId("first-day"), "2027-06-08");
  await fireEvent.changeText(screen.getByTestId("last-day"), "2027-06-09");
  await fireEvent.press(screen.getByTestId(`type-${MORNING}`));
  await fireEvent.press(screen.getByTestId("save-entry"));
  await waitFor(() => expect(api.calls("POST", ENTRIES)).toHaveLength(1));
  expect(api.calls("POST", ENTRIES)[0]!.body).toEqual({
    kind: "open",
    absence_kind: null,
    starts_on: "2027-06-08",
    ends_on: "2027-06-09",
    shift_type_id: MORNING,
  });
  await waitFor(() => expect(api.calls("GET", ENTRIES)).toHaveLength(2));
  expect(screen.queryByTestId("save-entry")).toBeNull();
});

test("adds an absence with its reason and no shift type", async () => {
  const api = new FakeApi().on("POST", ENTRIES, json(201, entry("e9", { kind: "absence" })));
  await show(api, [OPEN], []);
  await fireEvent.press(screen.getByTestId("add-entry"));
  await fireEvent.press(screen.getByTestId(`type-${MORNING}`));
  await fireEvent.press(screen.getByTestId("kind-absence"));
  expect(screen.queryByTestId(`type-${MORNING}`)).toBeNull();
  await fireEvent.press(screen.getByTestId("absence-reserve_duty"));
  await fireEvent.press(screen.getByTestId("save-entry"));
  await waitFor(() => expect(api.calls("POST", ENTRIES)).toHaveLength(1));
  expect(api.calls("POST", ENTRIES)[0]!.body).toMatchObject({
    kind: "absence",
    absence_kind: "reserve_duty",
    shift_type_id: null,
    starts_on: "2027-06-06",
  });
});

test("changes an entry", async () => {
  const api = new FakeApi().on("PATCH", "/api/v1/constraints/e1", json(200, entry("e1", { kind: "close" })));
  await show(api, [OPEN], [entry("e1")]);
  await fireEvent.press(screen.getByTestId("edit-e1"));
  expect(screen.getByTestId("first-day").props.value).toBe("2027-06-07");
  await fireEvent.press(screen.getByTestId("kind-close"));
  await fireEvent.press(screen.getByTestId("save-entry"));
  await waitFor(() => expect(api.calls("PATCH", "/api/v1/constraints/e1")).toHaveLength(1));
  expect(api.calls("PATCH", "/api/v1/constraints/e1")[0]!.body).toMatchObject({ kind: "close" });
});

test("cancelling a change sends nothing", async () => {
  const api = new FakeApi();
  await show(api, [OPEN], [entry("e1")]);
  await fireEvent.press(screen.getByTestId("edit-e1"));
  await fireEvent.press(screen.getByTestId("cancel-entry"));
  expect(screen.queryByTestId("save-entry")).toBeNull();
  expect(api.seen.filter((s) => s.method !== "GET" && s.path !== "/auth/login")).toEqual([]);
});

test("removes an entry", async () => {
  const api = new FakeApi().on("DELETE", "/api/v1/constraints/e1", new Response(null, { status: 204 }));
  await show(api, [OPEN], [entry("e1")]);
  await fireEvent.press(screen.getByTestId("remove-e1"));
  await waitFor(() => expect(api.calls("DELETE", "/api/v1/constraints/e1")).toHaveLength(1));
  await waitFor(() => expect(api.calls("GET", ENTRIES)).toHaveLength(2));
});

test.each([
  ["availability.window_closed", "The submission window for these dates has closed."],
  ["availability.conflict", "Another entry already applies to one of these shifts and dates."],
])("shows the refusal %s translated and keeps the form", async (code, text) => {
  const api = new FakeApi().on("POST", ENTRIES, problem(409, code));
  await show(api, [OPEN], []);
  await fireEvent.press(screen.getByTestId("add-entry"));
  await fireEvent.press(screen.getByTestId("save-entry"));
  expect(await screen.findByTestId("error-message")).toHaveTextContent(text);
  expect(screen.getByTestId("save-entry")).toBeTruthy();
});

test("after the deadline entries are read-only, and only an absence can be added", async () => {
  const api = new FakeApi();
  await show(api, [CLOSED], [entry("e1", { starts_on: "2027-06-01", ends_on: "2027-06-01" }), entry("e2", { kind: "absence", absence_kind: "sick" })]);
  const closed = screen.getByTestId("entry-e1");
  expect(within(closed).queryByTestId("edit-e1")).toBeNull();
  expect(within(closed).queryByTestId("remove-e1")).toBeNull();
  expect(closed).toHaveTextContent(/has closed/);
  expect(screen.getByTestId("edit-e2")).toBeTruthy();
  await fireEvent.press(screen.getByTestId("add-entry"));
  expect(screen.getByTestId("kind-absence")).toBeTruthy();
  expect(screen.queryByTestId("kind-blocked")).toBeNull();
});

test("an entry spanning two open windows can still be changed", async () => {
  const next = { ...OPEN, id: "w2", starts_on: "2027-06-13", ends_on: "2027-06-19" };
  const api = new FakeApi();
  await show(api, [OPEN, next], [entry("e1", { starts_on: "2027-06-11", ends_on: "2027-06-14" })]);
  expect(screen.getByTestId("edit-e1")).toBeTruthy();
});

test("an entry reaching past the open window is read-only", async () => {
  const api = new FakeApi();
  await show(api, [OPEN], [entry("e1", { starts_on: "2027-06-11", ends_on: "2027-06-14" })]);
  expect(screen.queryByTestId("edit-e1")).toBeNull();
});

test("a failed load is shown translated and can be tried again", async () => {
  const api = new FakeApi()
    .on("GET", "/api/v1/availability-windows", problem(403, "users.forbidden"), json(200, { items: [OPEN] }))
    .on("GET", ENTRIES, json(200, { items: [] }))
    .on("GET", "/api/v1/shift-types", json(200, TYPES));
  const session = await signedIn(api);
  await renderWith(session, <AvailabilityScreen />);
  expect(await screen.findByTestId("error-message")).toHaveTextContent("This action isn't available to you");
  await fireEvent.press(screen.getByTestId("retry"));
  expect(await screen.findByTestId("window-w1")).toBeTruthy();
});

test("only the current windows and entries are asked for", async () => {
  jest.useFakeTimers({ now: new Date("2027-06-03T10:00:00"), doNotFake: ["nextTick", "setImmediate", "queueMicrotask", "setTimeout", "setInterval"] });
  try {
    const api = new FakeApi();
    await show(api, [OPEN], []);
    expect(api.calls("GET", "/api/v1/availability-windows")[0]!.query).toBe("?from=2027-06-03");
    expect(api.calls("GET", ENTRIES)[0]!.query).toBe("?from=2027-06-03");
  } finally {
    jest.useRealTimers();
  }
});

test("an entry for one weekday says which day", async () => {
  const api = new FakeApi();
  await show(api, [OPEN], [entry("e1", { starts_on: "2027-06-06", ends_on: "2027-06-12", weekday: 0 })]);
  expect(screen.getByTestId("entry-e1")).toHaveTextContent(/Sundays only/);
});

test("with no window open an absence can be changed but not into another kind", async () => {
  const api = new FakeApi();
  await show(api, [CLOSED], [entry("e2", { kind: "absence", absence_kind: "sick" })]);
  await fireEvent.press(screen.getByTestId("edit-e2"));
  expect(screen.getByTestId("kind-absence")).toBeTruthy();
  expect(screen.queryByTestId("kind-blocked")).toBeNull();
});

test("a failed reload after a change is shown, not hidden behind the old list", async () => {
  const api = new FakeApi().on("DELETE", "/api/v1/constraints/e1", new Response(null, { status: 204 }));
  await show(api, [OPEN], [entry("e1")]);
  api.on("GET", ENTRIES, problem(503, "service.busy"));
  await fireEvent.press(screen.getByTestId("remove-e1"));
  expect(await screen.findByTestId("error-message")).toHaveTextContent(/Something went wrong/);
  expect(screen.getByTestId("retry")).toBeTruthy();
});
