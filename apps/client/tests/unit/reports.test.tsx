// Reports (005 US4, US5): who worked at a project, and hours with pay only where it is sent.
import { fireEvent, screen } from "@testing-library/react-native";

import { HoursReportScreen } from "../../src/screens/HoursReportScreen";
import { WorkLogScreen } from "../../src/screens/WorkLogScreen";
import { FakeApi, json, problem, renderWith, signedIn } from "./fakeApi";

const PROJECTS = {
  items: [
    { id: "p1", name: "Gate", start_date: "2026-10-01", status: "active" },
    { id: "p2", name: "Tower", start_date: "2026-09-01", status: "active" },
  ],
  next_cursor: null,
  total: 2,
};

test("the work-log asks for the chosen project and lists who worked there", async () => {
  const api = new FakeApi()
    .on("GET", "/api/v1/projects", json(200, PROJECTS))
    .on(
      "GET",
      "/api/v1/reports/work-log",
      json(200, {
        from: "2026-10-01",
        to: "2026-11-05",
        rows: [
          {
            user_id: "u1",
            full_name: "Dana Levi",
            project_id: "p1",
            project_name: "Gate",
            shifts: 2,
            hours: "16.00",
            first_date: "2026-10-05",
            last_date: "2026-10-06",
          },
        ],
      }),
    );
  await renderWith(await signedIn(api), <WorkLogScreen />);

  await fireEvent.press(await screen.findByTestId("project-p1"));
  const row = await screen.findByTestId("log-row-u1-p1");

  expect(api.calls("GET", "/api/v1/reports/work-log")[0]!.query).toBe("?project_id=p1");
  expect(row).toHaveTextContent(/Dana Levi/);
  expect(row).toHaveTextContent(/2 shifts/);
  expect(row).toHaveTextContent(/16\.00 h/);
  expect(screen.getByTestId("log-range")).toHaveTextContent(/Oct 1/);
});

test("a site's work-log asks for a start date first", async () => {
  const api = new FakeApi()
    .on("GET", "/api/v1/projects", json(200, PROJECTS))
    .on("GET", "/api/v1/sites", json(200, { items: [{ id: "s1", name: "HQ" }], total: 1 }))
    .on("GET", "/api/v1/reports/work-log", json(200, { from: "2026-10-01", to: "2026-10-31", rows: [] }));
  await renderWith(await signedIn(api), <WorkLogScreen />);

  await fireEvent.press(await screen.findByTestId("kind-site"));
  await fireEvent.press(await screen.findByTestId("site-s1"));
  expect(await screen.findByText("Give a start date.")).toBeTruthy();
  expect(api.calls("GET", "/api/v1/reports/work-log")).toHaveLength(0);

  await fireEvent.changeText(screen.getByTestId("log-from"), "2026-10-01");
  await fireEvent.press(screen.getByTestId("log-apply"));
  await screen.findByText("Nobody worked here in these dates.");
  expect(api.calls("GET", "/api/v1/reports/work-log")[0]!.query).toBe("?site_id=s1&from=2026-10-01");
});

test("a failed project list is shown and can be tried again", async () => {
  const api = new FakeApi().on("GET", "/api/v1/projects", problem(500, "unknown.code"), json(200, PROJECTS));
  await renderWith(await signedIn(api), <WorkLogScreen />);

  expect(await screen.findByTestId("error-message")).toHaveTextContent("Something went wrong. Please try again.");
  await fireEvent.press(screen.getByTestId("retry-choices"));
  expect(await screen.findByTestId("project-p1")).toBeTruthy();
});

test("the work-log says so when nobody worked in the range", async () => {
  const api = new FakeApi()
    .on("GET", "/api/v1/projects", json(200, PROJECTS))
    .on("GET", "/api/v1/reports/work-log", json(200, { from: "2026-09-01", to: "2026-11-05", rows: [] }));
  await renderWith(await signedIn(api), <WorkLogScreen />);

  await fireEvent.press(await screen.findByTestId("project-p2"));

  expect(await screen.findByText("Nobody worked here in these dates.")).toBeTruthy();
});

const HOURS = {
  from: "2026-10-01",
  to: "2026-10-31",
  group_by: "project",
  groups: [
    {
      id: "p1",
      name: "Gate",
      hours: "16.00",
      shifts: 2,
      shifts_missing_rate: 0,
      pay: "800.00",
      rows: [{ id: "u1", name: "Dana Levi", hours: "16.00", shifts: 2, shifts_missing_rate: 0, pay: "800.00" }],
    },
    {
      id: "p2",
      name: "Tower",
      hours: "8.00",
      shifts: 1,
      shifts_missing_rate: 0,
      rows: [{ id: "u1", name: "Dana Levi", hours: "8.00", shifts: 1, shifts_missing_rate: 0 }],
    },
  ],
  hours: "24.00",
  shifts: 3,
  shifts_missing_rate: 2,
  pay: "800.00",
};

test("the hours report shows pay only where the answer has it", async () => {
  jest.useFakeTimers({ now: new Date("2026-10-20T10:00:00"), doNotFake: ["nextTick", "setImmediate", "queueMicrotask", "setTimeout", "setInterval"] });
  try {
    const api = new FakeApi().on("GET", "/api/v1/reports/hours", json(200, HOURS));
    await renderWith(await signedIn(api), <HoursReportScreen />);

    const gate = await screen.findByTestId("hours-group-p1");
    expect(api.calls("GET", "/api/v1/reports/hours")[0]!.query).toBe(
      "?from=2026-10-01&to=2026-10-31&group_by=employee",
    );
    expect(gate).toHaveTextContent(/800\.00/);
    expect(screen.getByTestId("hours-group-p2")).not.toHaveTextContent(/Pay/);
    expect(screen.getByTestId("hours-total")).toHaveTextContent(/24\.00 h/);
    expect(screen.getByTestId("hours-missing")).toHaveTextContent(/2 shifts have no pay rate yet/);
    expect(gate).toHaveTextContent(/2 shifts/);
  } finally {
    jest.useRealTimers();
  }
});

test("the hours report can be grouped by project, never leaving the old grouping on screen", async () => {
  let release: (() => void) | undefined;
  const slow = new Promise<void>((resolve) => (release = resolve));
  const api = new FakeApi().on("GET", "/api/v1/reports/hours", async (seen) => {
    if (seen.query.endsWith("group_by=project")) await slow;
    return json(200, HOURS);
  });
  await renderWith(await signedIn(api), <HoursReportScreen />);
  await screen.findByTestId("hours-group-p1");

  await fireEvent.press(screen.getByTestId("group-by-project"));
  expect(await screen.findByText("Loading…")).toBeTruthy();
  expect(screen.queryByTestId("hours-group-p1")).toBeNull();
  release!();
  await screen.findByTestId("hours-group-p1");

  expect(api.calls("GET", "/api/v1/reports/hours").at(-1)!.query).toMatch(/group_by=project$/);
});
