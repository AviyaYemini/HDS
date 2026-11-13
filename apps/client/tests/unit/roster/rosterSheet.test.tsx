/**
 * The roster sheet on screen (specs/demo-ceo/plan.md, screen 2), at phone width a day at a time:
 * projects under their client, names as blocks with the "temporary" chip, empty places dashed, the
 * week's totals, and "copy last week"
 * as a dry run per editable project before anything is written. Made-up names only.
 */
import { fireEvent, screen, waitFor } from "@testing-library/react-native";
import * as Router from "expo-router";

import type { Me } from "../../../src/api/session";
import { setupI18n } from "../../../src/i18n";
import { RosterSheet } from "../../../src/screens/roster/RosterSheet";
import { FakeApi, ME, json, renderWith, signedIn } from "../fakeApi";

const router = Router as unknown as { __setSearchParams: (p: Record<string, string>) => void };

const ADMIN: Me = {
  ...(ME as Me),
  role: "admin",
  capabilities: ["roster.read", "roster.edit:project", "account.self"],
  managed_project_ids: [],
};

const WEEK = "2026-12-06";
const shift = (id: string, project: string, headcount: number, people: { user: string; name: string; temporary?: boolean }[]) => ({
  id,
  project_id: project,
  shift_type_id: "t",
  starts_at: "2026-12-07T05:00:00Z",
  ends_at: "2026-12-07T13:00:00Z",
  timezone: "Asia/Jerusalem",
  headcount,
  status: "draft",
  etag: "e",
  pay_rate: null,
  published_at: null,
  assignments: people.map((p, i) => ({ id: `${id}-${i}`, shift_id: id, user_id: p.user, full_name: p.name, starts_at: "2026-12-07T05:00:00Z", ends_at: "2026-12-07T13:00:00Z", locked: false, own_times: false, overrides: [], temporary: p.temporary ?? false })),
});

function api() {
  return new FakeApi()
    .on("GET", "/me", json(200, ADMIN))
    .on("GET", "/api/v1/projects", json(200, { items: [
      { id: "p1", name: "Harbour gate", client_id: "c1", status: "active", managers: [{ id: "m1", full_name: "Dana L." }] },
      { id: "p2", name: "City museum", client_id: "c2", status: "active" },
    ] }))
    .on("GET", "/api/v1/employees", json(200, { items: [{ id: "u2", full_name: "Omer Katz", employment_type: "temporary", engaged_from: "2026-11-13", engaged_until: "2027-02-12", project_ids: ["p1"] }] }))
    .on("GET", "/api/v1/clients", json(200, { items: [{ id: "c1", name: "Alpha Security" }, { id: "c2", name: "Beta Guarding" }] }))
    .on("GET", "/api/v1/shifts", json(200, { items: [shift("s1", "p1", 3, [{ user: "u1", name: "Dana Levi" }, { user: "u2", name: "Omer Katz", temporary: true }])], next_cursor: null }));
}

beforeEach(async () => {
  router.__setSearchParams({});
  await setupI18n("en");
});

test("on a phone, one day at a time: the day strip, then that day's projects under their clients", async () => {
  const fake = api();
  await renderWith(await signedIn(fake), <RosterSheet weekParam={WEEK} />);
  // The tests run at phone width: the week is a day strip, Sunday first, with no grid.
  expect(await screen.findByTestId("day-strip", {}, { timeout: 5000 })).toBeTruthy();
  expect(screen.queryByTestId("roster-sheet")).toBeNull();
  // The week's summary as small numbered figures.
  expect(screen.getByTestId("total-places")).toHaveTextContent(/01\s*Places\s*3/);
  expect(screen.getByTestId("total-filled")).toHaveTextContent(/02\s*Filled\s*2/);
  expect(screen.getByTestId("total-empty")).toHaveTextContent(/03\s*Empty\s*1/);
  // Sunday has no shifts; Monday has the harbour gate's.
  expect(screen.getByTestId("day-empty")).toBeTruthy();
  await fireEvent.press(screen.getByTestId("day-2026-12-07"));
  expect(await screen.findByText("Alpha Security")).toBeTruthy();
  expect(screen.queryByText("Beta Guarding")).toBeNull();
  expect(screen.getAllByTestId("day-card")).toHaveLength(1);
  // The task's manager under the project's name (the owner, 2026-11-13).
  expect(screen.getByTestId("sheet-managers")).toHaveTextContent("Task manager: Dana L.");
  expect(screen.getByText("Dana L.")).toBeTruthy();
  expect(screen.getAllByTestId("chip-temporary")).toHaveLength(1);
  expect(screen.getByTestId("sheet-empty")).toHaveTextContent("1 empty");
  // A name block opens a small card: the full name, the temporary chip with its end, the week's hours.
  await fireEvent.press(screen.getAllByTestId("sheet-person")[1]!);
  const card = await screen.findByTestId("person-card");
  expect(card).toHaveTextContent(/Omer Katz/);
  expect(card).toHaveTextContent(/Temporary · until 12\.02/);
  expect(card).toHaveTextContent(/8 h rostered this week/);
  await fireEvent.press(screen.getByTestId("person-card-scrim"));
  expect(screen.queryByTestId("person-card")).toBeNull();
  // Warnings are out of the app's scope (the owner, 2026-11-13): no strip, no count, no call.
  expect(screen.queryByTestId("warnings-strip")).toBeNull();
  expect(screen.getByTestId("sheet-totals")).not.toHaveTextContent(/warning/i);
  expect(fake.calls("GET", "/api/v1/roster/warnings")).toHaveLength(0);
  // The whole week across projects: no project in the query.
  const asked = new URLSearchParams(fake.calls("GET", "/api/v1/shifts")[0]!.query);
  expect(asked.get("from")).toBe(WEEK);
  expect(asked.get("to")).toBe("2026-12-12");
  expect(asked.has("project_id")).toBe(false);
});

test("copy last week runs a dry run per project first and writes only after the confirmation", async () => {
  const fake = api().on("POST", "/api/v1/projects/p1/copy-week", (seen) =>
    json(200, { dry_run: (seen.body as { dry_run: boolean }).dry_run, warnings: [], changes: [{ action: "create" }, { action: "skip" }] }),
  ).on("POST", "/api/v1/projects/p2/copy-week", json(200, { dry_run: true, warnings: [], changes: [] }));
  await renderWith(await signedIn(fake), <RosterSheet weekParam={WEEK} />);
  await fireEvent.press(await screen.findByTestId("action-copy-last-week", {}, { timeout: 5000 }));
  expect(await screen.findByTestId("copy-last-week-confirm")).toBeTruthy();
  const dry = fake.calls("POST", "/api/v1/projects/p1/copy-week");
  expect(dry).toHaveLength(1);
  expect(dry[0]!.body).toEqual({ from_week: "2026-11-29", to_week: WEEK, on_conflict: "unfilled", dry_run: true });
  await fireEvent.press(screen.getByTestId("action-confirm-copy-last-week"));
  await waitFor(() => expect(fake.calls("POST", "/api/v1/projects/p1/copy-week")).toHaveLength(2));
  expect(fake.calls("POST", "/api/v1/projects/p1/copy-week")[1]!.body).toMatchObject({ dry_run: false });
});
