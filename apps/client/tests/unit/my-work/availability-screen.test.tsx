/**
 * The availability week on screen (specs/007-ux-redesign FR-013, FR-014, FR-018; task T060): a tap
 * cycles a cell and the API follows, the banner shows the window's status and turns to draft on a
 * change, Undo puts the cell back, and a locked window cannot be changed.
 */
import { fireEvent, screen, waitFor } from "@testing-library/react-native";
import * as Router from "expo-router";

import type { Me } from "../../../src/api/session";
import { AvailabilityScreen } from "../../../src/screens/my-work/availability/AvailabilityScreen";
import { FakeApi, ME, json, renderWith, signedIn } from "../fakeApi";

const EMPLOYEE: Me = { ...(ME as Me), id: "me", capabilities: ["my_work.read", "my_work.availability", "account.self"], managed_project_ids: [] };
const setSearchParams = (Router as unknown as { __setSearchParams: (params: Record<string, string>) => void }).__setSearchParams;

const WINDOW = { id: "w1", starts_on: "2026-11-15", ends_on: "2026-11-21", deadline: "2026-11-13T23:30:00Z", is_open: true };
const TYPES = { items: [{ id: "morning", name: "Morning", starts_at_local: "06:00:00", ends_at_local: "14:00:00", crosses_midnight: false, is_active: true, created_at: "", updated_at: "" }] };

/** An API whose entries follow the calls made to it, as the server's would. */
function availabilityApi(status: "not_submitted" | "draft" | "submitted" | "locked", counted = false) {
  const entries: { id: string; kind: string; shift_type_id: string | null; starts_on: string; ends_on: string }[] = [];
  let next = 1;
  let current = status;
  const api = new FakeApi()
    .on("GET", "/me", json(200, EMPLOYEE))
    .on("GET", "/api/v1/availability-windows", json(200, { items: [WINDOW] }))
    .on("GET", "/api/v1/shift-types", json(200, TYPES))
    .on("GET", "/api/v1/employees/me/constraints", () => json(200, { items: entries.map((e) => ({ ...e, user_id: "me", weekday: null, absence_kind: null, created_at: "", updated_at: "" })) }))
    .on("GET", "/me/availability-submissions", () => json(200, { window_id: "w1", status: current, submitted_at: null, submitted_by: null, deadline: WINDOW.deadline, counted_unavailable: counted }))
    .on("POST", "/api/v1/employees/me/constraints", (seen) => {
      const body = seen.body as { kind: string; shift_type_id: string | null; starts_on: string; ends_on: string };
      const entry = { id: `e${next++}`, ...body };
      entries.push(entry);
      if (current !== "locked") current = "draft";
      return json(201, { ...entry, user_id: "me", weekday: null, absence_kind: null, created_at: "", updated_at: "" });
    })
    .on("POST", "/me/availability-submissions", () => {
      current = "submitted";
      return json(200, { window_id: "w1", status: "submitted", submitted_at: "2026-11-10T09:00:00Z", submitted_by: "me", deadline: WINDOW.deadline, counted_unavailable: false });
    });
  const original = api.fetch;
  // PATCH and DELETE carry the entry's id in the path, so they are answered here by hand.
  const fetch = async (request: Request): Promise<Response> => {
    const url = new URL(request.url);
    const match = /^\/api\/v1\/constraints\/(.+)$/.exec(url.pathname);
    if (match && request.method === "PATCH") {
      const body = (await request.clone().json()) as { kind: string };
      const entry = entries.find((e) => e.id === match[1]);
      if (entry) entry.kind = body.kind;
      if (current !== "locked") current = "draft";
      return json(200, { ...entry, user_id: "me", weekday: null, absence_kind: null, created_at: "", updated_at: "" });
    }
    if (match && request.method === "DELETE") {
      const index = entries.findIndex((e) => e.id === match[1]);
      if (index >= 0) entries.splice(index, 1);
      if (current !== "locked") current = "draft";
      return new Response(null, { status: 204 });
    }
    return original(request);
  };
  return { api: Object.assign(api, { fetch }), entries };
}

const tuesdayMorning = () => screen.getAllByTestId("availability-cell").find((cell) => cell.props.accessibilityLabel.includes("17/11/2026") && cell.props.accessibilityLabel.includes("Morning"))!;

describe("AvailabilityScreen", () => {
  beforeEach(() => setSearchParams({ window: "w1" }));
  afterEach(() => setSearchParams({}));

  it("cycles a cell through the states and the banner turns to draft (FR-013, FR-014)", async () => {
    const { api, entries } = availabilityApi("not_submitted");
    await renderWith(await signedIn(api), <AvailabilityScreen />);
    const banner = await screen.findByTestId("availability-banner");
    expect(banner.props.dataSet?.status ?? "not_submitted").toBe("not_submitted");
    expect(screen.getByTestId("availability-not-submitted-warning")).toBeTruthy();
    expect(screen.getByTestId("availability-rules")).toBeTruthy();
    // The deadline reads at the site: 23:30 UTC is 01:30 the next day in Asia/Jerusalem (R53).
    expect(banner).toHaveTextContent(/14\/11\/2026/);
    expect(banner).toHaveTextContent(/01:30/);

    expect(tuesdayMorning().props.accessibilityLabel).toContain("Available");
    await fireEvent.press(tuesdayMorning());
    await waitFor(() => expect(tuesdayMorning().props.accessibilityLabel).toContain("Would like to work"));
    await waitFor(() => expect(entries).toHaveLength(1));
    expect(entries[0]).toMatchObject({ kind: "open", shift_type_id: "morning", starts_on: "2026-11-17" });
    await waitFor(() => expect(screen.getByText("Draft")).toBeTruthy());

    await fireEvent.press(tuesdayMorning());
    await waitFor(() => expect(entries[0]?.kind).toBe("close"));
    await fireEvent.press(tuesdayMorning());
    await waitFor(() => expect(entries[0]?.kind).toBe("blocked"));
    await fireEvent.press(tuesdayMorning());
    await waitFor(() => expect(entries).toHaveLength(0));
    await waitFor(() => expect(tuesdayMorning().props.accessibilityLabel).toContain("Available"));
  });

  it("undoes the last change (FR-018)", async () => {
    const { api, entries } = availabilityApi("not_submitted");
    await renderWith(await signedIn(api), <AvailabilityScreen />);
    await screen.findByTestId("availability-banner");
    await fireEvent.press(tuesdayMorning());
    await waitFor(() => expect(entries[0]?.kind).toBe("open"));
    await fireEvent.press(tuesdayMorning());
    await waitFor(() => expect(entries[0]?.kind).toBe("close"));
    await fireEvent.press(await screen.findByTestId("action-undo"));
    await waitFor(() => expect(entries[0]?.kind).toBe("open"));
    await waitFor(() => expect(tuesdayMorning().props.accessibilityLabel).toContain("Would like to work"));
  });

  it("reviews and submits, and a change afterwards is a draft again (FR-014, FR-015)", async () => {
    const { api } = availabilityApi("not_submitted");
    await renderWith(await signedIn(api), <AvailabilityScreen />);
    await screen.findByTestId("availability-banner");
    await fireEvent.press(tuesdayMorning());
    await waitFor(() => expect(api.calls("POST", "/api/v1/employees/me/constraints")).toHaveLength(1));
    await fireEvent.press(screen.getByTestId("action-review"));
    expect(await screen.findAllByTestId("review-row", { includeHiddenElements: true })).toHaveLength(1);
    await fireEvent.press(screen.getByTestId("action-submit", { includeHiddenElements: true }));
    await waitFor(() => expect(api.calls("POST", "/me/availability-submissions")).toHaveLength(1));
    await waitFor(() => expect(screen.getByText("Submitted")).toBeTruthy());
    await fireEvent.press(tuesdayMorning());
    await waitFor(() => expect(screen.getByText("Draft")).toBeTruthy());
  });

  it("locks the week after the deadline and says when it counts as unavailable", async () => {
    const { api } = availabilityApi("locked", true);
    await renderWith(await signedIn(api), <AvailabilityScreen />);
    const banner = await screen.findByTestId("availability-banner");
    expect(banner).toHaveTextContent(/Locked/);
    expect(screen.getByText(/counts? as unavailable/)).toBeTruthy();
    expect(tuesdayMorning()).toBeDisabled();
    expect(screen.queryByTestId("action-review")).toBeNull();
    expect(screen.queryByTestId("availability-not-submitted-warning")).toBeNull();
  });
});

describe("AvailabilityScreen, changes that land right (R55, R57)", () => {
  beforeEach(() => setSearchParams({ window: "w1" }));
  afterEach(() => setSearchParams({}));

  it("clears an inherited all-day mark through the all-day entry and Undo brings that entry back", async () => {
    const { api, entries } = availabilityApi("draft");
    entries.push({ id: "a1", kind: "open", shift_type_id: null, starts_on: "2026-11-17", ends_on: "2026-11-17" });
    await renderWith(await signedIn(api), <AvailabilityScreen />);
    await screen.findByTestId("availability-banner");
    expect(tuesdayMorning().props.accessibilityLabel).toContain("Would like to work");
    // open → close on an inherited cell changes the all-day entry itself.
    await fireEvent.press(tuesdayMorning());
    await waitFor(() => expect(entries).toEqual([expect.objectContaining({ id: "a1", kind: "close", shift_type_id: null })]));
    await fireEvent.press(tuesdayMorning());
    await fireEvent.press(tuesdayMorning());
    await waitFor(() => expect(entries).toHaveLength(0));
    await fireEvent.press(await screen.findByTestId("action-undo"));
    await waitFor(() => expect(entries).toHaveLength(1));
    expect(entries[0]).toMatchObject({ kind: "blocked", shift_type_id: null, starts_on: "2026-11-17" });
  });

  it("copies last week in one go and reloads once at the end", async () => {
    const { api, entries } = availabilityApi("not_submitted");
    entries.push(
      { id: "p1", kind: "open", shift_type_id: null, starts_on: "2026-11-09", ends_on: "2026-11-09" },
      { id: "p2", kind: "blocked", shift_type_id: "morning", starts_on: "2026-11-11", ends_on: "2026-11-11" },
    );
    await renderWith(await signedIn(api), <AvailabilityScreen />);
    await screen.findByTestId("availability-banner");
    const loadsBefore = api.calls("GET", "/api/v1/employees/me/constraints").length;
    await fireEvent.press(screen.getByTestId("action-copy-last-week"));
    await waitFor(() => expect(api.calls("POST", "/api/v1/employees/me/constraints")).toHaveLength(2));
    await waitFor(() => expect(api.calls("GET", "/api/v1/employees/me/constraints").length).toBe(loadsBefore + 1));
    expect(entries.map((e) => `${e.starts_on}:${e.shift_type_id ?? "all"}:${e.kind}`).sort()).toEqual(["2026-11-09:all:open", "2026-11-11:morning:blocked", "2026-11-16:all:open", "2026-11-18:morning:blocked"]);
  });
});
