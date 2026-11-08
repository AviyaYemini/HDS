/**
 * The admin screens on one table and form pattern (specs/007-ux-redesign FR-043 to FR-049): a list
 * from the API, sorting into the address, the record form's checks and error summary, a pay change
 * confirmed in words before it is saved, delete only for whoever may, and leaving unsaved asking.
 */
import { act, fireEvent, screen, waitFor } from "@testing-library/react-native";
import * as Router from "expo-router";

import type { Me } from "../../../src/api/session";
import { ClientsScreen, PaySettingsScreen, ProjectsScreen, StaffScreen, StaffingNeedsScreen } from "../../../src/screens/admin/AdminScreens";
import { ConfirmLeave, hasUnsaved, mayLeaveFor, resetUnsaved } from "../../../src/design/unsaved";
import { ToastHost } from "../../../src/design/components";
import { dropHeld } from "../../../src/design/heldActions";
import { setupI18n } from "../../../src/i18n";
import { FakeApi, ME, json, problem, renderWith, signedIn } from "../fakeApi";

const router = (Router as unknown as { __router: { setParams: jest.Mock; push: jest.Mock }; __setSearchParams: (p: Record<string, string>) => void });

const ADMIN: Me = {
  ...(ME as Me),
  role: "admin",
  capabilities: ["staff.manage", "staff.delete", "clients.manage", "clients.delete", "projects.manage", "projects.delete", "sites.manage", "pay_settings.edit", "account.self"],
  managed_project_ids: [],
};
const MANAGER: Me = { ...ADMIN, role: "manager", capabilities: ["staff.manage", "clients.manage", "projects.manage", "account.self"] };

const CLIENT = { id: "c1", organization_id: "o", name: "Acme", contract_start: "2026-01-01", contract_end: null, contact_name: "Rina", contact_email: null, contact_phone: null, is_active: true, created_at: "2026-01-01T00:00:00Z", updated_at: "2026-01-01T00:00:00Z" };
const PERSON = { id: "u1", organization_id: "o", full_name: "Dana Levi", email: "dana@hds.test", role: "employee", is_active: true, phone: null, default_hourly_rate: "48.0000", created_at: "2026-01-01T00:00:00Z", updated_at: "2026-01-01T00:00:00Z" };

async function open(me: Me, params: Record<string, string> = {}) {
  router.__setSearchParams(params);
  const api = new FakeApi().on("GET", "/me", json(200, me)).on("GET", "/me/saved-views", json(200, { items: [] }));
  const session = await signedIn(api);
  return { api, session };
}

beforeEach(async () => {
  resetUnsaved();
  // A removal an earlier test held must not be sent in this one.
  dropHeld();
  router.__router.setParams.mockClear();
  await setupI18n("en");
});

test("a list shows the records and Sort by sorts into the address", async () => {
  const { api, session } = await open(ADMIN);
  api.on("GET", "/api/v1/clients", json(200, { items: [CLIENT] }));
  await renderWith(session, <ClientsScreen />);
  expect(await screen.findByText("Acme", {}, { timeout: 5000 })).toBeTruthy();
  // At phone width (the tests' default) a list sorts through "Sort by" (R166).
  await fireEvent.press(screen.getByTestId("sort-by"));
  await fireEvent.press(await screen.findByTestId("sort-by-option-name", {}, { timeout: 5000 }));
  expect(router.__router.setParams).toHaveBeenCalledWith(expect.objectContaining({ sort: "name:asc" }));
});

test("a failed save shows the error summary and the field's error; fixing it clears both", async () => {
  const { api, session } = await open(ADMIN, { open: "new" });
  api.on("GET", "/api/v1/clients", json(200, { items: [] }));
  await renderWith(session, <ClientsScreen />);
  await fireEvent.press(await screen.findByTestId("action-save", {}, { timeout: 5000 }));
  expect(await screen.findByTestId("error-summary", {}, { timeout: 5000 })).toBeTruthy();
  expect(screen.getByTestId("field-error-name")).toBeTruthy();
  await fireEvent.changeText(screen.getByTestId("field-name"), "Fixed");
  await fireEvent(screen.getByTestId("field-name"), "blur");
  await waitFor(() => expect(screen.queryByTestId("field-error-name")).toBeNull());
  expect(api.calls("POST", "/api/v1/clients")).toHaveLength(0);
});

test("a pay change is confirmed in words, old and new, before anything is saved", async () => {
  const { api, session } = await open(ADMIN, { open: "u1" });
  api.on("GET", "/api/v1/employees", json(200, { items: [PERSON] })).on("PATCH", "/api/v1/employees/u1", json(200, { ...PERSON, default_hourly_rate: "49.00" }));
  await renderWith(session, <StaffScreen />);
  const rate = await screen.findByTestId("field-default_hourly_rate", {}, { timeout: 5000 });
  // Stored as 48.0000, shown as 48.00.
  expect(rate.props.value).toBe("48.00");
  await fireEvent.changeText(rate, "49.00");
  await waitFor(() => expect(hasUnsaved()).toBe(true));
  await fireEvent.press(screen.getByTestId("action-save"));
  const dialog = await screen.findByTestId("confirm-dialog", {}, { timeout: 5000 });
  expect(dialog).toHaveTextContent(/48\.00.*49\.00/);
  expect(api.calls("PATCH", "/api/v1/employees/u1")).toHaveLength(0);
  await fireEvent.press(screen.getByTestId("action-confirm-save"));
  await waitFor(() => expect(api.calls("PATCH", "/api/v1/employees/u1")).toHaveLength(1));
  expect(api.calls("PATCH", "/api/v1/employees/u1")[0]!.body).toMatchObject({ default_hourly_rate: "49.00" });
  // Saved: nothing is unsaved any more, so leaving no longer asks.
  await waitFor(() => expect(hasUnsaved()).toBe(false));
});

test("a removal waits out its undo time before the delete is sent", async () => {
  jest.useFakeTimers();
  try {
    const { api, session } = await open(ADMIN, { open: "c1" });
    api.on("GET", "/api/v1/clients", json(200, { items: [CLIENT] })).on("DELETE", "/api/v1/clients/c1", new Response(null, { status: 204 }));
    await renderWith(session, <ClientsScreen />);
    await act(async () => {
      await jest.advanceTimersByTimeAsync(100);
    });
    await fireEvent.press(screen.getByTestId("action-delete"));
    await fireEvent.press(screen.getByTestId("action-confirm-delete"));
    await act(async () => {
      await jest.advanceTimersByTimeAsync(1000);
    });
    expect(api.calls("DELETE", "/api/v1/clients/c1")).toHaveLength(0);
    await act(async () => {
      await jest.advanceTimersByTimeAsync(6000);
    });
    expect(api.calls("DELETE", "/api/v1/clients/c1")).toHaveLength(1);
  } finally {
    jest.useRealTimers();
  }
});

test("delete shows for an admin, never for a manager", async () => {
  for (const [me, shown] of [[ADMIN, true], [MANAGER, false]] as const) {
    const { api, session } = await open(me, { open: "u1" });
    api.on("GET", "/api/v1/employees", json(200, { items: [PERSON] }));
    const view = await renderWith(session, <StaffScreen />);
    await screen.findByTestId("field-full_name", {}, { timeout: 5000 });
    expect(screen.queryByTestId("action-delete") !== null).toBe(shown);
    await view.unmount();
  }
});

test("leaving with unsaved changes asks first, and leaving anyway goes", async () => {
  const { api, session } = await open(ADMIN, { open: "new" });
  api.on("GET", "/api/v1/clients", json(200, { items: [] }));
  await renderWith(
    session,
    <>
      <ClientsScreen />
      <ConfirmLeave />
    </>,
  );
  await fireEvent.changeText(await screen.findByTestId("field-name", {}, { timeout: 5000 }), "Half typed");
  let went = true;
  await act(async () => {
    went = mayLeaveFor("/home");
  });
  expect(went).toBe(false);
  expect(await screen.findByTestId("confirm-leave", {}, { timeout: 5000 })).toBeTruthy();
  await fireEvent.press(screen.getByTestId("action-leave"));
  expect(router.__router.push).toHaveBeenCalledWith("/home");
});

test("a project's managers are found by name, and only staff who manage can be added", async () => {
  const project = { id: "p1", organization_id: "o", name: "Night Watch", client_id: "c1", site_id: "s1", type: "security", status: "active", start_date: "2026-01-01", end_date: null, billing_rate: "75.0000", created_at: "x", updated_at: "x" };
  const { api, session } = await open(ADMIN, { open: "p1" });
  api
    .on("GET", "/api/v1/projects", json(200, { items: [project] }))
    .on("GET", "/api/v1/clients", json(200, { items: [CLIENT] }))
    .on("GET", "/api/v1/sites", json(200, { items: [{ id: "s1", name: "HQ", address: "1 Main", timezone: "Asia/Jerusalem", organization_id: "o", created_at: "x", updated_at: "x" }] }))
    .on("GET", "/api/v1/projects/p1/managers", json(200, { items: [] }))
    .on("GET", "/api/v1/employees", json(200, { items: [PERSON, { ...PERSON, id: "u2", full_name: "Avi Manager", email: "avi@hds.test", role: "manager" }] }))
    .on("PUT", "/api/v1/projects/p1/managers", json(200, { items: [{ id: "u2", full_name: "Avi Manager" }] }));
  await renderWith(session, <ProjectsScreen />);
  await fireEvent.changeText(await screen.findByTestId("searchable-picker", {}, { timeout: 5000 }), "a");
  const options = await screen.findAllByTestId("picker-option", {}, { timeout: 5000 });
  expect(options).toHaveLength(2);
  const adds = screen.getAllByTestId("action-add-manager");
  // The employee can't manage: their button is disabled, the manager's is not.
  await fireEvent.press(adds[1]!);
  await waitFor(() => expect(api.calls("PUT", "/api/v1/projects/p1/managers")).toHaveLength(1));
  expect(api.calls("PUT", "/api/v1/projects/p1/managers")[0]!.body).toEqual({ user_ids: ["u2"] });
});

test("pay settings and staffing needs read and save", async () => {
  const { api, session } = await open(ADMIN, { open: "p1" });
  api
    .on("GET", "/api/v1/organisation/pay-settings", json(200, { day_normal_minutes: 480, night_normal_minutes: 420, rest_pct: "150", rest_window: {} }))
    .on("PATCH", "/api/v1/organisation/pay-settings", json(200, {}));
  const view = await renderWith(session, <PaySettingsScreen />);
  expect((await screen.findByTestId("field-day_normal_minutes", {}, { timeout: 5000 })).props.value).toBe("480");
  await fireEvent.changeText(screen.getByTestId("field-day_normal_minutes"), "500");
  await fireEvent.press(screen.getByTestId("action-save"));
  await waitFor(() => expect(api.calls("PATCH", "/api/v1/organisation/pay-settings")).toHaveLength(1));
  await view.unmount();

  api
    .on("GET", "/api/v1/projects", json(200, { items: [{ id: "p1", name: "Night Watch", type: "security", status: "active" }] }))
    .on("GET", "/api/v1/projects/p1/shift-types", json(200, { items: [{ id: "t1", name: "morning" }] }))
    .on("GET", "/api/v1/projects/p1/requirements", json(200, { items: [{ shift_type_id: "t1", weekday: 0, headcount: 2 }] }))
    .on("PUT", "/api/v1/projects/p1/requirements", json(200, { items: [] }));
  await renderWith(session, <StaffingNeedsScreen />);
  expect((await screen.findByTestId("field-t1|0", {}, { timeout: 5000 })).props.value).toBe("2");
  await fireEvent.changeText(screen.getByTestId("field-t1|1"), "1");
  await fireEvent.press(screen.getByTestId("action-save"));
  await waitFor(() => expect(api.calls("PUT", "/api/v1/projects/p1/requirements")).toHaveLength(1));
  expect(api.calls("PUT", "/api/v1/projects/p1/requirements")[0]!.body).toEqual({
    items: [
      { shift_type_id: "t1", weekday: 0, headcount: 2 },
      { shift_type_id: "t1", weekday: 1, headcount: 1 },
    ],
  });
});

describe("the bulk-action bar (FR-043; the owner, PR 5)", () => {
  const second = { ...CLIENT, id: "c2", name: "Borealis" };
  const third = { ...CLIENT, id: "c3", name: "Cobalt" };

  async function selectAll(api: FakeApi, session: Awaited<ReturnType<typeof signedIn>>) {
    api.on("GET", "/api/v1/clients", json(200, { items: [CLIENT, second, third] }));
    await renderWith(
      session,
      <>
        <ClientsScreen />
        <ToastHost />
      </>,
    );
    await screen.findByText("Acme", {}, { timeout: 5000 });
    await fireEvent.press(screen.getByTestId("action-select-mode"));
    const rows = screen.getAllByTestId("table-row");
    await fireEvent.press(rows[0]!);
    expect(screen.getByTestId("bulk-count")).toHaveTextContent(/1 selected in Clients/);
    await fireEvent.press(screen.getByTestId("bulk-select-all"));
    expect(screen.getByTestId("bulk-count")).toHaveTextContent(/3 selected in Clients/);
  }

  test("a removal of several is one held action with one undo, then sent record by record", async () => {
    jest.useFakeTimers();
    try {
      const { api, session } = await open(ADMIN);
      for (const id of ["c1", "c2", "c3"]) api.on("DELETE", `/api/v1/clients/${id}`, new Response(null, { status: 204 }));
      await selectAll(api, session);
      await fireEvent.press(screen.getByTestId("bulk-remove"));
      expect(screen.queryAllByTestId("table-row")).toHaveLength(0);
      await act(async () => {
        await jest.advanceTimersByTimeAsync(5000);
      });
      expect(["c1", "c2", "c3"].flatMap((id) => api.calls("DELETE", `/api/v1/clients/${id}`))).toHaveLength(0);
      await act(async () => {
        await jest.advanceTimersByTimeAsync(2000);
      });
      expect(["c1", "c2", "c3"].map((id) => api.calls("DELETE", `/api/v1/clients/${id}`).length)).toEqual([1, 1, 1]);
    } finally {
      jest.useRealTimers();
    }
  });

  test("a failure comes back with a summary, and the record stays", async () => {
    jest.useFakeTimers();
    try {
      const { api, session } = await open(ADMIN);
      api.on("DELETE", "/api/v1/clients/c1", new Response(null, { status: 204 }));
      api.on("DELETE", "/api/v1/clients/c2", problem(409, "clients.has_projects"));
      api.on("DELETE", "/api/v1/clients/c3", new Response(null, { status: 204 }));
      await selectAll(api, session);
      await fireEvent.press(screen.getByTestId("bulk-remove"));
      await act(async () => {
        await jest.advanceTimersByTimeAsync(7000);
      });
      expect(await screen.findByText(/2 of 3 removed; 1 could not be/, {}, { timeout: 5000 })).toBeTruthy();
    } finally {
      jest.useRealTimers();
    }
  });

  test("deactivating several goes through each record's own change", async () => {
    const { api, session } = await open(ADMIN);
    for (const id of ["c1", "c2", "c3"]) api.on("PATCH", `/api/v1/clients/${id}`, json(200, {}));
    await selectAll(api, session);
    await fireEvent.press(screen.getByTestId("bulk-deactivate"));
    await waitFor(() => expect(["c1", "c2", "c3"].map((id) => api.calls("PATCH", `/api/v1/clients/${id}`).length)).toEqual([1, 1, 1]));
    expect(api.calls("PATCH", "/api/v1/clients/c1")[0]!.body).toEqual({ is_active: false });
  });
});
