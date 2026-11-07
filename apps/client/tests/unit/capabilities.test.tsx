/**
 * The client capability layer (specs/007-ux-redesign FR-001, FR-007, T032): capabilities decide
 * what is drawn, a scoped one holds only on managed projects (every project for an Admin), and a
 * 403 reads the account again so a changed role redraws at once.
 */
import { act, screen, waitFor } from "@testing-library/react-native";
import { Text } from "react-native";

import { Gate, RequireCapability, capabilitiesOf, useCapabilities } from "../../src/capabilities";
import type { Me } from "../../src/api/session";
import { FakeApi, ME, json, problem, renderWith, signedIn } from "./fakeApi";

const MANAGED = "33333333-3333-4333-8333-333333333333";
const OTHER = "44444444-4444-4444-8444-444444444444";

function me(role: Me["role"], capabilities: Me["capabilities"], managed: string[] = []): Me {
  return { ...ME, role, capabilities, managed_project_ids: managed } as Me;
}

describe("capabilitiesOf", () => {
  it("holds exactly the names served", () => {
    const found = capabilitiesOf(me("employee", ["my_work.read", "account.self"]));
    expect(found.has("my_work.read")).toBe(true);
    expect(found.has("roster.read")).toBe(false);
  });

  it("gives a Manager a scoped capability only on the projects they manage", () => {
    const found = capabilitiesOf(me("manager", ["roster.read", "roster.edit:project"], [MANAGED]));
    expect(found.has("roster.edit:project")).toBe(true);
    expect(found.hasOnProject("roster.edit:project", MANAGED)).toBe(true);
    expect(found.hasOnProject("roster.edit:project", OTHER)).toBe(false);
    // An unscoped capability holds on every project.
    expect(found.hasOnProject("roster.read", OTHER)).toBe(true);
  });

  it("gives an Admin a scoped capability on every project", () => {
    const found = capabilitiesOf(me("admin", ["roster.edit:project"]));
    expect(found.hasOnProject("roster.edit:project", OTHER)).toBe(true);
  });

  it("gives nothing when the capability is not held, scoped or not", () => {
    const found = capabilitiesOf(me("manager", ["roster.read"], [MANAGED]));
    expect(found.hasOnProject("roster.edit:project", MANAGED)).toBe(false);
  });

  it("gives nothing when signed out", () => {
    const found = capabilitiesOf(null);
    expect(found.names.size).toBe(0);
    expect(found.has("account.self")).toBe(false);
  });
});

function Probe() {
  const { has } = useCapabilities();
  return <Text testID="probe">{has("roster.read") ? "roster" : "no roster"}</Text>;
}

describe("Gate and RequireCapability", () => {
  it("draw their children only when the capability holds", async () => {
    const api = new FakeApi().on("GET", "/me", json(200, me("manager", ["roster.read", "roster.edit:project"], [MANAGED])));
    const session = await signedIn(api);
    await renderWith(
      session,
      <>
        <Gate need="roster.read">
          <Text>roster</Text>
        </Gate>
        <Gate need="staff.delete" fallback={<Text>no delete</Text>}>
          <Text>delete</Text>
        </Gate>
        <Gate need="roster.edit:project" project={OTHER} fallback={<Text>read only</Text>}>
          <Text>edit other</Text>
        </Gate>
        <RequireCapability need="roster.edit:project" project={MANAGED} denied={<Text>denied</Text>}>
          <Text>edit managed</Text>
        </RequireCapability>
        <RequireCapability need="pay_settings.edit" denied={<Text>no access</Text>}>
          <Text>pay settings</Text>
        </RequireCapability>
      </>,
    );
    expect(screen.getByText("roster")).toBeTruthy();
    expect(screen.getByText("no delete")).toBeTruthy();
    expect(screen.queryByText("delete")).toBeNull();
    expect(screen.getByText("read only")).toBeTruthy();
    expect(screen.getByText("edit managed")).toBeTruthy();
    expect(screen.getByText("no access")).toBeTruthy();
    expect(screen.queryByText("pay settings")).toBeNull();
  });
});

describe("after a 403", () => {
  it("reads the account again and redraws with the new capabilities", async () => {
    const api = new FakeApi().on(
      "GET",
      "/me",
      json(200, me("manager", ["roster.read"])),
      json(200, me("employee", ["my_work.read"])),
    );
    api.on("GET", "/api/v1/projects", problem(403, "users.manager_required"));
    const session = await signedIn(api);
    await renderWith(session, <Probe />);
    expect(screen.getByTestId("probe")).toHaveTextContent("roster");

    await act(async () => {
      await session.api.GET("/api/v1/projects");
    });
    await waitFor(() => expect(screen.getByTestId("probe")).toHaveTextContent("no roster"));
    expect(api.calls("GET", "/me")).toHaveLength(2);
  });

  it("does not read the account again when /me itself was refused", async () => {
    const api = new FakeApi().on("GET", "/me", json(200, me("employee", ["my_work.read"])));
    const session = await signedIn(api);
    api.on("GET", "/me", problem(403, "users.forbidden"));
    await session.api.GET("/me");
    await Promise.resolve();
    expect(api.calls("GET", "/me")).toHaveLength(2);
  });

  it("shares one read between refusals that arrive together, and keeps the account when it fails", async () => {
    const api = new FakeApi().on("GET", "/me", json(200, me("manager", ["roster.read"])));
    api.on("GET", "/api/v1/projects", problem(403, "users.manager_required"));
    const session = await signedIn(api);
    api.on("GET", "/me", problem(500, "errors.unknown"));
    const first = session.reloadMe();
    expect(session.reloadMe()).toBe(first);
    await first;
    expect(api.calls("GET", "/me")).toHaveLength(2);
    expect(session.user?.capabilities).toEqual(["roster.read"]);
    await session.api.GET("/api/v1/projects");
    await waitFor(() => expect(api.calls("GET", "/me")).toHaveLength(3));
    expect(session.user?.capabilities).toEqual(["roster.read"]);
  });
});
