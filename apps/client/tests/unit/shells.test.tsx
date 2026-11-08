/**
 * The shells' destinations per capability list (specs/007-ux-redesign FR-001, FR-003 to FR-005,
 * task T045): the exact tab set and side-navigation set each role gets, drawn from capabilities
 * alone, previews marked and dropped when switched off, and the shell's everywhere controls.
 */
import { fireEvent, screen } from "@testing-library/react-native";
import { Text } from "react-native";

import { capabilitiesOf, type Capability } from "../../src/capabilities";
import type { Me } from "../../src/api/session";
import { Shell } from "../../src/navigation/Shell";
import {
  CLIENT_DESTINATIONS,
  STAFF_DESTINATIONS,
  destinationsFor,
  groupsFor,
  homeFor,
  isActiveTab,
  shellFor,
  tabsFor,
} from "../../src/navigation/destinations";
import { FakeApi, json, renderWith, signedIn } from "./fakeApi";

// The previews are on, as in development and the shared test environment (FR-069).
jest.mock("../../src/config/previews", () => ({ previewsEnabled: true }));

const EMPLOYEE: Capability[] = ["my_work.read", "my_work.availability", "account.self"];
const MANAGER: Capability[] = [
  ...EMPLOYEE,
  "roster.read",
  "roster.edit:project",
  "roster.publish:project",
  "roster.override:project",
  "schedule.generate",
  "availability.read_others",
  "reports.read",
  "pay.read_others",
  "staff.manage",
  "clients.manage",
  "sites.manage",
  "projects.manage",
  "projects.read",
  "client_requests.answer",
];
const ADMIN: Capability[] = [
  ...MANAGER,
  "availability.edit_others",
  "pay.edit_shift",
  "pay_settings.edit",
  "staff.delete",
  "clients.delete",
  "projects.delete",
  "special_days.edit",
  "organisation.contact.edit",
];
const CLIENT: Capability[] = ["client_portal.read", "client_portal.requests", "account.self"];

function me(role: Me["role"], capabilities: Capability[]): Me {
  return {
    id: "u",
    organization_id: "o",
    email: `${role}@hds.test`,
    full_name: `${role} user`,
    role,
    is_active: true,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
    capabilities,
    managed_project_ids: [],
  };
}

const keys = (list: { key: string; preview?: boolean }[]) => list.filter((item) => !item.preview).map((item) => item.key).sort();

describe("destinations by capability (FR-001)", () => {
  it("gives each role exactly the contract's side-navigation set", () => {
    expect(keys(destinationsFor(capabilitiesOf(me("employee", EMPLOYEE))))).toEqual(["availability", "home", "my-shifts", "pay"]);
    expect(keys(destinationsFor(capabilitiesOf(me("manager", MANAGER))))).toEqual(
      ["availability", "home", "my-shifts", "pay", "roster", "submissions", "runs", "work-log", "hours", "staff", "clients", "sites", "projects", "shift-templates", "staffing-needs", "pay-settings"].sort(),
    );
    expect(keys(destinationsFor(capabilitiesOf(me("admin", ADMIN))))).toEqual(
      ["availability", "home", "my-shifts", "pay", "roster", "submissions", "runs", "work-log", "hours", "staff", "clients", "sites", "projects", "shift-templates", "staffing-needs", "pay-settings", "special-days"].sort(),
    );
    // The client's Billing and Requests are previews of 009 (PR 2 review): marked, and gone without previews.
    expect(keys(destinationsFor(capabilitiesOf(me("client", CLIENT))))).toEqual(["client-home", "contact"]);
    expect(destinationsFor(capabilitiesOf(me("client", CLIENT))).filter((item) => item.preview).map((item) => item.key)).toEqual(["billing", "requests"]);
  });

  it("gives each role the contract's bottom tabs, never more than five", () => {
    expect(tabsFor(capabilitiesOf(me("employee", EMPLOYEE))).map((tab) => tab.key)).toEqual(["home", "availability", "my-shifts", "pay", "more"]);
    expect(tabsFor(capabilitiesOf(me("manager", MANAGER))).map((tab) => tab.key)).toEqual(["home", "roster", "my-work", "reports", "more"]);
    expect(tabsFor(capabilitiesOf(me("admin", ADMIN))).map((tab) => tab.key)).toEqual(["home", "roster", "my-work", "reports", "more"]);
    expect(tabsFor(capabilitiesOf(me("client", CLIENT))).map((tab) => tab.key)).toEqual(["projects", "billing", "requests", "contact"]);
    expect(tabsFor(capabilitiesOf(me("client", CLIENT))).filter((tab) => tab.preview).map((tab) => tab.key)).toEqual(["billing", "requests"]);
    for (const list of [EMPLOYEE, MANAGER, ADMIN, CLIENT]) expect(tabsFor(capabilitiesOf(me("employee", list))).length).toBeLessThanOrEqual(5);
  });

  it("never offers a client a staff destination, nor staff the client portal", () => {
    const client = destinationsFor(capabilitiesOf(me("client", CLIENT)));
    expect(client.some((item) => STAFF_DESTINATIONS.includes(item))).toBe(false);
    const admin = destinationsFor(capabilitiesOf(me("admin", ADMIN)));
    expect(admin.some((item) => CLIENT_DESTINATIONS.includes(item))).toBe(false);
    expect(shellFor(capabilitiesOf(me("client", CLIENT)))).toBe("client");
    expect(homeFor(capabilitiesOf(me("client", CLIENT)))).toBe("/client");
    expect(homeFor(capabilitiesOf(me("employee", EMPLOYEE)))).toBe("/home");
    expect(shellFor(capabilitiesOf(null))).toBe("none");
  });

  it("groups the staff navigation and marks previews", () => {
    const groups = groupsFor(capabilitiesOf(me("admin", ADMIN)));
    expect(groups.map((group) => group.group)).toEqual(["my-work", "manage", "admin"]);
    const previews = destinationsFor(capabilitiesOf(me("admin", ADMIN))).filter((item) => item.preview);
    expect(previews.map((item) => item.key).sort()).toEqual(["approvals", "audit-log", "manage-requests", "overview", "requests"]);
    expect(groupsFor(capabilitiesOf(me("employee", EMPLOYEE))).map((group) => group.group)).toEqual(["my-work"]);
  });

  it("lights the tab for the address", () => {
    const tabs = tabsFor(capabilitiesOf(me("manager", MANAGER)));
    expect(tabs.filter((tab) => isActiveTab(tab, "/roster/submissions")).map((tab) => tab.key)).toEqual(["roster"]);
    expect(tabs.filter((tab) => isActiveTab(tab, "/account")).map((tab) => tab.key)).toEqual(["more"]);
    expect(tabs.filter((tab) => isActiveTab(tab, "/my/pay")).map((tab) => tab.key)).toEqual(["my-work"]);
  });
});

describe("the shell (FR-005, FR-009)", () => {
  it("draws the tabs and the everywhere controls for a signed-in employee", async () => {
    const api = new FakeApi().on("GET", "/me", json(200, me("employee", EMPLOYEE)));
    const session = await signedIn(api);
    await renderWith(
      session,
      <Shell>
        <Text>page</Text>
      </Shell>,
    );
    expect(screen.getByTestId("nav-tabs")).toBeTruthy();
    for (const key of ["home", "availability", "my-shifts", "pay", "more"]) expect(screen.getByTestId(`tab-${key}`)).toBeTruthy();
    for (const id of ["action-sign-out", "link-help", "link-contact", "link-accessibility", "link-account"]) expect(screen.getByTestId(id)).toBeTruthy();
    expect(screen.getByText("page")).toBeTruthy();
  });

  it("signs out from the header and ends the session", async () => {
    const api = new FakeApi().on("GET", "/me", json(200, me("client", CLIENT))).on("POST", "/auth/logout", new Response(null, { status: 204 }));
    const session = await signedIn(api);
    await renderWith(
      session,
      <Shell>
        <Text>page</Text>
      </Shell>,
    );
    for (const key of ["projects", "billing", "requests", "contact"]) expect(screen.getByTestId(`tab-${key}`)).toBeTruthy();
    await fireEvent.press(screen.getByTestId("action-sign-out"));
    await screen.findByText("page");
    expect(api.calls("POST", "/auth/logout").length).toBe(1);
  });
});
