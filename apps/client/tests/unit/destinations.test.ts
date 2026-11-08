/**
 * Destinations and tabs with the previews off, as a production build is (specs/007-ux-redesign
 * FR-068; PR 2 review): no role is offered a preview, so a client sees Projects and Contact only,
 * on the desktop and on a phone, and every tab goes through the same rule as a destination.
 */
import { capabilitiesOf, type Capability } from "../../src/capabilities";
import { destinationsFor, tabsFor } from "../../src/navigation/destinations";

const CLIENT: Capability[] = ["client_portal.read", "client_portal.requests", "account.self"];
const EMPLOYEE: Capability[] = ["my_work.read", "my_work.availability", "account.self"];
const MANAGER: Capability[] = [...EMPLOYEE, "roster.read", "schedule.generate", "availability.read_others", "reports.read", "staff.manage", "clients.manage", "sites.manage", "projects.manage", "pay_settings.edit", "client_requests.answer"];

const caps = (list: Capability[]) => capabilitiesOf({ capabilities: list } as never);

describe("with previews off (the Jest build)", () => {
  it("offers a client Projects and Contact only, on both layouts", () => {
    expect(tabsFor(caps(CLIENT)).map((tab) => tab.key)).toEqual(["projects", "contact"]);
    expect(destinationsFor(caps(CLIENT)).map((item) => item.key)).toEqual(["client-home", "contact"]);
  });

  it("offers no preview to anyone", () => {
    for (const list of [CLIENT, EMPLOYEE, MANAGER]) {
      expect(tabsFor(caps(list)).filter((tab) => tab.preview)).toEqual([]);
      expect(destinationsFor(caps(list)).filter((item) => item.preview)).toEqual([]);
    }
    expect(destinationsFor(caps(MANAGER)).map((item) => item.key)).not.toContain("overview");
  });

  it("keeps the staff tabs, each behind its capability", () => {
    expect(tabsFor(caps(EMPLOYEE)).map((tab) => tab.key)).toEqual(["home", "availability", "my-shifts", "pay", "more"]);
    expect(tabsFor(caps(MANAGER)).map((tab) => tab.key)).toEqual(["home", "roster", "my-work", "reports", "more"]);
    const noAvailability = EMPLOYEE.filter((c) => c !== "my_work.availability");
    expect(tabsFor(caps(noAvailability)).map((tab) => tab.key)).not.toContain("availability");
    expect(tabsFor(caps([]))).toEqual([]);
  });
});
