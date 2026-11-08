/**
 * Every destination the app offers, in one list (specs/007-ux-redesign FR-001, FR-003 to FR-005,
 * FR-068; contracts/ui-routes.md).
 *
 * Each destination names the capability it needs; the side navigation and the bottom tabs are
 * drawn from this list and the signed-in person's capabilities, never from a role. Previews of
 * later features carry `preview`, show only while previews are switched on, and are marked in the
 * navigation so a reader knows they are not finished screens.
 */
import type { Capabilities, Capability } from "../capabilities";
import { previewsEnabled } from "../config/previews";
import type { IconName } from "../design/icons/Icon";

/** The groups of the staff navigation, and the client's one group. */
export type NavGroup = "my-work" | "manage" | "admin" | "client";

export interface Destination {
  /** The test id's key: `nav-<key>` on the desktop, `tab-<key>` on a phone. */
  key: string;
  href: string;
  /** The label's key in the strings file. */
  labelKey: string;
  icon: IconName;
  group: NavGroup;
  /** Held when any one of these is held. */
  need: readonly Capability[];
  /** A preview of a later feature (FR-068). */
  preview?: boolean;
  /** Other addresses that count as this destination being active. */
  also?: readonly string[];
}

/** The staff shell's destinations, in the order the side navigation shows them. */
export const STAFF_DESTINATIONS: readonly Destination[] = [
  { key: "home", href: "/home", labelKey: "nav.home", icon: "home", group: "my-work", need: ["my_work.read"] },
  { key: "availability", href: "/my/availability", labelKey: "nav.availability", icon: "calendar", group: "my-work", need: ["my_work.availability"] },
  { key: "my-shifts", href: "/my/shifts", labelKey: "nav.shifts", icon: "clock", group: "my-work", need: ["my_work.read"] },
  { key: "pay", href: "/my/pay", labelKey: "nav.pay", icon: "wallet", group: "my-work", need: ["my_work.read"] },
  { key: "requests", href: "/my/requests", labelKey: "nav.requests", icon: "mail", group: "my-work", need: ["my_work.read"], preview: true },
  { key: "roster", href: "/roster", labelKey: "nav.roster", icon: "grid", group: "manage", need: ["roster.read"], also: ["/roster/"] },
  { key: "submissions", href: "/roster/submissions", labelKey: "nav.submissions", icon: "list", group: "manage", need: ["availability.read_others"] },
  { key: "runs", href: "/runs", labelKey: "nav.runs", icon: "play", group: "manage", need: ["schedule.generate"] },
  { key: "work-log", href: "/reports/work-log", labelKey: "nav.work_log", icon: "chart", group: "manage", need: ["reports.read"] },
  { key: "hours", href: "/reports/hours", labelKey: "nav.hours", icon: "clock", group: "manage", need: ["reports.read"] },
  { key: "overview", href: "/manage/overview", labelKey: "nav.overview", icon: "chart", group: "manage", need: ["roster.read"], preview: true },
  { key: "manage-requests", href: "/manage/requests", labelKey: "nav.client_requests", icon: "mail", group: "manage", need: ["client_requests.answer"], preview: true },
  { key: "approvals", href: "/manage/approvals", labelKey: "nav.approvals", icon: "check", group: "manage", need: ["client_requests.answer"], preview: true },
  { key: "staff", href: "/admin/staff", labelKey: "nav.staff", icon: "people", group: "admin", need: ["staff.manage"] },
  { key: "clients", href: "/admin/clients", labelKey: "nav.clients", icon: "briefcase", group: "admin", need: ["clients.manage"] },
  { key: "sites", href: "/admin/sites", labelKey: "nav.sites", icon: "building", group: "admin", need: ["sites.manage"] },
  { key: "projects", href: "/admin/projects", labelKey: "nav.projects", icon: "layers", group: "admin", need: ["projects.manage"] },
  { key: "shift-templates", href: "/admin/shift-templates", labelKey: "nav.shift_templates", icon: "clock", group: "admin", need: ["projects.manage"] },
  { key: "staffing-needs", href: "/admin/staffing-needs", labelKey: "nav.staffing_needs", icon: "target", group: "admin", need: ["projects.manage"] },
  { key: "special-days", href: "/admin/special-days", labelKey: "nav.special_days", icon: "star", group: "admin", need: ["special_days.edit"] },
  { key: "pay-settings", href: "/admin/pay-settings", labelKey: "nav.pay_settings", icon: "settings", group: "admin", need: ["pay_settings.edit", "pay.read_others"] },
  { key: "audit-log", href: "/admin/audit-log", labelKey: "nav.audit_log", icon: "list", group: "admin", need: ["staff.delete"], preview: true },
];

/** The client shell's destinations (FR-004). */
export const CLIENT_DESTINATIONS: readonly Destination[] = [
  { key: "client-home", href: "/client", labelKey: "nav.projects", icon: "layers", group: "client", need: ["client_portal.read"], also: ["/client/projects/"] },
  { key: "billing", href: "/client/billing", labelKey: "nav.billing", icon: "wallet", group: "client", need: ["client_portal.read"], preview: true },
  { key: "requests", href: "/client/requests", labelKey: "nav.requests", icon: "mail", group: "client", need: ["client_portal.read"], preview: true },
  { key: "contact", href: "/client/contact", labelKey: "nav.contact", icon: "phone", group: "client", need: ["client_portal.read"] },
];

/** Whether `destination` (or a tab) is offered to someone with `capabilities`. */
export function offered(destination: Pick<Destination, "need" | "preview">, capabilities: Capabilities): boolean {
  if (destination.preview && !previewsEnabled) return false;
  return destination.need.some((capability) => capabilities.has(capability));
}

/** Which shell a person gets: the client's when they read the client portal, else the staff's. */
export function shellFor(capabilities: Capabilities): "staff" | "client" | "none" {
  if (capabilities.has("client_portal.read")) return "client";
  if (capabilities.has("my_work.read")) return "staff";
  return "none";
}

/** The person's home address. */
export function homeFor(capabilities: Capabilities): string {
  return shellFor(capabilities) === "client" ? "/client" : "/home";
}

/** The destinations a person may open, in navigation order. */
export function destinationsFor(capabilities: Capabilities): Destination[] {
  const list = shellFor(capabilities) === "client" ? CLIENT_DESTINATIONS : STAFF_DESTINATIONS;
  return list.filter((destination) => offered(destination, capabilities));
}

/** The groups of the side navigation, each with its destinations, groups with none left out. */
export function groupsFor(capabilities: Capabilities): { group: NavGroup; items: Destination[] }[] {
  const order: NavGroup[] = ["my-work", "manage", "admin", "client"];
  const items = destinationsFor(capabilities);
  return order
    .map((group) => ({ group, items: items.filter((item) => item.group === group) }))
    .filter((entry) => entry.items.length > 0);
}

/**
 * A bottom tab (FR-005): at most five per shell, with labels short enough for a fifth of a phone.
 * A tab is offered by the same rule as a destination: held capability, and previews only while
 * previews are on (FR-068), so a production build never offers a tab that opens the 404 page.
 */
export interface Tab {
  key: string;
  href: string;
  labelKey: string;
  icon: IconName;
  /** Held when any one of these is held. */
  need: readonly Capability[];
  /** A preview of a later feature (FR-068). */
  preview?: boolean;
  /** Addresses that light this tab. */
  matches: readonly string[];
}

const EMPLOYEE_TABS: readonly Tab[] = [
  { key: "home", href: "/home", labelKey: "nav.home", icon: "home", need: ["my_work.read"], matches: ["/home"] },
  { key: "availability", href: "/my/availability", labelKey: "nav.availability", icon: "calendar", need: ["my_work.availability"], matches: ["/my/availability"] },
  { key: "my-shifts", href: "/my/shifts", labelKey: "nav.shifts_short", icon: "clock", need: ["my_work.read"], matches: ["/my/shifts"] },
  { key: "pay", href: "/my/pay", labelKey: "nav.pay_short", icon: "wallet", need: ["my_work.read"], matches: ["/my/pay"] },
  { key: "more", href: "/more", labelKey: "nav.more", icon: "more", need: ["my_work.read"], matches: ["/more", "/account", "/admin", "/help", "/contact", "/accessibility", "/my/requests", "/inbox"] },
];

const MANAGER_TABS: readonly Tab[] = [
  { key: "home", href: "/home", labelKey: "nav.home", icon: "home", need: ["my_work.read"], matches: ["/home"] },
  { key: "roster", href: "/roster", labelKey: "nav.roster", icon: "grid", need: ["roster.read"], matches: ["/roster", "/runs"] },
  { key: "my-work", href: "/my", labelKey: "nav.my_work", icon: "user", need: ["my_work.read"], matches: ["/my"] },
  { key: "reports", href: "/reports", labelKey: "nav.reports", icon: "chart", need: ["reports.read"], matches: ["/reports"] },
  { key: "more", href: "/more", labelKey: "nav.more", icon: "more", need: ["my_work.read"], matches: ["/more", "/account", "/admin", "/manage", "/help", "/contact", "/accessibility", "/inbox"] },
];

const CLIENT_TABS: readonly Tab[] = [
  { key: "projects", href: "/client", labelKey: "nav.projects", icon: "layers", need: ["client_portal.read"], matches: ["/client", "/client/projects"] },
  { key: "billing", href: "/client/billing", labelKey: "nav.billing", icon: "wallet", need: ["client_portal.read"], preview: true, matches: ["/client/billing"] },
  { key: "requests", href: "/client/requests", labelKey: "nav.requests", icon: "mail", need: ["client_portal.read"], preview: true, matches: ["/client/requests"] },
  { key: "contact", href: "/client/contact", labelKey: "nav.contact", icon: "phone", need: ["client_portal.read"], matches: ["/client/contact"] },
];

/** The bottom tabs a person is offered: the shell's set, each through the same rule as a destination. */
export function tabsFor(capabilities: Capabilities): readonly Tab[] {
  const shell = shellFor(capabilities);
  if (shell === "none") return [];
  const set = shell === "client" ? CLIENT_TABS : capabilities.has("roster.read") ? MANAGER_TABS : EMPLOYEE_TABS;
  return set.filter((tab) => offered(tab, capabilities));
}

/** Whether `pathname` lies at or under `href`. */
export function isUnder(pathname: string, href: string): boolean {
  return pathname === href || pathname.startsWith(`${href}/`);
}

/** Whether `destination` is the one the person is on. */
export function isActiveDestination(destination: Destination, pathname: string): boolean {
  if (destination.href === "/client") return pathname === "/client" || pathname.startsWith("/client/projects");
  if (destination.href === "/roster") return pathname === "/roster";
  return isUnder(pathname, destination.href);
}

/** Whether `tab` is the one the person is on. */
export function isActiveTab(tab: Tab, pathname: string): boolean {
  if (tab.key === "projects") return pathname === "/client" || pathname.startsWith("/client/projects");
  return tab.matches.some((prefix) => isUnder(pathname, prefix));
}
