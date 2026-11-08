/** One shift's page (FR-023; PR 3 review): without a project manager, the company's contact details stand in. */
import { screen } from "@testing-library/react-native";
import * as Router from "expo-router";

import type { Me } from "../../../src/api/session";
import { ShiftDetailScreen } from "../../../src/screens/my-work/shifts/ShiftDetailScreen";
import { FakeApi, ME, json, renderWith, signedIn } from "../fakeApi";

const EMPLOYEE: Me = { ...(ME as Me), id: "me", capabilities: ["my_work.read", "account.self"], managed_project_ids: [] };
const setSearchParams = (Router as unknown as { __setSearchParams: (params: Record<string, string>) => void }).__setSearchParams;

const detail = (contacts: { full_name: string; email: string; phone: string | null }[]) => ({
  id: "s1",
  starts_at: "2026-11-09T06:00:00Z",
  ends_at: "2026-11-09T14:00:00Z",
  timezone: "Asia/Jerusalem",
  project: { id: "p", name: "Night Watch" },
  site: { id: "x", name: "Gate 3" },
  site_address: "Rothschild 1, Tel Aviv",
  shift_type: "morning",
  contacts,
});

afterEach(() => setSearchParams({}));

it("falls back to the organisation's phone and e-mail and links to the contact page", async () => {
  setSearchParams({ id: "s1" });
  const api = new FakeApi()
    .on("GET", "/me", json(200, EMPLOYEE))
    .on("GET", "/me/shifts/s1", json(200, detail([])))
    .on("GET", "/api/v1/organisation/contact", json(200, { phone: "03-5550100", email: "office@hds.test", opening_hours: [] }));
  await renderWith(await signedIn(api), <ShiftDetailScreen />);
  expect(await screen.findByTestId("shift-org-contact")).toBeTruthy();
  expect(await screen.findByTestId("contact-phone")).toHaveTextContent(/03-5550100/);
  expect(screen.getByTestId("contact-email")).toHaveTextContent(/office@hds\.test/);
  expect(screen.getByTestId("link-contact-page")).toBeTruthy();
  expect(screen.queryByTestId("shift-contact")).toBeNull();
});

it("shows the project's managers when there are some", async () => {
  setSearchParams({ id: "s1" });
  const api = new FakeApi().on("GET", "/me", json(200, EMPLOYEE)).on("GET", "/me/shifts/s1", json(200, detail([{ full_name: "Dana Manager", email: "dana@hds.test", phone: "050-1234567" }])));
  await renderWith(await signedIn(api), <ShiftDetailScreen />);
  expect(await screen.findByTestId("shift-contact")).toHaveTextContent(/Dana Manager/);
  expect(screen.queryByTestId("shift-org-contact")).toBeNull();
  expect(api.calls("GET", "/api/v1/organisation/contact")).toHaveLength(0);
});
