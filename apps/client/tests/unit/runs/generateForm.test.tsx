/**
 * The run form (the owner, 2026-11-13): when the range already holds draft shifts of the chosen
 * projects, a run defaults to "keep changes small", so it fills the gaps instead of reshuffling the
 * managers' drafts; with no drafts it keeps the plain default. Made-up names only.
 */
import { fireEvent, screen, waitFor } from "@testing-library/react-native";

import { setupI18n } from "../../../src/i18n";
import { GenerateForm } from "../../../src/screens/runs/GenerateForm";
import { FakeApi, json, renderWith, signedIn } from "../fakeApi";

const offered = [{ id: "p1", name: "Harbour gate", type: "security" }];
const draft = { id: "s1", project_id: "p1", shift_type_id: "t", starts_at: "2027-06-07T05:00:00Z", ends_at: "2027-06-07T13:00:00Z", timezone: "Asia/Jerusalem", headcount: 1, status: "draft", etag: "e", pay_rate: null, published_at: null, assignments: [] };

function api(drafts: unknown[]) {
  return new FakeApi()
    .on("GET", "/api/v1/shifts", json(200, { items: drafts, next_cursor: null }))
    .on("POST", "/api/v1/schedule-jobs", json(202, { id: "j1" }));
}

beforeEach(async () => {
  await setupI18n("en");
});

test("a range with drafts runs with few changes, and says why", async () => {
  const fake = api([draft]);
  await renderWith(await signedIn(fake), <GenerateForm onCancel={() => undefined} offered={offered} />);
  expect(await screen.findByTestId("run-drafts-note", {}, { timeout: 5000 })).toHaveTextContent(/1 draft shift/);
  await fireEvent.press(screen.getByTestId("action-generate"));
  await waitFor(() => expect(fake.calls("POST", "/api/v1/schedule-jobs")).toHaveLength(1));
  expect((fake.calls("POST", "/api/v1/schedule-jobs")[0]!.body as { controls: { keep_changes_small: boolean } }).controls.keep_changes_small).toBe(true);
  // Only drafts are asked for.
  expect(new URLSearchParams(fake.calls("GET", "/api/v1/shifts")[0]!.query).get("status")).toBe("draft");
});

test("a range without drafts keeps the plain default", async () => {
  const fake = api([]);
  await renderWith(await signedIn(fake), <GenerateForm onCancel={() => undefined} offered={offered} />);
  await waitFor(() => expect(fake.calls("GET", "/api/v1/shifts").length).toBeGreaterThan(0));
  expect(screen.queryByTestId("run-drafts-note")).toBeNull();
  await fireEvent.press(screen.getByTestId("action-generate"));
  await waitFor(() => expect(fake.calls("POST", "/api/v1/schedule-jobs")).toHaveLength(1));
  expect((fake.calls("POST", "/api/v1/schedule-jobs")[0]!.body as { controls: { keep_changes_small: boolean } }).controls.keep_changes_small).toBe(false);
});
