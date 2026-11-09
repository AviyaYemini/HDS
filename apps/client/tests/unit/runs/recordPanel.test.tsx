/**
 * A run's record (specs/008-auto-scheduler-review FR-030, T052): each step in words with who and
 * when, an override's reason, the system's steps named as such, and the next page on request.
 */
import { act, fireEvent, screen } from "@testing-library/react-native";

import { RecordPanel } from "../../../src/screens/runs/RecordPanel";
import { FakeApi, json, renderWith, signedIn } from "../fakeApi";

const JOB = "33333333-3333-4333-8333-333333333333";
const ANN = "44444444-4444-4444-8444-444444444444";
const RECORD = `/api/v1/schedule-jobs/${JOB}/record`;

const step = (seq: number, kind: string, actor: string | null, body: Record<string, unknown> = {}) => ({
  seq,
  kind,
  at: "2027-06-06T08:00:00Z",
  actor_id: actor,
  version: null,
  body,
});

test("steps read in words, with who, a reason, and the next page on request", async () => {
  const api = new FakeApi().on(
    "GET",
    RECORD,
    json(200, { items: [step(1, "created", ANN), step(2, "finished", null, { filled: 3, needed: 4 })], next_after: 2 }),
    json(200, { items: [step(3, "overridden", ANN, { reason: "Asked to stay" })], next_after: null }),
  );
  const session = await signedIn(api);
  await renderWith(session, <RecordPanel jobId={JOB} name={(id) => (id === ANN ? "Ann" : "?")} />);

  expect(await screen.findByText("Run started")).toBeTruthy();
  expect(screen.getByText("3 of 4 places filled")).toBeTruthy();
  expect(screen.getAllByText(/^The system/)).toHaveLength(1);
  await act(async () => {
    fireEvent.press(screen.getByTestId("action-record-more"));
  });

  expect(await screen.findByText("Reason: Asked to stay")).toBeTruthy();
  expect(screen.getAllByTestId("record-step")).toHaveLength(3);
  expect(screen.queryByTestId("action-record-more")).toBeNull();
  expect(api.calls("GET", RECORD).map((c) => c.query)).toEqual(["?limit=50", "?after=2&limit=50"]);
});
