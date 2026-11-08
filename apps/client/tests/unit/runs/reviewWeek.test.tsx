/**
 * The review's edit that breaks a rule asks for a reason (specs/008-auto-scheduler-review FR-010,
 * T017): the API's 422 with its breaks turns the sheet to the reason, and keeping it sends the
 * reason with the same edit.
 */
import { act, fireEvent, screen } from "@testing-library/react-native";

import { ToastHost } from "../../../src/design/components";
import { ReviewWeek } from "../../../src/screens/runs/ReviewWeek";
import type { Job } from "../../../src/screens/runs/model";
import { FakeApi, json, renderWith, signedIn } from "../fakeApi";

const JOB = "33333333-3333-4333-8333-333333333333";
const ANN = "44444444-4444-4444-8444-444444444444";
const BEN = "55555555-5555-4555-8555-555555555555";
const PROJECT = "66666666-6666-4666-8666-666666666666";
const KEY = "need:mon";

const review = (version: number, people: string[]) => ({
  job_id: JOB,
  version,
  state: "open",
  optimal: true,
  settled: false,
  counts: { added: people.length, changed: 0, removed: 0, unchanged: 0 },
  coverage: [{ date: "2027-06-07", project_id: PROJECT, filled: people.length, needed: 1 }],
  slots: [
    {
      key: KEY,
      shift_id: null,
      project_id: PROJECT,
      shift_type_id: "77777777-7777-4777-8777-777777777777",
      date: "2027-06-07",
      starts_at: "2027-06-07T04:00:00Z",
      ends_at: "2027-06-07T12:00:00Z",
      headcount: 1,
      kept: [],
      people: people.map((p) => ({ user_id: p, by: "solver", locked: false, override: null })),
      places: Math.max(0, 1 - people.length),
      marks: people.map((p) => ({ mark: "added", user_id: p, replaces: null, accepted: false })),
    },
  ],
  unfilled: [],
  employees: [],
  no_availability: [],
});

const job = { id: JOB, from: "2027-06-06", to: "2027-06-12", status: "succeeded", proposal_state: "open" } as unknown as Job;
const names = { projects: new Map([[PROJECT, "North Gate"]]), types: new Map(), people: new Map([[ANN, "Ann"], [BEN, "Ben"]]) };

test("a break asks for a reason, and keeping it sends the reason with the same edit", async () => {
  const api = new FakeApi();
  api.on("GET", `/api/v1/schedule-jobs/${JOB}/proposal`, json(200, review(0, [ANN])));
  api.on(
    "POST",
    `/api/v1/schedule-jobs/${JOB}/edits`,
    json(422, {
      type: "x",
      title: "x",
      status: 422,
      code: "schedule.override_reason_required",
      errors: [{ key: KEY, date: "2027-06-07", user_id: null, code: "warnings.over_headcount", values: {}, shift_ids: [] }],
    }),
    json(200, { proposal: review(1, [ANN, BEN]), warnings: [] }),
  );
  const session = await signedIn(api);
  await renderWith(session, <><ReviewWeek job={job} names={names} people={[{ id: ANN, full_name: "Ann" }, { id: BEN, full_name: "Ben" }]} onDecided={() => undefined} /><ToastHost /></>);

  await act(async () => {
    fireEvent.press(await screen.findByTestId("action-review-assign"));
  });
  await act(async () => {
    fireEvent.press(await screen.findByTestId("review-assign-option"));
  });

  expect(await screen.findByTestId("review-break")).toBeTruthy();
  await act(async () => {
    fireEvent.changeText(screen.getByTestId("review-override-reason"), "One more is needed");
  });
  await act(async () => {
    fireEvent.press(screen.getByTestId("action-keep-anyway"));
  });

  const sent = api.calls("POST", `/api/v1/schedule-jobs/${JOB}/edits`).map((c) => c.body);
  expect(sent).toEqual([
    { op: "assign", key: KEY, user_id: BEN },
    { op: "assign", key: KEY, user_id: BEN, override_reason: "One more is needed" },
  ]);
});
