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
      kept: [] as string[],
      people: people.map((p) => ({ user_id: p, by: "solver", locked: false, override: null })),
      places: Math.max(0, 1 - people.length),
      marks: people.map((p) => ({ mark: "added", user_id: p, replaces: null, accepted: false })),
      part_accepted: false,
    },
  ],
  unfilled: [],
  employees: [],
  no_availability: [],
});

const COVER = `/api/v1/schedule-jobs/${JOB}/slots/${encodeURIComponent(KEY)}/cover`;
const option = (user_id: string, extra: Record<string, unknown> = {}) => ({
  user_id,
  free: true,
  code: null,
  have: null,
  need: null,
  week_minutes: null,
  slot_minutes: null,
  limit: null,
  close: false,
  open: true,
  minutes: 480,
  nights: 0,
  weekends: 0,
  ...extra,
});
const cover = (...options: ReturnType<typeof option>[]) => ({ key: KEY, options, totals: { people: 2, minutes: 480, nights: 0, weekends: 0 } });

const job = { id: JOB, from: "2027-06-06", to: "2027-06-12", status: "succeeded", proposal_state: "open" } as unknown as Job;
const names = { projects: new Map([[PROJECT, "North Gate"]]), types: new Map(), people: new Map([[ANN, "Ann"], [BEN, "Ben"]]) };

test("a break asks for a reason, and keeping it sends the reason with the same edit", async () => {
  const api = new FakeApi();
  api.on("GET", `/api/v1/schedule-jobs/${JOB}/proposal`, json(200, review(0, [ANN])));
  api.on("GET", COVER, json(200, cover(option(BEN))));
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

test("staff kept on a draft the run could not place are shown, and not offered for edits", async () => {
  const api = new FakeApi();
  const body = review(0, [ANN]);
  body.slots[0]!.kept = [BEN];
  api.on("GET", `/api/v1/schedule-jobs/${JOB}/proposal`, json(200, body));
  const session = await signedIn(api);
  await renderWith(session, <ReviewWeek job={job} names={names} people={[{ id: ANN, full_name: "Ann" }, { id: BEN, full_name: "Ben" }]} onDecided={() => undefined} />);

  expect(await screen.findByTestId("proposal-kept")).toBeTruthy();
  expect(screen.getByText("Ben")).toBeTruthy();
  // Ann is the proposal's, so her name is a button that opens the edits; Ben's is text only.
  expect(screen.getAllByTestId("review-person")).toHaveLength(1);
});


test("who else could cover: free people with their load, then a rule with its numbers in words", async () => {
  const api = new FakeApi();
  api.on("GET", `/api/v1/schedule-jobs/${JOB}/proposal`, json(200, review(0, [])));
  api.on(
    "GET",
    COVER,
    json(200, cover(option(BEN), option(ANN, { free: false, code: "schedule.minimum_rest", have: 540, need: 660 }))),
  );
  api.on("POST", `/api/v1/schedule-jobs/${JOB}/edits`, json(200, { proposal: review(1, [ANN]), warnings: [] }));
  const session = await signedIn(api);
  await renderWith(session, <ReviewWeek job={job} names={names} people={[{ id: ANN, full_name: "Ann" }, { id: BEN, full_name: "Ben" }]} onDecided={() => undefined} />);

  await act(async () => {
    fireEvent.press(await screen.findByTestId("action-review-assign"));
  });

  const options = await screen.findAllByTestId("review-assign-option");
  expect(options.map((o) => o.props.accessibilityLabel ?? "")).toHaveLength(2);
  expect(screen.getByText("Free then")).toBeTruthy();
  expect(screen.getByText("Rest between shifts · 9 h rest, needs 11 h")).toBeTruthy();
  expect(screen.getAllByText("After it: 8 h, 0 nights, 0 weekend shifts (average 4 h)")).toHaveLength(2);
  await act(async () => {
    fireEvent.press(screen.getByText("Assign Ann anyway"));
  });
  expect(api.calls("POST", `/api/v1/schedule-jobs/${JOB}/edits`).map((c) => c.body)).toEqual([{ op: "assign", key: KEY, user_id: ANN }]);
});

test("when who else could cover cannot be read, the staff are still offered, with a retry (R24)", async () => {
  const api = new FakeApi();
  api.on("GET", `/api/v1/schedule-jobs/${JOB}/proposal`, json(200, review(0, [ANN])));
  api.on("GET", COVER, json(503, { type: "x", title: "x", status: 503, code: "errors.unavailable" }), json(200, cover(option(BEN))));
  const session = await signedIn(api);
  await renderWith(session, <ReviewWeek job={job} names={names} people={[{ id: ANN, full_name: "Ann" }, { id: BEN, full_name: "Ben" }]} onDecided={() => undefined} />);

  await act(async () => {
    fireEvent.press(await screen.findByTestId("action-review-assign"));
  });

  // Ann is on the slot already; Ben is offered from the staff list, and the retry reads the list.
  expect(await screen.findByText("Ben")).toBeTruthy();
  expect(screen.queryByTestId("review-cover-option")).toBeNull();
  await act(async () => {
    fireEvent.press(screen.getByText("Try again"));
  });
  expect(await screen.findByTestId("review-cover-option")).toBeTruthy();
  expect(api.calls("GET", COVER)).toHaveLength(2);
});
