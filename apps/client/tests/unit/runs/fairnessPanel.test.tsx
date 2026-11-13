/**
 * What a proposal guarantees, traded off and how fair it is (specs/008-auto-scheduler-review FR-023
 * to FR-026, T041): the guarantee line and override count, the summary in words, each chip's state
 * in words, and outliers marked by words, not colour alone; a new version reads it again.
 */
import { fireEvent, screen } from "@testing-library/react-native";

import { FairnessPanel } from "../../../src/screens/runs/FairnessPanel";
import { FakeApi, json, renderWith, signedIn } from "../fakeApi";

const JOB = "33333333-3333-4333-8333-333333333333";
const ANN = "44444444-4444-4444-8444-444444444444";
const BEN = "55555555-5555-4555-8555-555555555555";

const answer = {
  shares: [
    { user_id: ANN, minutes: 2400, nights: 4, weekends: 0, hours_outlier: true, nights_outlier: true, weekends_outlier: false },
    { user_id: BEN, minutes: 480, nights: 0, weekends: 0, hours_outlier: false, nights_outlier: false, weekends_outlier: false },
  ],
  totals: { people: 2, minutes: 2880, nights: 4, weekends: 0 },
  tradeoffs: { empty_places: 1, open_wishes_missed: 0, close_wishes_given: 2, minutes_low: 480, minutes_high: 2400, nights_low: 0, nights_high: 4, chips_not_met: 1 },
  chips: [{ kind: "max_nights", user_id: ANN, count: 2, state: "not_met", cause: "coverage" }],
  overridden: 1,
  locks: 2,
};

test("the guarantees, the summary, the chips and the outliers read in words", async () => {
  const api = new FakeApi().on("GET", `/api/v1/schedule-jobs/${JOB}/fairness`, json(200, answer));
  const session = await signedIn(api);
  const names: Record<string, string> = { [ANN]: "Ann", [BEN]: "Ben" };
  await renderWith(session, <FairnessPanel jobId={JOB} version={0} name={(id) => names[id] ?? "?"} place={() => ""} type={() => ""} />);

  expect(await screen.findByText("Respects rest hours, weekly maximums, blocked dates and absences.")).toBeTruthy();
  // No count of rule breaks: warnings are out of the app's scope (the owner, 2026-11-13).
  expect(screen.queryByTestId("review-overridden")).toBeNull();
  expect(screen.getByText("2 locked assignments kept.")).toBeTruthy();
  expect(screen.getByText("1 place left empty.")).toBeTruthy();
  expect(screen.getByText("Ann: at most 2 nights: not met: meeting it would leave a place empty")).toBeTruthy();
  // Above the average in a quiet tone, most hours first, no warning icon (the owner, 2026-11-13).
  expect(screen.getByText("hours above average · nights above average")).toBeTruthy();
  expect(screen.getAllByTestId("fairness-outlier")).toHaveLength(1);
});

test("fairness lists the most hours first, the top ten until 'show all'", async () => {
  const id = (i: number) => `00000000-0000-4000-8000-${String(i).padStart(12, "0")}`;
  const shares = Array.from({ length: 12 }, (_, i) => ({ user_id: id(i), minutes: 60 * (i + 1), nights: 0, weekends: 0, hours_outlier: false, nights_outlier: false, weekends_outlier: false }));
  const api = new FakeApi().on("GET", `/api/v1/schedule-jobs/${JOB}/fairness`, json(200, { ...answer, shares, chips: [], totals: { people: 12, minutes: 4680, nights: 0, weekends: 0 } }));
  const session = await signedIn(api);
  await renderWith(session, <FairnessPanel jobId={JOB} version={0} name={(u) => `P${Number(u.slice(-12))}`} place={() => ""} type={() => ""} />);
  const rows = await screen.findAllByTestId("fairness-row");
  expect(rows).toHaveLength(10);
  expect(rows[0]).toHaveTextContent(/^P11/);
  await fireEvent.press(screen.getByTestId("fairness-show-all"));
  expect(screen.getAllByTestId("fairness-row")).toHaveLength(12);
});
