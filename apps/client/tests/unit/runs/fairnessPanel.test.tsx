/**
 * What a proposal guarantees, traded off and how fair it is (specs/008-auto-scheduler-review FR-023
 * to FR-026, T041): the guarantee line and override count, the summary in words, each chip's state
 * in words, and outliers marked by words, not colour alone; a new version reads it again.
 */
import { screen } from "@testing-library/react-native";

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
  expect(screen.getByText("1 assignment breaks a rule by someone's reason.")).toBeTruthy();
  expect(screen.getByText("2 locked assignments kept.")).toBeTruthy();
  expect(screen.getByText("1 place left empty.")).toBeTruthy();
  expect(screen.getByText("Ann: at most 2 nights: not met: meeting it would leave a place empty")).toBeTruthy();
  expect(screen.getByText("hours well above average · nights well above average")).toBeTruthy();
  expect(screen.getAllByTestId("fairness-outlier")).toHaveLength(1);
});
