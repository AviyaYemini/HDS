/**
 * Steering a run, its pure parts (specs/008-auto-scheduler-review FR-017, FR-020, FR-024; T040,
 * T041): a chip is complete only with what its kind needs, a chip reads in words, and an average
 * keeps one decimal place.
 */
import { average, chipWords, complete, needs, weekdayName, type Chip } from "../../../src/screens/runs/steering";

const ANN = "00000000-0000-0000-0000-00000000000a";

test("each kind needs its value", () => {
  expect(needs("max_hours")).toBe("count");
  expect(needs("keep_on")).toBe("place");
  expect(needs("prefer")).toBe("when");
  expect(complete({ kind: "max_nights", user_id: ANN })).toBe(false);
  expect(complete({ kind: "max_nights", user_id: ANN, count: 0 })).toBe(true);
  expect(complete({ kind: "keep_off", user_id: ANN, weekday: 5 })).toBe(true);
  expect(complete({ kind: "keep_on", user_id: ANN })).toBe(false);
});

test("a chip reads in words with its person and value", () => {
  const name = () => "Ann";
  const said = chipWords({ kind: "keep_off", user_id: ANN, weekday: 5 } as Chip, name, () => "", () => "", "en");
  expect(said).toEqual({ key: "runs.chip.keep_off", values: { person: "Ann", when: "Friday" } });
  expect(chipWords({ kind: "max_shifts", user_id: ANN, count: 3 } as Chip, name, () => "", () => "", "en")).toEqual({
    key: "runs.chip.max_shifts",
    values: { person: "Ann", count: 3 },
  });
});

test("weekdays count from Sunday, and averages keep one decimal place", () => {
  expect(weekdayName(0, "en")).toBe("Sunday");
  expect(weekdayName(6, "en")).toBe("Saturday");
  expect(average(10, 3)).toBe(3.3);
  expect(average(5, 0)).toBe(0);
});
