/**
 * The overview charts' arithmetic (the owner, 2026-11-13): exact sums, donut slices with "others",
 * weekly candles from the daily series, scale ticks and the arc geometry.
 */
import { arcPath, niceTicks, pieSlices, sumAmounts, weeklyCandles } from "../../../src/screens/deck/chartMath";

test("amounts add up exactly, in agorot", () => {
  expect(sumAmounts(["0.10", "0.20"])).toBe("0.30");
  expect(sumAmounts(["123456.30", "63140.00"])).toBe("186596.30");
  expect(sumAmounts(["5", "-7.5"])).toBe("-2.50");
  expect(sumAmounts([])).toBe("0.00");
});

test("slices: grouped by label, the top N and others, shares adding to 100", () => {
  const items = [
    { label: "Alpha", amount: "500.00" },
    { label: "Beta", amount: "300.00" },
    { label: "Alpha", amount: "100.00" },
    { label: "Gamma", amount: "50.00" },
    { label: "Delta", amount: "50.00" },
    { label: "Zero", amount: "0.00" },
  ];
  const slices = pieSlices(items, 2, "Others");
  expect(slices.map((s) => [s.label, s.amount])).toEqual([
    ["Alpha", "600.00"],
    ["Beta", "300.00"],
    ["Others", "100.00"],
  ]);
  expect(slices[2]!.others).toBe(true);
  expect(slices.reduce((n, s) => n + s.pct, 0)).toBeCloseTo(100);
  expect(slices[0]!.pct).toBeCloseTo(60);
  // No others slice when everything fits.
  expect(pieSlices(items, 5, "Others").some((s) => s.others)).toBe(false);
});

test("candles: a week's first and last working day, its most and least; forecast weeks marked", () => {
  const days = [
    // Week of Sunday 1 November 2026.
    { date: "2026-11-02", billing: "1000.00", forecast: false },
    { date: "2026-11-01", billing: "800.00", forecast: false },
    { date: "2026-11-04", billing: "1500.00", forecast: false },
    { date: "2026-11-06", billing: "600.00", forecast: false },
    // Week of 8 November: rising, with a forecast day; a day without billing is skipped.
    { date: "2026-11-08", billing: "700.00", forecast: false },
    { date: "2026-11-10", billing: null, forecast: false },
    { date: "2026-11-14", billing: "900.00", forecast: true },
  ];
  expect(weeklyCandles(days)).toEqual([
    { week: "2026-11-01", open: 800, close: 600, high: 1500, low: 600, rising: false, forecast: false, days: 4 },
    { week: "2026-11-08", open: 700, close: 900, high: 900, low: 700, rising: true, forecast: true, days: 2 },
  ]);
  expect(weeklyCandles([{ date: "2026-11-02", billing: null, forecast: false }])).toEqual([]);
});

test("ticks cover the range in round steps", () => {
  expect(niceTicks(0, 1000)).toEqual([0, 250, 500, 750, 1000]);
  const ticks = niceTicks(1234, 9876);
  expect(ticks[0]).toBeLessThanOrEqual(1234);
  expect(ticks[ticks.length - 1]).toBeGreaterThanOrEqual(9876);
});

test("arcs: a quarter, and a whole circle as two halves", () => {
  expect(arcPath(50, 50, 40, 20, 0, Math.PI / 2)).toBe("M 50.00 10.00 A 40 40 0 0 1 90.00 50.00 L 70.00 50.00 A 20 20 0 0 0 50.00 30.00 Z");
  expect(arcPath(50, 50, 40, 20, 0, Math.PI * 2).match(/M /g)).toHaveLength(2);
});
