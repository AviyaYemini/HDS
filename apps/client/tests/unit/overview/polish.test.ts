/**
 * The dashboards' small touches (the owner, 2026-11-13): the greeting, change lines, whole-shekel
 * rounding done exactly, and the trend line's scaling.
 */
import { changePct, changePoints, easeOut, firstName, formatChange, formatMoneyWhole, greetingKey, roundAmount, sparkPoints } from "../../../src/screens/deck/polish";

test("greeting by the hour", () => {
  expect([4, 5, 11, 12, 16, 17, 23].map(greetingKey)).toEqual([
    "greeting.evening",
    "greeting.morning",
    "greeting.morning",
    "greeting.afternoon",
    "greeting.afternoon",
    "greeting.evening",
    "greeting.evening",
  ]);
  expect(firstName("Dana Levi")).toBe("Dana");
  expect(firstName("  Omer ")).toBe("Omer");
  expect(firstName(null)).toBe("");
});

test("changes against the previous period", () => {
  expect(changePct("110.00", "100.00")).toBeCloseTo(10);
  expect(changePct("90.00", "100.00")).toBeCloseTo(-10);
  expect(changePct("5.00", "0.00")).toBeNull();
  expect(changePct("5.00", null)).toBeNull();
  expect(formatChange(12.4)).toBe("▲ 12%");
  expect(formatChange(-3.44)).toBe("▼ 3.4%");
  expect(formatChange(0.02)).toBe("= 0%");
  expect(formatChange(null)).toBeNull();
  expect(changePoints("18.30", "17.10")).toBe(1.2);
  expect(changePoints("-9.09", "2.00")).toBe(-11.1);
});

test("whole shekels, rounded half up on the decimal string", () => {
  expect(roundAmount("123456.30")).toBe("123456");
  expect(roundAmount("123456.50")).toBe("123457");
  expect(roundAmount("0.49")).toBe("0");
  expect(roundAmount("-12.70")).toBe("-13");
  expect(roundAmount("99999999999999999.99")).toBe("100000000000000000");
  expect(formatMoneyWhole("123456.30", "en")).toMatch(/123,456(?!\.)/);
  expect(formatMoneyWhole("123456.30", "en")).not.toMatch(/\.30|\.00/);
});

test("trend line points scale between the least and the most", () => {
  expect(sparkPoints([0, 10], 100, 20, 0)).toBe("0.0,20.0 100.0,0.0");
  expect(sparkPoints([5, 5, 5], 100, 20, 0)).toBe("0.0,10.0 50.0,10.0 100.0,10.0");
  expect(sparkPoints([7], 100, 20, 0)).toBe("50.0,10.0");
  expect(sparkPoints([], 100, 20)).toBe("");
});

test("the entrance eases out and stays within 0–1", () => {
  expect(easeOut(0)).toBe(0);
  expect(easeOut(1)).toBe(1);
  expect(easeOut(2)).toBe(1);
  expect(easeOut(0.5)).toBeGreaterThan(0.5);
});
