/**
 * Locale plumbing (specs/007-ux-redesign FR-053 to FR-058, task T017): dates read day first and
 * print dd/MM/yyyy, weeks start on Sunday, presets agree with their dates, times carry "+1" and sit
 * in a left-to-right isolate, money is formatted from the decimal string without arithmetic, and
 * Israeli holidays and eves are found.
 */
import {
  FORWARD_PRESETS,
  FSI,
  LRI,
  PDI,
  addDays,
  addMonths,
  crossesClockChange,
  dayOffset,
  daysInMonth,
  durationParts,
  endOfMonth,
  endOfWeek,
  formatHeaderDate,
  formatMoney,
  formatNumericDate,
  formatNumericRange,
  formatShortNumericRange,
  formatTime,
  formatTimeRange,
  groupDigits,
  holidaysBetween,
  isolate,
  isolateLtr,
  monthNames,
  parseTypedDate,
  presetFor,
  presetRange,
  splitDecimal,
  startOfWeek,
  stripIsolates,
  todayIn,
  weekday,
  weekdayNames,
} from "../../src/locale";

const ZONE = "Asia/Jerusalem";
// Saturday 7 November 2026, 10:00 in Jerusalem (the e2e specs' fixed clock).
const NOW = new Date("2026-11-07T08:00:00Z");

describe("typed dates (FR-051, D1)", () => {
  it.each(["7/11/2026", "7.11.2026", "7-11-2026", "07/11/2026", " 7 / 11 / 26 "])("reads %s as 7 November 2026", (typed) => {
    expect(parseTypedDate(typed)).toBe("2026-11-07");
  });

  it("reads the day first, never the month (7/11 is not 11 July)", () => {
    expect(parseTypedDate("7/11/2026")).toBe("2026-11-07");
    expect(parseTypedDate("11/7/2026")).toBe("2026-07-11");
  });

  it("refuses impossible dates and other text", () => {
    expect(parseTypedDate("31/11/2026")).toBeNull();
    expect(parseTypedDate("29/2/2026")).toBeNull();
    expect(parseTypedDate("29/2/2028")).toBe("2028-02-29");
    expect(parseTypedDate("2026-11-07")).toBeNull();
    expect(parseTypedDate("tomorrow")).toBeNull();
  });

  it("prints dd/MM/yyyy (FR-055)", () => {
    expect(formatNumericDate("2026-11-07")).toBe("07/11/2026");
    expect(stripIsolates(formatNumericRange("2026-10-25", "2026-10-31"))).toBe("25/10/2026 – 31/10/2026");
    expect(formatNumericRange("2026-10-25", "2026-10-31").startsWith(LRI)).toBe(true);
  });
});

describe("calendar arithmetic (FR-054, D6)", () => {
  it("knows the weekday and starts weeks on Sunday", () => {
    expect(weekday("2026-11-07")).toBe(6);
    expect(weekday("2026-11-01")).toBe(0);
    expect(startOfWeek("2026-11-07")).toBe("2026-11-01");
    expect(endOfWeek("2026-11-01")).toBe("2026-11-07");
  });

  it("moves by days and months without a device zone", () => {
    expect(addDays("2026-10-31", 1)).toBe("2026-11-01");
    expect(addDays("2027-03-26", 1)).toBe("2027-03-27"); // the clocks change in Israel that night
    expect(addMonths("2026-11-07", -1)).toBe("2026-10-01");
    expect(addMonths("2026-11-07", 2)).toBe("2027-01-01");
    expect(endOfMonth("2026-02-10")).toBe("2026-02-28");
    expect(daysInMonth(2028, 2)).toBe(29);
  });

  it("reads today in the site's zone", () => {
    expect(todayIn(ZONE, NOW)).toBe("2026-11-07");
    // 23:30 UTC on the 7th is already the 8th in Jerusalem.
    expect(todayIn(ZONE, new Date("2026-11-07T23:30:00Z"))).toBe("2026-11-08");
    expect(todayIn("UTC", new Date("2026-11-07T23:30:00Z"))).toBe("2026-11-07");
  });

  it("spells out headers in both languages (FR-055)", () => {
    expect(formatHeaderDate("2026-11-07", "en")).toBe("Saturday, 7 November");
    expect(formatHeaderDate("2026-11-07", "he")).toContain("שבת");
    expect(formatHeaderDate("2026-11-07", "he")).toContain("נובמבר");
    expect(weekdayNames("en", "long")[0]).toBe("Sunday");
    expect(weekdayNames("he", "long")).toHaveLength(7);
    expect(monthNames("he")[10]).toBe("נובמבר");
  });
});

describe("range presets (US5, FR-051)", () => {
  const today = "2026-11-07";

  it("calculates every preset from today with Sunday weeks", () => {
    expect(presetRange("today", today)).toEqual({ start: "2026-11-07", end: "2026-11-07" });
    expect(presetRange("yesterday", today)).toEqual({ start: "2026-11-06", end: "2026-11-06" });
    expect(presetRange("this-week", today)).toEqual({ start: "2026-11-01", end: "2026-11-07" });
    expect(presetRange("last-week", today)).toEqual({ start: "2026-10-25", end: "2026-10-31" });
    expect(presetRange("last-7-days", today)).toEqual({ start: "2026-11-01", end: "2026-11-07" });
    expect(presetRange("last-30-days", today)).toEqual({ start: "2026-10-09", end: "2026-11-07" });
    expect(presetRange("this-month", today)).toEqual({ start: "2026-11-01", end: "2026-11-30" });
    expect(presetRange("last-month", today)).toEqual({ start: "2026-10-01", end: "2026-10-31" });
    expect(presetRange("last-pay-period", today)).toEqual(presetRange("last-month", today));
  });

  it("plans ahead from today, never starting in the past (a schedule run, FR-042)", () => {
    expect(presetRange("next-week", today)).toEqual({ start: "2026-11-08", end: "2026-11-14" });
    expect(presetRange("next-7-days", today)).toEqual({ start: "2026-11-07", end: "2026-11-13" });
    // Thirty days, inside a run's 31-day limit.
    expect(presetRange("next-30-days", today)).toEqual({ start: "2026-11-07", end: "2026-12-06" });
    expect(presetFor({ start: "2026-11-08", end: "2026-11-14" }, today, FORWARD_PRESETS)).toBe("next-week");
  });

  it("names the preset a range stands for, else custom", () => {
    expect(presetFor({ start: "2026-10-25", end: "2026-10-31" }, today)).toBe("last-week");
    expect(presetFor({ start: "2026-10-20", end: "2026-10-31" }, today)).toBe("custom");
    // This week and the last 7 days coincide on a Saturday; the first in the list wins.
    expect(presetFor({ start: "2026-11-01", end: "2026-11-07" }, today)).toBe("this-week");
  });
});

describe("times (FR-053, D5, D8)", () => {
  it("prints 24-hour times in the site's zone inside a left-to-right isolate", () => {
    expect(formatTime("2026-11-07T04:00:00Z", ZONE)).toBe(`${LRI}06:00${PDI}`);
    expect(formatTimeRange("2026-11-07T04:00:00Z", "2026-11-07T12:00:00Z", ZONE)).toBe(`${LRI}06:00–14:00${PDI}`);
  });

  it("marks an end on the next day with +1", () => {
    const start = "2026-11-07T20:00:00Z"; // 22:00 in Jerusalem
    const end = "2026-11-08T04:00:00Z"; // 06:00 the next day
    expect(dayOffset(start, end, ZONE)).toBe(1);
    expect(stripIsolates(formatTimeRange(start, end, ZONE))).toBe("22:00–06:00 +1");
  });

  it("splits durations and notices a clock change", () => {
    expect(durationParts(510)).toEqual({ hours: 8, minutes: 30 });
    // Israel moves to winter time on the night of 24 to 25 October 2026.
    expect(crossesClockChange("2026-10-24T19:00:00Z", "2026-10-25T03:00:00Z", ZONE)).toBe(true);
    expect(crossesClockChange("2026-11-07T04:00:00Z", "2026-11-07T12:00:00Z", ZONE)).toBe(false);
  });
});

describe("mixed direction (FR-056)", () => {
  it("wraps text in isolates", () => {
    expect(isolate("Dana")).toBe(`${FSI}Dana${PDI}`);
    expect(isolateLtr("050-1234567")).toBe(`${LRI}050-1234567${PDI}`);
    expect(stripIsolates(isolateLtr("x"))).toBe("x");
  });
});

describe("money from the decimal string (FR-058, D10)", () => {
  it("splits and groups digits without arithmetic", () => {
    expect(splitDecimal("1234.5")).toEqual({ negative: false, integer: "1234", fraction: "5" });
    expect(splitDecimal("-0.10")).toEqual({ negative: true, integer: "0", fraction: "10" });
    expect(splitDecimal("-0.00").negative).toBe(false);
    expect(groupDigits("1234567", ",")).toBe("1,234,567");
    expect(groupDigits("12", ",")).toBe("12");
    expect(() => splitDecimal("12,5")).toThrow();
  });

  it("formats in English and Hebrew exactly to the agorot", () => {
    expect(stripIsolates(formatMoney("1234.5", "en"))).toBe("₪1,234.50");
    expect(stripIsolates(formatMoney("45", "en"))).toBe("₪45.00");
    expect(stripIsolates(formatMoney("-45.1", "en"))).toBe("-₪45.10");
    const hebrew = stripIsolates(formatMoney("1234.5", "he"));
    expect(hebrew).toContain("1,234.50");
    expect(hebrew).toContain("₪");
    expect(formatMoney("1", "he").startsWith(LRI)).toBe(true);
  });

  it("keeps every decimal place the server sent and never rounds", () => {
    expect(stripIsolates(formatMoney("45.1234", "en"))).toBe("₪45.1234");
    expect(stripIsolates(formatMoney("0.125", "en"))).toBe("₪0.125");
    expect(stripIsolates(formatMoney("12345678901234567890.99", "en"))).toBe("₪12,345,678,901,234,567,890.99");
  });
});

describe("Israeli holidays (FR-054, D6)", () => {
  it("finds the days of rest, their eves and the intermediate days", () => {
    const found = holidaysBetween("2026-09-01", "2026-10-31", "en");
    expect(found.get("2026-09-11")?.kind).toBe("eve"); // Erev Rosh Hashana 5787
    expect(found.get("2026-09-12")?.kind).toBe("holiday");
    expect(found.get("2026-09-21")?.kind).toBe("holiday"); // Yom Kippur
    expect(found.get("2026-09-26")?.kind).toBe("holiday"); // Sukkot I
    expect(found.get("2026-09-27")?.kind).toBe("intermediate");
    expect(found.get("2026-10-03")?.kind).toBe("holiday"); // Shmini Atzeret (Israel)
    expect(found.get("2026-10-04")).toBeUndefined(); // Simchat Torah falls with Shmini Atzeret in Israel
    expect(found.get("2026-10-20")).toBeUndefined();
  });

  it("includes Independence Day and names days in Hebrew", () => {
    const found = holidaysBetween("2027-04-01", "2027-05-31", "he");
    expect(found.get("2027-05-12")?.kind).toBe("holiday"); // Yom HaAtzmaut 5787
    expect(found.get("2027-04-22")?.name).toContain("פסח"); // Pesach I
  });
});

test("a short range drops the first date's year when both share it (R79)", () => {
  expect(formatShortNumericRange("2026-12-06", "2026-12-12")).toBe("⁦06/12 – 12/12/2026⁩");
  expect(formatShortNumericRange("2026-12-27", "2027-01-02")).toBe("⁦27/12/2026 – 02/01/2027⁩");
});
