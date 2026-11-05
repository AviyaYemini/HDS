// Shift times are shown in the site's zone, across midnight and the DST changes (FR-123).
import { formatDay, formatInstant, shiftTimes } from "../../src/time";

const ZONE = "Asia/Jerusalem";

test("a day shift in the site's zone", () => {
  expect(shiftTimes("2027-06-07T04:00:00Z", "2027-06-07T12:00:00Z", ZONE, "en")).toEqual({
    day: "Mon, Jun 7",
    start: "07:00",
    end: "15:00",
    endDay: null,
  });
});

test("a night shift crossing midnight names the day it ends", () => {
  expect(shiftTimes("2027-06-07T19:00:00Z", "2027-06-08T03:00:00Z", ZONE, "en")).toEqual({
    day: "Mon, Jun 7",
    start: "22:00",
    end: "06:00",
    endDay: "Tue, Jun 8",
  });
});

test("the night the clocks go forward", () => {
  // 2027-03-26 02:00 local becomes 03:00: eight hours from midnight end at 09:00.
  expect(shiftTimes("2027-03-25T22:00:00Z", "2027-03-26T06:00:00Z", ZONE, "en")).toMatchObject({
    day: "Fri, Mar 26",
    start: "00:00",
    end: "09:00",
    endDay: null,
  });
});

test("the night the clocks go back", () => {
  // 2027-10-31 02:00 local becomes 01:00: nine hours from midnight end at 08:00.
  expect(shiftTimes("2027-10-30T21:00:00Z", "2027-10-31T06:00:00Z", ZONE, "en")).toMatchObject({
    day: "Sun, Oct 31",
    start: "00:00",
    end: "08:00",
  });
});

test("the same shift reads differently in another zone", () => {
  expect(shiftTimes("2027-06-07T04:00:00Z", "2027-06-07T12:00:00Z", "UTC", "en")).toMatchObject({
    start: "04:00",
    end: "12:00",
  });
});

test("a calendar date is shown as that date wherever the device is", () => {
  expect(formatDay("2027-06-06", "en")).toBe("Sun, Jun 6");
});

test("an instant is shown with its date and time", () => {
  expect(formatInstant("2027-06-01T09:30:00Z", "en", "UTC")).toMatch(/Jun 1, 2027.*9:30/);
});
