/** My pay's arithmetic (specs/007-ux-redesign FR-024): exact, never through floating point. */
import { amount, breakdownByRate, cents, changeSince, fromCents, nextPeriod, payUpTo, previousPeriod, sameDayBefore, shortHours, units, type PayLine } from "../../../src/screens/my-work/pay/model";

const line = (base_rate: string | null, parts: { hours: string; rate_pct: string; pay?: string | null }[], pay: string | null, pay_enabled = true): PayLine => ({
  base_rate,
  date: "2026-11-03",
  starts_at: "2026-11-03T06:00:00Z",
  ends_at: "2026-11-03T14:00:00Z",
  hours: parts.reduce((sum, part) => sum + Number(part.hours), 0).toString(),
  parts,
  pay,
  pay_enabled,
  project_id: "p",
  project_name: "Night Watch",
  rate_missing: base_rate === null,
  shift_id: "s",
  timezone: "Asia/Jerusalem",
});

it("reads and writes ten-thousandths exactly", () => {
  expect(units("1234.5")).toBe(12345000n);
  expect(units("0.1") + units("0.2")).toBe(units("0.3"));
  expect(amount(12345000n)).toBe("1234.5000");
  expect(amount(-5n)).toBe("-0.0005");
});

it("adds the parts' pay up by rate in agorot, so the rows meet the total by construction", () => {
  // The API prices each part to cents and makes a line's parts sum to its pay (price.py).
  const lines = [
    line("50.00", [{ hours: "8", rate_pct: "100", pay: "400.00" }], "400.00"),
    line("50.00", [{ hours: "6", rate_pct: "100", pay: "300.00" }, { hours: "2", rate_pct: "125", pay: "125.00" }, { hours: "1.5", rate_pct: "150", pay: "112.50" }], "537.50"),
    line("33.33", [{ hours: "7", rate_pct: "100", pay: "233.31" }, { hours: "1", rate_pct: "150", pay: "50.00" }], "283.31"),
  ];
  const rows = breakdownByRate({ lines });
  expect(rows).toEqual([
    { ratePct: "100", hours: "21", amount: "933.31" },
    { ratePct: "125", hours: "2", amount: "125.00" },
    { ratePct: "150", hours: "2.5", amount: "162.50" },
  ]);
  const total = lines.reduce((sum, l) => sum + cents(l.pay!), 0n);
  expect(rows.reduce((sum, row) => sum + cents(row.amount), 0n)).toBe(total);
  expect(fromCents(total)).toBe("1220.81");
});

it("reads and writes agorot exactly", () => {
  expect(cents("1220.81")).toBe(122081n);
  expect(cents("0.1") + cents("0.2")).toBe(cents("0.3"));
  expect(fromCents(5n)).toBe("0.05");
  expect(fromCents(-122081n)).toBe("-1220.81");
});

it("tells the change since the last period and walks the periods", () => {
  // In agorot: the API prices to agorot, and four decimals read as noise (the owner, 2026-11-13).
  expect(changeSince("1220.8300", "1000.0000")).toBe("220.83");
  expect(changeSince("900", "1000.5")).toBe("-100.50");
  expect(changeSince("8550.0000", "17100.0000")).toBe("-8550.00");
  expect(changeSince(null, "1")).toBeNull();
  expect(previousPeriod("2026-01")).toBe("2025-12");
  expect(nextPeriod("2026-12")).toBe("2027-01");
  expect(shortHours("37.5000")).toBe("37.5");
  expect(shortHours("8.0000")).toBe("8");
});

test("a month in progress compares with the same days of the month before", () => {
  const line = (date: string, pay: string | null, enabled = true) => ({ date, pay, pay_enabled: enabled }) as unknown as PayLine;
  const before = { lines: [line("2026-10-02", "400.00"), line("2026-10-13", "300.50"), line("2026-10-14", "999.00"), line("2026-10-05", null), line("2026-10-06", "50.00", false)] };
  expect(payUpTo(before, "2026-10-13")).toBe("700.50");
  expect(sameDayBefore("2026-11", 13)).toBe("2026-10-13");
  expect(sameDayBefore("2027-03", 31)).toBe("2027-02-28");
  expect(sameDayBefore("2026-01", 15)).toBe("2025-12-15");
});
