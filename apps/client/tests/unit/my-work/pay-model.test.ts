/** My pay's arithmetic (specs/007-ux-redesign FR-024): exact, never through floating point. */
import { amount, breakdownByRate, changeSince, nextPeriod, previousPeriod, shortHours, units, type PayLine } from "../../../src/screens/my-work/pay/model";

const line = (base_rate: string | null, parts: { hours: string; rate_pct: string }[], pay: string | null, pay_enabled = true): PayLine => ({
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

it("breaks the pay down by rate so the rows add up to the total exactly", () => {
  const lines = [
    line("50.0000", [{ hours: "8", rate_pct: "100" }], "400.0000"),
    line("50.0000", [{ hours: "6", rate_pct: "100" }, { hours: "2", rate_pct: "125" }, { hours: "1.5", rate_pct: "150" }], "537.5000"),
    line("33.3333", [{ hours: "7", rate_pct: "100" }, { hours: "1", rate_pct: "150" }], "283.3331"),
  ];
  const total = "1220.8331";
  const rows = breakdownByRate({ lines, pay: total });
  expect(rows.map((row) => row.ratePct)).toEqual(["100", "125", "150"]);
  expect(rows.map((row) => row.hours)).toEqual(["21", "2", "2.5"]);
  const sum = rows.reduce((acc, row) => acc + units(row.amount), 0n);
  expect(sum).toBe(units(total));
  expect(rows[1]!.amount).toBe("125.0000");
});

it("leaves out lines without pay and copes with no total", () => {
  const rows = breakdownByRate({ lines: [line(null, [{ hours: "8", rate_pct: "100" }], null), line("40", [{ hours: "2", rate_pct: "100" }], "80.0000", false), line("40", [{ hours: "3", rate_pct: "100" }], "120.0000")], pay: "120.0000" });
  expect(rows).toEqual([{ ratePct: "100", hours: "3", amount: "120.0000" }]);
  expect(breakdownByRate({ lines: [], pay: null })).toEqual([]);
});

it("tells the change since the last period and walks the periods", () => {
  expect(changeSince("1220.8331", "1000.0000")).toBe("220.8331");
  expect(changeSince("900", "1000.5")).toBe("-100.5000");
  expect(changeSince(null, "1")).toBeNull();
  expect(previousPeriod("2026-01")).toBe("2025-12");
  expect(nextPeriod("2026-12")).toBe("2027-01");
  expect(shortHours("37.5000")).toBe("37.5");
  expect(shortHours("8.0000")).toBe("8");
});
