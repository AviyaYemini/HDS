/**
 * The date pickers (specs/007-ux-redesign US5, FR-051, FR-052, task T023): typed entry day first,
 * the calendar grid's shape and keyboard steps, presets that agree with their dates, and Custom
 * after a hand change.
 */
import { fireEvent, render, screen } from "@testing-library/react-native";

import { DateField, DateRangeField, monthGrid, sameDayInMonth, stepDate } from "../../../src/design/components";

const TODAY = "2026-11-07";

describe("calendar arithmetic", () => {
  it("lays November 2026 out from Sunday with blanks before the 1st", async () => {
    const grid = monthGrid("2026-11-01");
    expect(grid).toHaveLength(6);
    expect(grid[0]).toEqual(["2026-11-01", "2026-11-02", "2026-11-03", "2026-11-04", "2026-11-05", "2026-11-06", "2026-11-07"]);
    const october = monthGrid("2026-10-01");
    expect(october[0]!.slice(0, 4)).toEqual([null, null, null, null]);
    expect(october[0]![4]).toBe("2026-10-01");
  });

  it("steps days inside the allowed range and keeps the day of month across months", async () => {
    expect(stepDate("2026-11-30", 1, null, null)).toBe("2026-12-01");
    expect(stepDate("2026-11-07", 7, null, "2026-11-10")).toBe("2026-11-10");
    expect(sameDayInMonth("2026-10-31", 1)).toBe("2026-11-30");
    expect(sameDayInMonth("2026-11-15", 1)).toBe("2026-12-15");
    expect(sameDayInMonth("2028-02-29", -12)).toBe("2027-02-28");
  });
});

describe("DateField", () => {
  it("reads a typed date with any separator as day, month, year and prints dd/MM/yyyy", async () => {
    const onChange = jest.fn();
    await render(<DateField label="Start date" value={null} onChange={onChange} today={TODAY} testID="d" />);
    const input = screen.getByTestId("d");
    await fireEvent.changeText(input, "7.11.2026");
    await fireEvent(input, "blur");
    expect(onChange).toHaveBeenCalledWith("2026-11-07");
    expect(input.props.value).toBe("07/11/2026");
  });

  it("refuses an impossible date with a reason", async () => {
    const onChange = jest.fn();
    await render(<DateField label="Start date" value={null} onChange={onChange} today={TODAY} testID="d" />);
    const input = screen.getByTestId("d");
    await fireEvent.changeText(input, "31/11/2026");
    await fireEvent(input, "blur");
    expect(onChange).not.toHaveBeenCalled();
    expect(screen.getByRole("alert")).toHaveTextContent(/day\/month\/year/);
  });

  it("offers the calendar as a dialog", async () => {
    await render(<DateField label="Start date" value="2026-11-07" onChange={() => undefined} today={TODAY} testID="d" />);
    expect(screen.getByTestId("d").props.value).toBe("07/11/2026");
    await fireEvent.press(screen.getByTestId("d-open"));
    expect(screen.getByTestId("d-dialog")).toBeTruthy();
    expect(screen.getAllByLabelText(/7 November 2026/).length).toBeGreaterThan(0);
  });
});

describe("DateRangeField", () => {
  it("applies a preset, shows its dates and turns to Custom on a hand change", async () => {
    const onChange = jest.fn();
    const { rerender } = await render(
      <DateRangeField label="Range" value={{ start: "2026-11-01", end: "2026-11-07" }} onChange={onChange} today={TODAY} testID="r" />,
    );
    expect(screen.getByRole("radio", { name: "This week", checked: true })).toBeTruthy();
    await fireEvent.press(screen.getByTestId("preset-last-week"));
    expect(onChange).toHaveBeenLastCalledWith({ start: "2026-10-25", end: "2026-10-31" });
    await rerender(<DateRangeField label="Range" value={{ start: "2026-10-25", end: "2026-10-31" }} onChange={onChange} today={TODAY} testID="r" />);
    expect(screen.getByRole("radio", { name: "Last week", checked: true })).toBeTruthy();
    expect(screen.getByTestId("r-summary")).toHaveTextContent(/25\/10\/2026 – 31\/10\/2026/);
    await rerender(<DateRangeField label="Range" value={{ start: "2026-10-20", end: "2026-10-31" }} onChange={onChange} today={TODAY} testID="r" />);
    expect(screen.getByRole("radio", { name: "Custom", checked: true })).toBeTruthy();
  });

  it("refuses an end before the start when typed (FR-051)", async () => {
    const onChange = jest.fn();
    await render(<DateRangeField label="Range" value={{ start: "2026-11-01", end: "2026-11-07" }} onChange={onChange} today={TODAY} testID="r" />);
    const to = screen.getByTestId("r-to");
    await fireEvent.changeText(to, "20/10/2026");
    await fireEvent(to, "blur");
    expect(onChange).not.toHaveBeenCalled();
    expect(screen.getAllByRole("alert").some((node) => /before the start/.test(String(node.props.children)))).toBe(true);
  });
});
