/** The record form's checks and money text (specs/007-ux-redesign FR-045, FR-046). */
import { fieldError, moneyChanges, moneyText } from "../../../src/screens/admin/RecordForm";

const t = (key: string) => key;

test("an amount stored with four places shows two when the rest are zeros, never rounded", () => {
  expect(moneyText("75.0000")).toBe("75.00");
  expect(moneyText("48.1250")).toBe("48.1250");
  expect(moneyText("12")).toBe("12");
});

test("each kind checks its format; required fields say so; a field's own check runs last", () => {
  expect(fieldError({ name: "n", label: "N", required: true }, { n: " " }, t)).toBe("form.required");
  expect(fieldError({ name: "r", label: "R", kind: "money" }, { r: "75.0000" }, t)).toBeNull();
  expect(fieldError({ name: "r", label: "R", kind: "money" }, { r: "7.5.0" }, t)).toBe("form.money_invalid");
  expect(fieldError({ name: "s", label: "S", kind: "time" }, { s: "25:00" }, t)).toBe("form.time_invalid");
  expect(fieldError({ name: "e", label: "E", kind: "email" }, { e: "a@b.co" }, t)).toBeNull();
  expect(fieldError({ name: "r", label: "R", kind: "money", validate: (v) => (Number(v) > 0 ? null : "zero") }, { r: "0" }, t)).toBe("zero");
});

test("only a changed pay field asks for confirmation", () => {
  const fields = [
    { name: "rate", label: "Rate", kind: "money" as const },
    { name: "rest", label: "Rest", kind: "percent" as const },
    { name: "name", label: "Name" },
  ];
  expect(moneyChanges(fields, { rate: "48.00", rest: "150", name: "a" }, { rate: "49.00", rest: "150", name: "b" })).toEqual([{ field: fields[0], from: "48.00", to: "49.00" }]);
});
