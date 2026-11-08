/** An admin table's state in the address (specs/007-ux-redesign FR-049). */
import { fromViewQuery, readState, searched, sorted, toParams, viewQuery } from "../../../src/screens/admin/tableState";

const SORTABLE = ["name", "rate"];

test("the address holds the search, the sort, the view and the open record", () => {
  const state = readState({ q: "Employee", sort: "name:desc", view: "v1", open: "abc" }, SORTABLE);
  expect(state).toEqual({ q: "Employee", sort: { key: "name", direction: "desc" }, view: "v1", open: "abc" });
  expect(toParams(state)).toEqual({ q: "Employee", sort: "name:desc", view: "v1", open: "abc" });
  // An unknown column is no sort; empty values leave the address.
  expect(readState({ sort: "password:asc", q: "" }, SORTABLE)).toEqual({ q: "", sort: null, view: null, open: null });
  expect(toParams({ q: "", sort: null, view: null, open: null })).toEqual({ q: undefined, sort: undefined, view: undefined, open: undefined });
});

test("a saved view keeps the search and the sort, not the open record", () => {
  const query = viewQuery({ q: "night shift", sort: { key: "rate", direction: "asc" } });
  expect(fromViewQuery(query, SORTABLE)).toEqual({ q: "night shift", sort: { key: "rate", direction: "asc" } });
});

test("search matches every word in any order; sort orders numbers as numbers", () => {
  const rows = [
    { name: "Dana Levi", rate: 48 },
    { name: "Avi Cohen", rate: 9 },
    { name: "Levi Dana-Bar", rate: null },
  ];
  expect(searched(rows, "levi dana", (r) => r.name).map((r) => r.name)).toEqual(["Dana Levi", "Levi Dana-Bar"]);
  const byRate = sorted(rows, { key: "rate", direction: "asc" }, (r, key) => (key === "rate" ? r.rate : r.name), "en");
  expect(byRate.map((r) => r.rate)).toEqual([9, 48, null]);
  const byName = sorted(rows, { key: "name", direction: "desc" }, (r) => r.name, "en");
  expect(byName.map((r) => r.name)).toEqual(["Levi Dana-Bar", "Dana Levi", "Avi Cohen"]);
});
