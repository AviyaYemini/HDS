/**
 * An admin table's state as the web address holds it (specs/007-ux-redesign FR-049, A19): the search
 * (`q`), the sort (`sort=name:desc`), the saved view (`view`) and the open record (`open=<id>` or
 * `open=new`). Pure, so the screen and the tests read it the same way.
 */
import type { SortDirection } from "../../design/components/Table";

export interface TableState {
  q: string;
  sort: { key: string; direction: SortDirection } | null;
  view: string | null;
  open: string | null;
}

type Params = Record<string, string | string[] | undefined>;

function one(value: string | string[] | undefined): string | null {
  const text = Array.isArray(value) ? value[0] : value;
  return text === undefined || text === "" ? null : text;
}

/** The sort `key:direction`, when `key` is one of `sortable`. */
export function parseSort(raw: string | null, sortable: readonly string[]): TableState["sort"] {
  if (raw === null) return null;
  const [key, direction] = raw.split(":");
  if (!key || !sortable.includes(key)) return null;
  return { key, direction: direction === "desc" ? "desc" : "asc" };
}

/** The table's state from the address. */
export function readState(params: Params, sortable: readonly string[]): TableState {
  return {
    q: one(params.q) ?? "",
    sort: parseSort(one(params.sort), sortable),
    view: one(params.view),
    open: one(params.open),
  };
}

/** The address params for `state` (unset ones as undefined, so they leave the address). */
export function toParams(state: TableState): Record<string, string | undefined> {
  return {
    q: state.q || undefined,
    sort: state.sort ? `${state.sort.key}:${state.sort.direction}` : undefined,
    view: state.view ?? undefined,
    open: state.open ?? undefined,
  };
}

/** What a saved view keeps: the search and the sort, as a query string (not the open record). */
export function viewQuery(state: Pick<TableState, "q" | "sort">): string {
  const query = new URLSearchParams();
  if (state.q) query.set("q", state.q);
  if (state.sort) query.set("sort", `${state.sort.key}:${state.sort.direction}`);
  return query.toString();
}

/** A saved view's search and sort, read back. */
export function fromViewQuery(query: string, sortable: readonly string[]): Pick<TableState, "q" | "sort"> {
  const params = new URLSearchParams(query);
  return { q: params.get("q") ?? "", sort: parseSort(params.get("sort"), sortable) };
}

/** Rows whose searchable text holds every word of `q`, in any order, ignoring case. */
export function searched<Row>(rows: readonly Row[], q: string, text: (row: Row) => string): Row[] {
  const words = q.toLowerCase().split(/\s+/).filter(Boolean);
  if (words.length === 0) return [...rows];
  return rows.filter((row) => {
    const haystack = text(row).toLowerCase();
    return words.every((word) => haystack.includes(word));
  });
}

/** Rows ordered by `sort`, using `value` per column; numbers as numbers, text by the locale. */
export function sorted<Row>(
  rows: readonly Row[],
  sort: TableState["sort"],
  value: (row: Row, key: string) => string | number | null,
  locale: string,
): Row[] {
  if (sort === null) return [...rows];
  const collator = new Intl.Collator(locale, { numeric: true, sensitivity: "base" });
  const sign = sort.direction === "asc" ? 1 : -1;
  return [...rows].sort((a, b) => {
    const left = value(a, sort.key);
    const right = value(b, sort.key);
    if (left === right) return 0;
    if (left === null) return 1;
    if (right === null) return -1;
    if (typeof left === "number" && typeof right === "number") return (left - right) * sign;
    return collator.compare(String(left), String(right)) * sign;
  });
}
