/**
 * The two locale files stay complete and match the glossary (specs/007-ux-redesign FR-075, FR-076,
 * SC-012, task T011).
 *
 * - `he.json` and `en.json` have the same keys (today Hebrew silently falls back to English);
 * - no value is empty, equal to its own key, or a placeholder (`TODO`, `TBD`, `FIXME`, `lorem`, `xxx`);
 * - every key the glossary (`docs/ux/glossary.md`) names carries the glossary's words, once the key
 *   exists in the English file.
 */
import { readFileSync } from "node:fs";
import { join } from "node:path";

import en from "../../src/locales/en.json";
import he from "../../src/locales/he.json";

type Strings = Record<string, string>;

const PLACEHOLDER = /\b(TODO|TBD|FIXME|lorem|xxx)\b/i;

/** Keys in one file and not the other, both ways. */
export function missingKeys(a: Strings, b: Strings): { onlyA: string[]; onlyB: string[] } {
  const inA = new Set(Object.keys(a));
  const inB = new Set(Object.keys(b));
  return {
    onlyA: [...inA].filter((k) => !inB.has(k)).sort(),
    onlyB: [...inB].filter((k) => !inA.has(k)).sort(),
  };
}

/** Values that are empty, equal to their key, or placeholders. */
export function badValues(strings: Strings): string[] {
  return Object.entries(strings)
    .filter(([key, value]) => typeof value !== "string" || !value.trim() || value === key || PLACEHOLDER.test(value))
    .map(([key]) => key)
    .sort();
}

/** The glossary rows that name a locale key: key → [English, Hebrew]. */
export function glossaryTerms(markdown: string): Map<string, [string, string]> {
  const terms = new Map<string, [string, string]>();
  for (const line of markdown.split("\n")) {
    const cells = line.split("|").map((cell) => cell.trim());
    // | `key` | Term | עברית | Avoid |  →  ["", "`key`", "Term", "עברית", "Avoid", ""]
    const key = cells[1]?.match(/^`([\w.]+)`$/)?.[1];
    if (key && cells[2] && cells[3]) terms.set(key, [cells[2], cells[3]]);
  }
  return terms;
}

const glossary = readFileSync(join(__dirname, "../../../../docs/ux/glossary.md"), "utf8");

describe("locale files (FR-076)", () => {
  it("detects a missing key and a placeholder, so the checks can fail", () => {
    expect(missingKeys({ a: "A", b: "B" }, { a: "א" })).toEqual({ onlyA: ["b"], onlyB: [] });
    expect(badValues({ ok: "Fine", empty: " ", same: "same", todo: "TODO later" })).toEqual(["empty", "same", "todo"]);
  });

  it("have the same keys in Hebrew and English", () => {
    expect(missingKeys(en as Strings, he as Strings)).toEqual({ onlyA: [], onlyB: [] });
  });

  it("have no empty or placeholder values", () => {
    expect(badValues(en as Strings)).toEqual([]);
    expect(badValues(he as Strings)).toEqual([]);
  });
});

describe("glossary (FR-075)", () => {
  const terms = glossaryTerms(glossary);

  it("is read from docs/ux/glossary.md", () => {
    expect(terms.get("nav.roster")).toEqual(["Roster", "סידור עבודה"]);
    expect(terms.get("nav.work_log")).toEqual(["Work log", "יומן עבודה"]);
  });

  it("is what every key it names says, in both languages", () => {
    const wrong: string[] = [];
    for (const [key, [english, hebrew]] of terms) {
      if (!(key in en)) continue; // the screen that needs it has not been built yet
      if ((en as Strings)[key] !== english) wrong.push(`en ${key}: "${(en as Strings)[key]}" ≠ "${english}"`);
      if ((he as Strings)[key] !== hebrew) wrong.push(`he ${key}: "${(he as Strings)[key] ?? "(missing)"}" ≠ "${hebrew}"`);
    }
    expect(wrong).toEqual([]);
  });
});
