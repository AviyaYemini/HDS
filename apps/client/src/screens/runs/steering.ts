/**
 * Steering a run, its pure parts (specs/008-auto-scheduler-review FR-017 to FR-026): a chip in words,
 * the summary's figures, and which of a person's numbers stand out. The server judges; these only
 * shape what the screen says.
 */
import type { components } from "@hds/api-client";

export type Chip = components["schemas"]["ChipIn"];
export type ChipKind = Chip["kind"];
export type Controls = components["schemas"]["Controls"];
export type Fairness = components["schemas"]["Fairness"];

/** The kinds a manager may add, in the order the editor offers them (FR-020). */
export const CHIP_KINDS: readonly ChipKind[] = ["max_shifts", "max_nights", "max_hours", "keep_on", "keep_off", "prefer"];

/** A run takes at most this many chips (FR-020). */
export const MAX_CHIPS = 20;

/** The plain controls when untouched: today's order (FR-017). */
export const DEFAULT_CONTROLS: Controls = { keep_changes_small: false, wishes_strictly: true };

/** What a chip needs besides its person, by kind. */
export function needs(kind: ChipKind): "count" | "place" | "when" {
  if (kind === "max_shifts" || kind === "max_nights" || kind === "max_hours") return "count";
  return kind === "keep_on" ? "place" : "when";
}

/** Whether a chip has what its kind needs. */
export function complete(chip: Partial<Chip>): chip is Chip {
  if (!chip.kind || !chip.user_id) return false;
  const what = needs(chip.kind);
  if (what === "count") return chip.count != null && chip.count >= 0;
  if (what === "place") return !!chip.project_id || !!chip.site_id;
  return chip.weekday != null || !!chip.shift_type_id;
}

/** The weekday's name in ``language``, counting from Sunday (0) as HDS weeks do. */
export function weekdayName(weekday: number, language: string): string {
  // 2027-06-06 is a Sunday.
  const day = new Date(Date.UTC(2027, 5, 6 + weekday, 12));
  return new Intl.DateTimeFormat(language, { weekday: "long", timeZone: "UTC" }).format(day);
}

/** The translation key and values that say a chip in words ("Dana: at most 3 nights"). */
export function chipWords(
  chip: Chip,
  name: (id: string) => string,
  place: (id: string) => string,
  type: (id: string) => string,
  language: string,
): { key: string; values: Record<string, string | number> } {
  const person = name(chip.user_id);
  const when = chip.weekday != null ? weekdayName(chip.weekday, language) : chip.shift_type_id ? type(chip.shift_type_id) : "";
  switch (chip.kind) {
    case "keep_on":
      return { key: "runs.chip.keep_on", values: { person, place: place(chip.project_id ?? chip.site_id ?? "") } };
    case "keep_off":
    case "prefer":
      return { key: `runs.chip.${chip.kind}`, values: { person, when } };
    default:
      return { key: `runs.chip.${chip.kind}`, values: { person, count: chip.count ?? 0 } };
  }
}

/** A total's average over ``people``, to one decimal place at most; 0 with no one. */
export function average(total: number, people: number): number {
  return people > 0 ? Math.round((total / people) * 10) / 10 : 0;
}
