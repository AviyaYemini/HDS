/**
 * A proposal under review, its pure parts (specs/008-auto-scheduler-review FR-001 to FR-012): the
 * slots by day, what a person's menu may offer, which pairs are still open, and the part a choice
 * of days, projects or pairs accepts. The server decides; these only shape what the screen shows
 * and sends.
 */
import type { components } from "@hds/api-client";

export type Review = components["schemas"]["Review"];
export type ReviewSlot = components["schemas"]["ReviewSlot"];
export type Mark = components["schemas"]["Mark"];
export type Edit = components["schemas"]["EditRequest"];
export type EditWarning = components["schemas"]["Warning"];
export type AcceptPart = components["schemas"]["AcceptRequest"];

/** A pair on a slot: who, and the slot's key. */
export interface Pair {
  key: string;
  userId: string;
}

/** The icon each mark shows beside its words (never colour alone). */
export const MARK_ICON = { added: "plus", changed: "arrow-next", removed: "minus", unchanged: "check" } as const;

/** The review's slots by local date, each day's by start time then key. */
export function slotsByDay(review: Pick<Review, "slots">): Map<string, ReviewSlot[]> {
  const days = new Map<string, ReviewSlot[]>();
  for (const slot of review.slots) {
    const list = days.get(slot.date) ?? [];
    list.push(slot);
    days.set(slot.date, list);
  }
  for (const list of days.values()) list.sort((a, b) => a.starts_at.localeCompare(b.starts_at) || a.key.localeCompare(b.key));
  return days;
}

/** The marks still open (not yet accepted, and with something to accept). */
export function openMarks(slot: ReviewSlot): Mark[] {
  return slot.marks.filter((m) => m.mark !== "unchanged" && !m.accepted);
}

/** Whether any pair of the review is still open. */
export function hasOpen(review: Pick<Review, "slots">): boolean {
  return review.slots.some((slot) => openMarks(slot).length > 0);
}

/** The days, and the projects, that still have open pairs, each once and in order. */
export function openDaysAndProjects(review: Pick<Review, "slots">): { days: string[]; projects: string[] } {
  const days = new Set<string>();
  const projects = new Set<string>();
  for (const slot of review.slots) {
    if (openMarks(slot).length === 0) continue;
    days.add(slot.date);
    projects.add(slot.project_id);
  }
  return { days: [...days].sort(), projects: [...projects].sort() };
}

/**
 * The pairs a chosen mark stands for: a changed mark is two pairs on one slot (the person added and
 * the one they replace); the others, one.
 */
export function pairsOf(slot: ReviewSlot, mark: Mark): Pair[] {
  const pairs = [{ key: slot.key, userId: mark.user_id }];
  if (mark.mark === "changed" && mark.replaces) pairs.push({ key: slot.key, userId: mark.replaces });
  return pairs;
}

/** The accept body for chosen pairs. */
export function partOfPairs(pairs: readonly Pair[]): AcceptPart {
  return { assignments: pairs.map((p) => ({ key: p.key, user_id: p.userId })) };
}

/** Whether a slot has a part accepted (its pairs differ from the run's, and agree with the roster). */
export function partAccepted(slot: ReviewSlot): boolean {
  return slot.marks.some((m) => m.mark !== "unchanged" && m.accepted);
}

/** People on the slot now, in the order the proposal lists them. */
export function peopleOn(slot: ReviewSlot): string[] {
  return slot.people.map((p) => p.user_id);
}

/** What a person's menu offers on a slot, for the edits of FR-008. */
export function menuFor(review: Pick<Review, "slots">, slot: ReviewSlot): { canEdit: boolean; targets: ReviewSlot[] } {
  if (partAccepted(slot)) return { canEdit: false, targets: [] };
  return { canEdit: true, targets: review.slots.filter((s) => s.key !== slot.key && !partAccepted(s)) };
}

/** A key string the translation files have for a warning's numbers, when it has numbers. */
export function numbersKey(values: Record<string, string> | undefined): string | null {
  const v = values ?? {};
  if ("rest_hours" in v) return "roster.warning.rest";
  if ("maximum_hours" in v) return "roster.warning.weekly";
  return null;
}

/** The breaks an edit was refused for, from the problem's `errors`, or none. */
export function breaksOf(error: unknown): EditWarning[] {
  if (typeof error !== "object" || error === null || !("errors" in error)) return [];
  const items = (error as { errors: unknown }).errors;
  if (!Array.isArray(items)) return [];
  return items
    .filter((e): e is { key: string; user_id: string | null; code: string; values?: Record<string, string> } => typeof e === "object" && e !== null && "code" in e && "key" in e)
    .map((e) => ({ key: e.key, user_id: e.user_id ?? null, code: e.code, values: e.values ?? {}, excused: false }));
}
