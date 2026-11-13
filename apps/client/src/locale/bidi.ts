/**
 * Mixed-direction text (specs/007-ux-redesign FR-056, D8).
 *
 * A name, time, phone number or amount inserted into a Hebrew sentence keeps its own direction when
 * it is wrapped in a Unicode isolate: FSI … PDI lets the text decide, LRI … PDI forces left to right
 * (times, phones, amounts), RLI … PDI right to left. Isolates are invisible and never reorder the
 * sentence around them.
 */

/** First Strong Isolate: the wrapped text decides its own direction. */
export const FSI = "⁨";
/** Left-to-Right Isolate. */
export const LRI = "⁦";
/** Right-to-Left Isolate. */
export const RLI = "⁧";
/** Pop Directional Isolate: closes any of the three. */
export const PDI = "⁩";

/** The direction an isolate forces, or `auto` to let the text decide. */
export type IsolateDirection = "auto" | "ltr" | "rtl";

/** `text` wrapped so it keeps its own direction inside a sentence of the other direction. */
export function isolate(text: string, direction: IsolateDirection = "auto"): string {
  const open = direction === "ltr" ? LRI : direction === "rtl" ? RLI : FSI;
  return `${open}${text}${PDI}`;
}

/** `text` forced left to right: times, phone numbers, amounts and codes (D8). */
export function isolateLtr(text: string): string {
  return isolate(text, "ltr");
}

/** `text` without any isolate characters, for comparisons and inputs. */
export function stripIsolates(text: string): string {
  return text.replace(/[⁦⁧⁨⁩]/g, "");
}

/** Times, time ranges and dates inside a name someone typed ("בוקר 07:00–19:00", "קורס 7 · 15.11"). */
const NUMERIC_RUN = /\d{1,2}[:.]\d{2}(?:\s*[–-]\s*\d{1,2}[:.]\d{2})?|\d{1,2}\/\d{1,2}(?:\/\d{2,4})?/g;

/**
 * A name someone typed, ready to sit inside text of either direction: its times and dates held left
 * to right, and the whole name isolated, taking its direction from its own first letter. Without it
 * an English line breaks "בוקר 07:00–19:00" into "–07:00 / 19:00" (the owner, 2026-11-13).
 */
export function nameText(name: string): string {
  return isolate(name.replace(NUMERIC_RUN, (run) => isolateLtr(run)));
}
