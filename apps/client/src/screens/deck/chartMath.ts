/**
 * The overview charts' arithmetic (the owner, 2026-11-13): donut slices (the top few and "others"),
 * weekly candles from the overview's daily series, and the SVG geometry. Amounts are added exactly,
 * in agorot, never as floating point; shares and positions are drawing, not money.
 */
import { startOfWeek } from "../../locale";

/** The sum of decimal amounts ("12.50"), exactly, as a decimal string with two places. */
export function sumAmounts(amounts: readonly string[]): string {
  let agorot = 0n;
  for (const a of amounts) {
    const match = /^(-?)(\d+)(?:\.(\d{1,2})\d*)?$/.exec(a.trim());
    if (!match) continue;
    const value = BigInt(match[2]!) * 100n + BigInt((match[3] ?? "").padEnd(2, "0") || "0");
    agorot += match[1] === "-" ? -value : value;
  }
  const negative = agorot < 0n;
  const abs = negative ? -agorot : agorot;
  return `${negative ? "-" : ""}${abs / 100n}.${String(abs % 100n).padStart(2, "0")}`;
}

/** One donut slice. */
export interface Slice {
  key: string;
  label: string;
  /** The exact amount or count, as text (an Amount, or a whole number). */
  amount: string;
  /** For drawing and the share: the amount as a number. */
  value: number;
  /** Its share of the whole, 0–100. */
  pct: number;
  others?: boolean;
}

/**
 * Groups summed by label, the largest `top` as their own slices and the rest as one "others"
 * slice; zero and negative groups are left out of a pie.
 */
export function pieSlices(items: readonly { label: string; amount: string }[], top: number, othersLabel: string): Slice[] {
  const groups = new Map<string, string[]>();
  for (const i of items) groups.set(i.label, [...(groups.get(i.label) ?? []), i.amount]);
  const summed = [...groups.entries()]
    .map(([label, amounts]) => ({ label, amount: sumAmounts(amounts) }))
    .filter((g) => Number(g.amount) > 0)
    .sort((a, b) => Number(b.amount) - Number(a.amount));
  const head = summed.slice(0, top);
  const rest = summed.slice(top);
  const all = rest.length > 0 ? [...head, { label: othersLabel, amount: sumAmounts(rest.map((r) => r.amount)), others: true }] : head;
  const total = all.reduce((n, g) => n + Number(g.amount), 0);
  return all.map((g, i) => ({
    key: (g as { others?: boolean }).others ? "others" : `s${i}`,
    label: g.label,
    amount: g.amount,
    value: Number(g.amount),
    pct: total > 0 ? (Number(g.amount) / total) * 100 : 0,
    ...((g as { others?: boolean }).others ? { others: true } : {}),
  }));
}

/** One week's candle of daily billing. */
export interface Candle {
  week: string;
  open: number;
  close: number;
  high: number;
  low: number;
  /** Close at or above open: drawn filled. */
  rising: boolean;
  /** Any day of the week still a forecast: drawn lighter. */
  forecast: boolean;
  days: number;
}

/**
 * A candle per week (Sunday first) from the days with billing: open is the first working day's
 * billing, close the last's, high the most and low the least. Days without billing are skipped.
 */
export function weeklyCandles(days: readonly { date: string; billing?: string | null; forecast: boolean }[]): Candle[] {
  const weeks = new Map<string, { date: string; value: number; forecast: boolean }[]>();
  for (const d of days) {
    if (d.billing == null) continue;
    const week = startOfWeek(d.date);
    weeks.set(week, [...(weeks.get(week) ?? []), { date: d.date, value: Number(d.billing), forecast: d.forecast }]);
  }
  return [...weeks.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([week, list]) => {
      const sorted = [...list].sort((a, b) => a.date.localeCompare(b.date));
      const values = sorted.map((d) => d.value);
      const open = values[0]!;
      const close = values[values.length - 1]!;
      return { week, open, close, high: Math.max(...values), low: Math.min(...values), rising: close >= open, forecast: sorted.some((d) => d.forecast), days: sorted.length };
    });
}

/** Round, readable scale ticks from `min` to `max` (about `count` of them). */
export function niceTicks(min: number, max: number, count = 4): number[] {
  if (!(max > min)) return [min];
  const raw = (max - min) / count;
  const magnitude = 10 ** Math.floor(Math.log10(raw));
  const step = [1, 2, 2.5, 5, 10].map((m) => m * magnitude).find((s) => s >= raw)!;
  const first = Math.floor(min / step) * step;
  const ticks: number[] = [];
  for (let v = first; v <= max + step * 0.001; v += step) ticks.push(Math.round(v * 100) / 100);
  if (ticks[ticks.length - 1]! < max) ticks.push(ticks[ticks.length - 1]! + step);
  return ticks;
}

/** The SVG path of a donut segment from angle `a0` to `a1` (radians, clockwise from 12 o'clock). */
export function arcPath(cx: number, cy: number, outer: number, inner: number, a0: number, a1: number): string {
  // A whole circle is two halves: one arc cannot start and end on the same point.
  if (a1 - a0 >= Math.PI * 2 - 1e-6) return `${arcPath(cx, cy, outer, inner, a0, a0 + Math.PI)} ${arcPath(cx, cy, outer, inner, a0 + Math.PI, a1)}`;
  const point = (r: number, a: number) => `${(cx + r * Math.sin(a)).toFixed(2)} ${(cy - r * Math.cos(a)).toFixed(2)}`;
  const large = a1 - a0 > Math.PI ? 1 : 0;
  return [`M ${point(outer, a0)}`, `A ${outer} ${outer} 0 ${large} 1 ${point(outer, a1)}`, `L ${point(inner, a1)}`, `A ${inner} ${inner} 0 ${large} 0 ${point(inner, a0)}`, "Z"].join(" ");
}
