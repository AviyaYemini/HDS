/**
 * The dashboards' small touches (the owner, 2026-11-13, "I want to be amazed"): the greeting by the
 * time of day, change lines against the previous period, money in whole shekels on the dashboards
 * (rounded half up on the decimal string, never through floating point; the detailed tables keep
 * their agorot), and the points of a small trend line.
 */
import { formatMoney } from "../../locale";

/** The greeting's key for an hour of the day (0–23) where the organisation is. */
export function greetingKey(hour: number): "greeting.morning" | "greeting.afternoon" | "greeting.evening" {
  if (hour >= 5 && hour < 12) return "greeting.morning";
  if (hour >= 12 && hour < 17) return "greeting.afternoon";
  return "greeting.evening";
}

/** The first word of a full name ("Dana Levi" → "Dana"). */
export function firstName(full: string | null | undefined): string {
  return (full ?? "").trim().split(/\s+/)[0] ?? "";
}

/** The change from `previous` to `current`, in percent, or null when there is nothing to compare. */
export function changePct(current: string | null | undefined, previous: string | null | undefined): number | null {
  if (current == null || previous == null) return null;
  const before = Number(previous);
  if (!Number.isFinite(before) || before === 0) return null;
  return ((Number(current) - before) / Math.abs(before)) * 100;
}

/** "▲ 12%", "▼ 3.4%", "= 0%": the direction by the arrow alone, the size under 10 with one decimal. */
export function formatChange(pct: number | null): string | null {
  if (pct === null) return null;
  const size = Math.abs(pct);
  const shown = size < 10 ? (Math.round(size * 10) / 10).toString() : Math.round(size).toString();
  if (shown === "0") return "= 0%";
  return `${pct > 0 ? "▲" : "▼"} ${shown}%`;
}

/** A margin's change in percentage points: "▲ 1.2 pt" (the text's unit comes from the caller). */
export function changePoints(current: string | null | undefined, previous: string | null | undefined): number | null {
  if (current == null || previous == null) return null;
  return Math.round((Number(current) - Number(previous)) * 10) / 10;
}

/** A decimal amount rounded half up to whole shekels, exactly, as a decimal string ("1234.50" → "1235"). */
export function roundAmount(amount: string): string {
  const match = /^(-?)(\d+)(?:\.(\d+))?$/.exec(amount.trim());
  if (!match) return amount;
  let whole = BigInt(match[2]!);
  if (Number((match[3] ?? "0")[0] ?? "0") >= 5) whole += 1n;
  return `${match[1] === "-" && whole !== 0n ? "-" : ""}${whole}`;
}

/** Money in whole shekels for a dashboard figure ("₪123,456"); tables keep `formatMoney`'s agorot. */
export function formatMoneyWhole(amount: string, locale: string): string {
  // formatMoney pads to two places; the padded ".00" is dropped, whatever the locale's separator.
  return formatMoney(roundAmount(amount), locale).replace(/[.,]00(?=\D*$)/, "");
}

/**
 * A trend line's points in a `width` × `height` box, oldest first, scaled between the series'
 * least and most (a flat series sits in the middle). Left to right; a right-to-left caller mirrors it.
 */
export function sparkPoints(values: readonly number[], width: number, height: number, pad = 2): string {
  if (values.length === 0) return "";
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min;
  const step = values.length > 1 ? (width - pad * 2) / (values.length - 1) : 0;
  return values
    .map((v, i) => {
      const x = values.length > 1 ? pad + i * step : width / 2;
      const y = span === 0 ? height / 2 : pad + (1 - (v - min) / span) * (height - pad * 2);
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
}

/** An eased progress (0–1) for the entrance: fast at first, settling. */
export function easeOut(t: number): number {
  const c = Math.min(1, Math.max(0, t));
  return 1 - (1 - c) ** 3;
}
