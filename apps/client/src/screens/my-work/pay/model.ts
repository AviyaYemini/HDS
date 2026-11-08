/**
 * My pay's arithmetic (specs/007-ux-redesign FR-024): the breakdown by rate that adds up exactly to
 * the total, the change since the last period, and the hours. Money is a decimal string from the API
 * and stays one: every sum here runs on integers in ten-thousandths, never on floating point.
 */
import type { components } from "@hds/api-client";

export type PayToDate = components["schemas"]["PayToDateRead"];
export type PayLine = components["schemas"]["PayLineRead"];

const SCALE = 4;
const UNIT = 10n ** BigInt(SCALE);

/** A decimal string as an integer count of ten-thousandths; extra decimals are cut, never rounded here. */
export function units(amount: string): bigint {
  const negative = amount.startsWith("-");
  const [whole = "0", fraction = ""] = (negative ? amount.slice(1) : amount).split(".");
  const value = BigInt(whole || "0") * UNIT + BigInt((fraction + "0000").slice(0, SCALE));
  return negative ? -value : value;
}

/** Ten-thousandths back to a decimal string with four decimals. */
export function amount(value: bigint): string {
  const negative = value < 0n;
  const abs = negative ? -value : value;
  const whole = abs / UNIT;
  const fraction = (abs % UNIT).toString().padStart(SCALE, "0");
  return `${negative ? "-" : ""}${whole}.${fraction}`;
}

/** The number of decimals a decimal string carries. */
function decimals(text: string): number {
  const dot = text.indexOf(".");
  return dot === -1 ? 0 : text.length - dot - 1;
}

/** `text` as an integer scaled by 10^places (places at least its own decimals). */
function scaled(text: string, places: number): bigint {
  const negative = text.startsWith("-");
  const [whole = "0", fraction = ""] = (negative ? text.slice(1) : text).split(".");
  const value = BigInt(whole || "0") * 10n ** BigInt(places) + BigInt((fraction + "0".repeat(places)).slice(0, places));
  return negative ? -value : value;
}

/** Round `value / divisor` to the nearest integer, halves away from zero. */
function divideRounded(value: bigint, divisor: bigint): bigint {
  const twice = value * 2n + (value >= 0n ? divisor : -divisor);
  return twice / (divisor * 2n);
}

export interface RateRow {
  /** The rate as the API spells it, e.g. "100", "125", "150". */
  ratePct: string;
  /** Hours at this rate, a decimal string. */
  hours: string;
  /** Pay at this rate, four decimals, so that all rows sum to the total exactly. */
  amount: string;
}

/**
 * The pay by rate (FR-024). Each row's raw amount is base rate × hours × rate ÷ 100, computed
 * exactly; the rows are then rounded to four decimals and the largest row takes whatever rounding
 * left over, so the rows add up to the API's total to the last digit.
 */
export function breakdownByRate(pay: Pick<PayToDate, "lines" | "pay">): RateRow[] {
  const hours = new Map<string, bigint>();
  const raw = new Map<string, bigint>();
  let places = SCALE;
  for (const line of pay.lines) {
    if (!line.pay_enabled || line.base_rate === null) continue;
    for (const part of line.parts) {
      const p = decimals(line.base_rate) + decimals(part.hours) + decimals(part.rate_pct) + 2;
      places = Math.max(places, p);
    }
  }
  for (const line of pay.lines) {
    if (!line.pay_enabled || line.base_rate === null) continue;
    for (const part of line.parts) {
      hours.set(part.rate_pct, (hours.get(part.rate_pct) ?? 0n) + scaled(part.hours, SCALE));
      // base × hours × pct / 100, all scaled to `places` decimals in total.
      const own = decimals(line.base_rate) + decimals(part.hours) + decimals(part.rate_pct) + 2;
      const product = scaled(line.base_rate, decimals(line.base_rate)) * scaled(part.hours, decimals(part.hours)) * scaled(part.rate_pct, decimals(part.rate_pct));
      raw.set(part.rate_pct, (raw.get(part.rate_pct) ?? 0n) + product * 10n ** BigInt(places - own));
    }
  }
  const rates = [...raw.keys()].sort((a, b) => Number(a) - Number(b));
  const rounded = new Map(rates.map((rate) => [rate, divideRounded(raw.get(rate)!, 10n ** BigInt(places - SCALE))]));
  const total = pay.pay === null || pay.pay === undefined ? [...rounded.values()].reduce((sum, value) => sum + value, 0n) : units(pay.pay);
  const sum = [...rounded.values()].reduce((a, b) => a + b, 0n);
  if (rates.length > 0 && sum !== total) {
    const largest = rates.reduce((best, rate) => (rounded.get(rate)! > rounded.get(best)! ? rate : best), rates[0]!);
    rounded.set(largest, rounded.get(largest)! + (total - sum));
  }
  return rates.map((rate) => ({ ratePct: rate, hours: amount(hours.get(rate)!).replace(/\.?0+$/, "").replace(/^$/, "0"), amount: amount(rounded.get(rate)!) }));
}

/** The change from `previous` to `current`, as a signed four-decimal string, or null without both. */
export function changeSince(current: string | null | undefined, previous: string | null | undefined): string | null {
  if (current === null || current === undefined || previous === null || previous === undefined) return null;
  return amount(units(current) - units(previous));
}

/** The month before `period` (`YYYY-MM`). */
export function previousPeriod(period: string): string {
  const [year, month] = period.split("-").map(Number) as [number, number];
  const date = new Date(Date.UTC(year, month - 2, 1));
  return date.toISOString().slice(0, 7);
}

/** The month after `period`. */
export function nextPeriod(period: string): string {
  const [year, month] = period.split("-").map(Number) as [number, number];
  const date = new Date(Date.UTC(year, month, 1));
  return date.toISOString().slice(0, 7);
}

/** Hours as a short decimal ("37.5", "8") from a decimal string. */
export function shortHours(hours: string): string {
  return hours.replace(/(\.\d*?[1-9])0+$/, "$1").replace(/\.0+$/, "");
}
