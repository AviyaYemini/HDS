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

const CENTS = 100n;

/** A decimal string as whole agorot (cents); the API prices to cents, so nothing is lost. */
export function cents(text: string): bigint {
  const negative = text.startsWith("-");
  const [whole = "0", fraction = ""] = (negative ? text.slice(1) : text).split(".");
  const value = BigInt(whole || "0") * CENTS + BigInt((fraction + "00").slice(0, 2));
  return negative ? -value : value;
}

/** Agorot back to a decimal string with two decimals. */
export function fromCents(value: bigint): string {
  const negative = value < 0n;
  const abs = negative ? -value : value;
  return `${negative ? "-" : ""}${abs / CENTS}.${(abs % CENTS).toString().padStart(2, "0")}`;
}

export interface RateRow {
  /** The rate as the API spells it, e.g. "100", "125", "150". */
  ratePct: string;
  /** Hours at this rate, a decimal string. */
  hours: string;
  /** Pay at this rate in agorot, two decimals; the rows add up to the total by construction. */
  amount: string;
}

/**
 * The pay by rate (FR-024): the API prices each part of a line and makes the parts sum to the
 * line's pay, so the rows here only add those amounts up by rate, in whole agorot. No arithmetic on
 * rates or hours, and nothing to redistribute.
 */
export function breakdownByRate(pay: Pick<PayToDate, "lines">): RateRow[] {
  const hours = new Map<string, bigint>();
  const money = new Map<string, bigint>();
  for (const line of pay.lines) {
    if (!line.pay_enabled) continue;
    for (const part of line.parts) {
      if (part.pay === null || part.pay === undefined) continue;
      hours.set(part.rate_pct, (hours.get(part.rate_pct) ?? 0n) + units(part.hours));
      money.set(part.rate_pct, (money.get(part.rate_pct) ?? 0n) + cents(part.pay));
    }
  }
  return [...money.keys()]
    .sort((a, b) => Number(a) - Number(b))
    .map((rate) => ({ ratePct: rate, hours: shortHours(amount(hours.get(rate)!)), amount: fromCents(money.get(rate)!) }));
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
