/**
 * Money as people read it, exactly as the server holds it (specs/007-ux-redesign FR-058, D10;
 * Constitution VII).
 *
 * The API sends every amount as a decimal string. This module never turns it into a `number`: the
 * digits are grouped and placed by hand into the shape `Intl.NumberFormat` gives the locale and
 * currency, so an amount with four decimal places shows all four and nothing is ever rounded on
 * screen.
 */
import { isolateLtr } from "./bidi";

const DECIMAL = /^(-?)(\d+)(?:\.(\d+))?$/;

/** The sign, integer digits and fraction digits of a decimal string; throws on anything else. */
export function splitDecimal(amount: string): { negative: boolean; integer: string; fraction: string } {
  const match = DECIMAL.exec(amount.trim());
  if (!match) throw new Error(`not a decimal amount: ${amount}`);
  const integer = match[2]!.replace(/^0+(?=\d)/, "");
  return { negative: match[1] === "-" && /[1-9]/.test(match[2]! + (match[3] ?? "")), integer, fraction: match[3] ?? "" };
}

/** `digits` grouped in threes from the right with `separator`. */
export function groupDigits(digits: string, separator: string): string {
  let out = "";
  for (let i = digits.length; i > 0; i -= 3) {
    const piece = digits.slice(Math.max(0, i - 3), i);
    out = out ? `${piece}${separator}${out}` : piece;
  }
  return out;
}

const templates = new Map<string, Intl.NumberFormatPart[]>();

function template(locale: string, currency: string, negative: boolean): Intl.NumberFormatPart[] {
  const key = `${locale}|${currency}|${negative ? "-" : "+"}`;
  let found = templates.get(key);
  if (found === undefined) {
    // A sample with grouping, a decimal part and the sign, so every part the locale uses appears.
    found = new Intl.NumberFormat(locale, {
      style: "currency",
      currency,
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).formatToParts(negative ? -1234567.89 : 1234567.89);
    templates.set(key, found);
  }
  return found;
}

/**
 * `amount` (a decimal string such as `"1234.5"` or `"-45.1234"`) in `currency`, laid out as
 * `locale` writes money, isolated left to right so it reads the same inside Hebrew.
 *
 * Fewer than the currency's two decimal places are padded; more are kept as they are. Nothing is
 * computed from the digits.
 */
export function formatMoney(amount: string, locale: string, currency = "ILS"): string {
  const { negative, integer, fraction } = splitDecimal(amount);
  const sample = template(locale, currency, negative);
  const group = sample.find((part) => part.type === "group")?.value ?? "";
  const shownFraction = fraction.length < 2 ? fraction.padEnd(2, "0") : fraction;
  let out = "";
  let integerDone = false;
  for (const part of sample) {
    switch (part.type) {
      case "integer":
      case "group":
        if (!integerDone) {
          out += groupDigits(integer, group);
          integerDone = true;
        }
        break;
      case "fraction":
        out += shownFraction;
        break;
      default:
        out += part.value;
    }
  }
  return isolateLtr(out);
}
