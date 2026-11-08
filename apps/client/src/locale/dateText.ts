/**
 * Dates as words: headers, month and weekday names (specs/007-ux-redesign FR-055, D7; O13).
 *
 * Numeric dates are dd/MM/yyyy everywhere (`formatNumericDate`); headers spell the month and the
 * weekday out in the language, through `Intl`. A calendar date is formatted at noon UTC, so no
 * zone can move it to the day before.
 */
import { LRI, PDI } from "./bidi";
import { formatNumericDate, parts } from "./plainDate";
import { RANGE_DASH } from "./times";

const formatters = new Map<string, Intl.DateTimeFormat>();

function formatter(locale: string, options: Intl.DateTimeFormatOptions): Intl.DateTimeFormat {
  const key = `${locale}|${JSON.stringify(options)}`;
  let found = formatters.get(key);
  if (found === undefined) {
    found = new Intl.DateTimeFormat(locale, { ...options, timeZone: "UTC" });
    formatters.set(key, found);
  }
  return found;
}

/** A calendar date as a `Date` at noon UTC, safe to format in UTC. */
export function atNoonUtc(date: string): Date {
  const { year, month, day } = parts(date);
  return new Date(Date.UTC(year, month - 1, day, 12));
}

/**
 * A header from its parts. HDS English reads day before month ("Saturday, 7 November"), whatever
 * the engine's English layout, so English is assembled by hand; other languages keep their own
 * order with a comma after the weekday. Reading parts also keeps the output right on Hermes, whose
 * Intl polyfill carries only the app's two languages.
 */
function header(parts: Intl.DateTimeFormatPart[], locale: string): string {
  const of = (type: string): string => parts.find((part) => part.type === type)?.value ?? "";
  if (locale.toLowerCase().startsWith("en")) {
    const year = of("year");
    return `${of("weekday")}, ${of("day")} ${of("month")}${year ? ` ${year}` : ""}`;
  }
  return parts
    .map((part, index) => {
      const previous = parts[index - 1];
      if (part.type === "literal" && previous?.type === "weekday" && !part.value.includes(",")) return ", ";
      return part.value;
    })
    .join("");
}

/** `date` for a header: "Saturday, 7 November" / "יום שבת, 7 בנובמבר" (the weekday spelled out). */
export function formatHeaderDate(date: string, locale: string): string {
  return header(formatter(locale, { weekday: "long", day: "numeric", month: "long" }).formatToParts(atNoonUtc(date)), locale);
}

/** `date` with its year for a header: "Saturday, 7 November 2026". */
export function formatLongDate(date: string, locale: string): string {
  return header(
    formatter(locale, { weekday: "long", day: "numeric", month: "long", year: "numeric" }).formatToParts(atNoonUtc(date)),
    locale,
  );
}

/** The month of `date` with its year: "November 2026" / "נובמבר 2026". */
export function formatMonthYear(date: string, locale: string): string {
  return formatter(locale, { month: "long", year: "numeric" }).format(atNoonUtc(date));
}

/**
 * The seven weekday names, Sunday first, in `width`. Hebrew's short names drop the "יום" prefix
 * ("א׳" rather than "יום א׳"), which is how calendar headers are written.
 */
export function weekdayNames(locale: string, width: "long" | "short" | "narrow" = "short"): string[] {
  // 2026-11-01 is a Sunday.
  return Array.from({ length: 7 }, (_, i) => {
    const name = formatter(locale, { weekday: width }).format(new Date(Date.UTC(2026, 10, 1 + i, 12)));
    return width === "long" ? name : name.replace(/^יום\s+/, "");
  });
}

/** The twelve month names in `width`. */
export function monthNames(locale: string, width: "long" | "short" = "long"): string[] {
  return Array.from({ length: 12 }, (_, i) =>
    formatter(locale, { month: width }).format(new Date(Date.UTC(2026, i, 15, 12))),
  );
}

/** A range as `dd/MM/yyyy – dd/MM/yyyy`, isolated so Hebrew does not swap the two dates. */
export function formatNumericRange(start: string, end: string): string {
  return `${LRI}${formatNumericDate(start)} ${RANGE_DASH} ${formatNumericDate(end)}${PDI}`;
}

/** The same range for a narrow screen: the first date without its year when both share one. */
export function formatShortNumericRange(start: string, end: string): string {
  const first = start.slice(0, 4) === end.slice(0, 4) ? formatNumericDate(start).slice(0, 5) : formatNumericDate(start);
  return `${LRI}${first} ${RANGE_DASH} ${formatNumericDate(end)}${PDI}`;
}
