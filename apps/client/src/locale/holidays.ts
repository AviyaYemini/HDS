/**
 * Israeli holidays and their eves, for the roster, the availability grid and the pickers
 * (specs/007-ux-redesign FR-054, D6).
 *
 * Built on `@hebcal/core` with the Israeli calendar (`il: true`): the days of rest (Rosh Hashana,
 * Yom Kippur, the first and last days of Sukkot and Pesach, Shavuot), their eves, the intermediate
 * days, and Independence Day. Names come from hebcal in the asked language.
 */
import { HebrewCalendar, HDate, flags, type Event } from "@hebcal/core";

import { iso } from "./plainDate";

/** What kind of special day a date is. */
export type HolidayKind = "holiday" | "eve" | "intermediate";

/** One marked day. */
export interface Holiday {
  date: string;
  kind: HolidayKind;
  /** The holiday's name in the language asked for. */
  name: string;
}

const INDEPENDENCE_DAY = "Yom HaAtzma'ut";

/** Hebrew vowel points and cantillation (U+0591 to U+05C7), which the screens do not show. */
const NIKUD = /[\u0591-\u05C7]/g;

function kindOf(event: Event): HolidayKind | null {
  const mask = event.getFlags();
  if (mask & flags.CHAG) return "holiday";
  if (event.getDesc() === INDEPENDENCE_DAY) return "holiday";
  if (mask & flags.EREV) return "eve";
  if (mask & flags.CHOL_HAMOED) return "intermediate";
  return null;
}

function isoOf(date: HDate): string {
  const greg = date.greg();
  return iso(greg.getFullYear(), greg.getMonth() + 1, greg.getDate());
}

/**
 * The marked days from `start` to `end` (ISO dates, inclusive), by date.
 *
 * `locale` is `he` or `en`; hebcal renders the name in it, without vowel points. A day that is
 * both an eve and a holiday keeps the stronger kind, `holiday`.
 */
export function holidaysBetween(start: string, end: string, locale: string): Map<string, Holiday> {
  const [sy, sm, sd] = start.split("-").map(Number) as [number, number, number];
  const [ey, em, ed] = end.split("-").map(Number) as [number, number, number];
  const events = HebrewCalendar.calendar({
    start: new HDate(new Date(sy, sm - 1, sd)),
    end: new HDate(new Date(ey, em - 1, ed)),
    il: true,
    noMinorFast: true,
    noSpecialShabbat: true,
    noRoshChodesh: true,
    noModern: false,
    sedrot: false,
    omer: false,
    candlelighting: false,
  });
  const found = new Map<string, Holiday>();
  const rank: Record<HolidayKind, number> = { holiday: 3, eve: 2, intermediate: 1 };
  for (const event of events) {
    const kind = kindOf(event);
    if (!kind) continue;
    const date = isoOf(event.getDate());
    const current = found.get(date);
    if (current && rank[current.kind] >= rank[kind]) continue;
    found.set(date, { date, kind, name: event.render(locale === "he" ? "he" : "en").replace(NIKUD, "") });
  }
  return found;
}
