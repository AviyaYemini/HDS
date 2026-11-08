/**
 * The time zone screens format in until every site carries its own on screen (specs/007-ux-redesign
 * FR-053, D5), and a check that `Intl` can do what the app needs.
 */

/** The organisation's zone. */
export const DEFAULT_TIME_ZONE = "Asia/Jerusalem";

/** Whether this engine can format dates in a named zone and money to parts, which every screen needs. */
export function intlReady(): boolean {
  try {
    new Intl.DateTimeFormat("he", { timeZone: DEFAULT_TIME_ZONE, hour: "2-digit" }).format(new Date());
    new Intl.NumberFormat("he", { style: "currency", currency: "ILS" }).formatToParts(1);
    return true;
  } catch {
    return false;
  }
}
