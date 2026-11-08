/**
 * The locale plumbing on an engine whose `Intl` is the FormatJS polyfill with only the app's two
 * languages, as on Hermes (specs/007-ux-redesign research R13; review finding R1). The polyfills
 * are forced on in this file alone, so every formatter must read parts rather than rely on a
 * locale's layout.
 */
import "@formatjs/intl-getcanonicallocales/polyfill-force.js";
import "@formatjs/intl-locale/polyfill-force.js";
import "@formatjs/intl-pluralrules/polyfill-force.js";
import "@formatjs/intl-pluralrules/locale-data/en.js";
import "@formatjs/intl-pluralrules/locale-data/he.js";
import "@formatjs/intl-numberformat/polyfill-force.js";
import "@formatjs/intl-numberformat/locale-data/en.js";
import "@formatjs/intl-numberformat/locale-data/he.js";
import "@formatjs/intl-datetimeformat/polyfill-force.js";
import "@formatjs/intl-datetimeformat/locale-data/en.js";
import "@formatjs/intl-datetimeformat/locale-data/he.js";
import "@formatjs/intl-datetimeformat/add-golden-tz.js";

import {
  crossesClockChange,
  dateOf,
  formatHeaderDate,
  formatMoney,
  formatMonthYear,
  formatTimeRange,
  stripIsolates,
  todayIn,
  weekdayNames,
  zoneOffsetMinutes,
} from "../../src/locale";

const ZONE = "Asia/Jerusalem";

describe("locale plumbing on the Intl polyfill (R13, R1)", () => {
  it("is really running the polyfill", () => {
    expect((Intl.DateTimeFormat as unknown as { polyfilled?: boolean }).polyfilled).toBe(true);
  });

  it("reads today's date and a shift's times in the site's zone", () => {
    expect(todayIn(ZONE, new Date("2026-11-07T08:00:00Z"))).toBe("2026-11-07");
    expect(todayIn(ZONE, new Date("2026-11-07T23:30:00Z"))).toBe("2026-11-08");
    expect(dateOf("2026-11-08T04:00:00Z", ZONE)).toBe("2026-11-08");
    expect(stripIsolates(formatTimeRange("2026-11-07T20:00:00Z", "2026-11-08T04:00:00Z", ZONE))).toBe("22:00–06:00 +1");
    // Midnight to 07:30 on the same calendar day in Jerusalem: no "+1".
    expect(stripIsolates(formatTimeRange("2026-11-07T22:00:00Z", "2026-11-08T05:30:00Z", ZONE))).toBe("00:00–07:30");
  });

  it("spells English headers day first and Hebrew headers in Hebrew", () => {
    expect(formatHeaderDate("2026-11-07", "en")).toBe("Saturday, 7 November");
    expect(formatHeaderDate("2026-11-07", "he")).toContain("נובמבר");
    expect(formatMonthYear("2026-11-07", "en")).toBe("November 2026");
    expect(weekdayNames("he", "short")[0]).toBe("א׳");
  });

  it("knows the zone's offset and the autumn clock change", () => {
    expect(zoneOffsetMinutes("2026-11-07T08:00:00Z", ZONE)).toBe(120);
    expect(zoneOffsetMinutes("2026-07-07T08:00:00Z", ZONE)).toBe(180);
    expect(crossesClockChange("2026-10-24T19:00:00Z", "2026-10-25T03:00:00Z", ZONE)).toBe(true);
  });

  it("formats money exactly", () => {
    expect(stripIsolates(formatMoney("1234.5", "en"))).toBe("₪1,234.50");
    expect(stripIsolates(formatMoney("45.1234", "he"))).toContain("45.1234");
  });
});
