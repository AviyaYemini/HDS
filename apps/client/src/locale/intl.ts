/**
 * The `Intl` the app relies on, complete on every engine (specs/007-ux-redesign research R13, D11).
 *
 * Hermes ships a partial `Intl`; the FormatJS polyfills fill what is missing, in dependency order,
 * and only where the engine lacks it (each `polyfill` entry checks first). Hebrew and English data
 * and the "golden" time zones, which include Asia/Jerusalem, are bundled. Import this module once,
 * at the entry point, before i18next and before any formatter is built.
 */
import "@formatjs/intl-getcanonicallocales/polyfill.js";
import "@formatjs/intl-locale/polyfill.js";
import "@formatjs/intl-pluralrules/polyfill.js";
import "@formatjs/intl-pluralrules/locale-data/en.js";
import "@formatjs/intl-pluralrules/locale-data/he.js";
import "@formatjs/intl-numberformat/polyfill.js";
import "@formatjs/intl-numberformat/locale-data/en.js";
import "@formatjs/intl-numberformat/locale-data/he.js";
import "@formatjs/intl-datetimeformat/polyfill.js";
import "@formatjs/intl-datetimeformat/locale-data/en.js";
import "@formatjs/intl-datetimeformat/locale-data/he.js";
import "@formatjs/intl-datetimeformat/add-golden-tz.js";
