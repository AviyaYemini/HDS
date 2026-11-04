/**
 * Strings and text direction (Constitution VIII).
 *
 * Every text the app shows is a key in `src/locales/<language>.json`; English is complete and other
 * languages fall back to it key by key. Right-to-left languages turn the layout around through
 * `I18nManager` on phones and the document's `dir` on the web; styles use start and end only.
 */
import { getLocales } from "expo-localization";
import { createInstance } from "i18next";
import { initReactI18next } from "react-i18next";
import { I18nManager, Platform } from "react-native";

import en from "../locales/en.json";
import he from "../locales/he.json";

const SUPPORTED = ["en", "he"];
const RIGHT_TO_LEFT = new Set(["he", "ar", "fa", "ur"]);

/** The app's strings; `react-i18next` uses it once {@link setupI18n} has run. */
export const i18n = createInstance();

let rtl = false;

/** Whether `language` is written right to left. */
export function isRtlLanguage(language: string): boolean {
  return RIGHT_TO_LEFT.has(language);
}

/** The language to show: `?lang=` on the web, else the device's, else English. */
export function pickLanguage(): string {
  if (Platform.OS === "web" && typeof window !== "undefined") {
    const asked = new URLSearchParams(window.location.search).get("lang");
    if (asked && SUPPORTED.includes(asked)) return asked;
  }
  const device = getLocales()[0]?.languageCode ?? "en";
  return SUPPORTED.includes(device) ? device : "en";
}

/** Lay the app out right to left (`true`) or left to right. */
export function applyDirection(rightToLeft: boolean): void {
  rtl = rightToLeft;
  I18nManager.allowRTL(rightToLeft);
  I18nManager.forceRTL(rightToLeft);
  if (Platform.OS === "web" && typeof document !== "undefined") {
    document.documentElement.dir = rightToLeft ? "rtl" : "ltr";
    document.documentElement.lang = i18n.language ?? "en";
  }
}

/**
 * Whether the layout is right to left now: decided by the language, not by `I18nManager.isRTL`,
 * which on a phone keeps the previous run's direction until the app restarts.
 */
export function isRtl(): boolean {
  return rtl;
}

/** Load the strings and set the language and its direction. */
export async function setupI18n(language: string = pickLanguage()): Promise<typeof i18n> {
  if (!i18n.isInitialized) {
    await i18n.use(initReactI18next).init({
      resources: { en: { translation: en }, he: { translation: he } },
      lng: language,
      fallbackLng: "en",
      keySeparator: false,
      nsSeparator: false,
      interpolation: { escapeValue: false },
    });
  } else {
    await i18n.changeLanguage(language);
  }
  applyDirection(isRtlLanguage(language));
  return i18n;
}
