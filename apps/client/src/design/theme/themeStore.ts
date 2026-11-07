/**
 * The theme and density a person chose (specs/007-ux-redesign FR-061, X12, X15).
 *
 * The four themes follow the device (dark, high contrast) unless overridden here. On the web the
 * choice is kept in `localStorage` and can be set for one visit with `?theme=`; on phones it lives
 * in memory until the Account screen (group B) persists it. A small external store, so any screen
 * can read it with `useSyncExternalStore` and the whole app redraws on a change.
 */
import { Platform } from "react-native";

import type { Density, ThemeName } from "./theme.generated";

/** A fixed theme, or `system` to follow the device. */
export type ThemePreference = ThemeName | "system";

export const THEME_NAMES: readonly ThemeName[] = ["light", "dark", "highContrastLight", "highContrastDark"];

const STORAGE_KEY = "hds.theme";
const QUERY_ALIASES: Record<string, ThemePreference> = {
  light: "light",
  dark: "dark",
  "high-contrast-light": "highContrastLight",
  "high-contrast-dark": "highContrastDark",
  highcontrastlight: "highContrastLight",
  highcontrastdark: "highContrastDark",
  system: "system",
};

/** The preference a `?theme=` value or a stored string names, or null. */
export function parsePreference(text: string | null | undefined): ThemePreference | null {
  if (!text) return null;
  const key = text.trim().toLowerCase();
  if (key in QUERY_ALIASES) return QUERY_ALIASES[key]!;
  const exact = THEME_NAMES.find((name) => name.toLowerCase() === key);
  return exact ?? null;
}

function initialPreference(): ThemePreference {
  if (Platform.OS !== "web" || typeof window === "undefined") return "system";
  const asked = parsePreference(new URLSearchParams(window.location.search).get("theme"));
  if (asked) return asked;
  try {
    return parsePreference(window.localStorage.getItem(STORAGE_KEY)) ?? "system";
  } catch {
    return "system";
  }
}

interface State {
  preference: ThemePreference;
  density: Density | null;
}

let state: State = { preference: initialPreference(), density: null };
const listeners = new Set<() => void>();

function emit(): void {
  for (const listener of listeners) listener();
}

/** The current choice. */
export function getThemeState(): State {
  return state;
}

/** Listen for changes; returns the unsubscribe. */
export function subscribeTheme(listener: () => void): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

/** Choose a theme, or `system` to follow the device again. */
export function setThemePreference(preference: ThemePreference): void {
  if (state.preference === preference) return;
  state = { ...state, preference };
  if (Platform.OS === "web" && typeof window !== "undefined") {
    try {
      if (preference === "system") window.localStorage.removeItem(STORAGE_KEY);
      else window.localStorage.setItem(STORAGE_KEY, preference);
    } catch {
      // Private browsing or blocked storage: the choice still holds for this visit.
    }
  }
  emit();
}

/** Force a density, or null to let the screen size decide (compact on desktop, comfortable on phones). */
export function setDensityPreference(density: Density | null): void {
  if (state.density === density) return;
  state = { ...state, density };
  emit();
}

/** For tests: back to the defaults. */
export function resetThemeStore(): void {
  state = { preference: "system", density: null };
  emit();
}
