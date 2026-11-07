/**
 * The theme and density a person chose (specs/007-ux-redesign FR-061, X12, X15).
 *
 * The four themes follow the device (dark, high contrast) unless overridden here. On the web the
 * choice is kept in `localStorage` and can be set for one visit with `?theme=`; on phones it is kept
 * in AsyncStorage and read back when the app starts (`hydrateThemePreference`). A small external
 * store, so any screen can read it with `useSyncExternalStore` and the whole app redraws on a change.
 */
import AsyncStorage from "@react-native-async-storage/async-storage";
import { Platform } from "react-native";

import type { Density, ThemeName } from "./theme.generated";

/** A fixed theme, or `system` to follow the device. */
export type ThemePreference = ThemeName | "system";

export const THEME_NAMES: readonly ThemeName[] = ["light", "dark", "highContrastLight", "highContrastDark"];

const STORAGE_KEY = "hds.theme";
// A Map, not an object: `?theme=constructor` or `__proto__` must not find Object's own members.
const QUERY_ALIASES = new Map<string, ThemePreference>([
  ["light", "light"],
  ["dark", "dark"],
  ["high-contrast-light", "highContrastLight"],
  ["high-contrast-dark", "highContrastDark"],
  ["highcontrastlight", "highContrastLight"],
  ["highcontrastdark", "highContrastDark"],
  ["system", "system"],
]);

/** The preference a `?theme=` value or a stored string names, or null. */
export function parsePreference(text: string | null | undefined): ThemePreference | null {
  if (!text) return null;
  const key = text.trim().toLowerCase();
  const alias = QUERY_ALIASES.get(key);
  if (alias) return alias;
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
// Set once the person chooses in this run, so a late read of the stored choice never undoes it.
let chosenThisRun = false;

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
  chosenThisRun = true;
  // Stored even when unchanged here: before the stored choice is read back, memory says `system`
  // while storage may still hold an older choice.
  if (Platform.OS === "web") {
    if (typeof window !== "undefined") {
      try {
        if (preference === "system") window.localStorage.removeItem(STORAGE_KEY);
        else window.localStorage.setItem(STORAGE_KEY, preference);
      } catch {
        // Private browsing or blocked storage: the choice still holds for this visit.
      }
    }
  } else {
    const saved = preference === "system" ? AsyncStorage.removeItem(STORAGE_KEY) : AsyncStorage.setItem(STORAGE_KEY, preference);
    // A failed write keeps the choice for this run; the next start follows the device.
    saved.catch(() => undefined);
  }
  if (state.preference === preference) return;
  state = { ...state, preference };
  emit();
}

/**
 * On phones, read back the choice kept from an earlier run (X12). Called once when the app starts;
 * a choice made in this run before the read finishes wins. Does nothing on the web, where the
 * choice is read synchronously at load.
 */
export async function hydrateThemePreference(): Promise<void> {
  if (Platform.OS === "web") return;
  let stored: ThemePreference | null = null;
  try {
    stored = parsePreference(await AsyncStorage.getItem(STORAGE_KEY));
  } catch {
    return;
  }
  if (stored === null || chosenThisRun || stored === state.preference) return;
  state = { ...state, preference: stored };
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
  chosenThisRun = false;
  emit();
}
