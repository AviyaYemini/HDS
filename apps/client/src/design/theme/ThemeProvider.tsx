/**
 * The theme every component reads (specs/007-ux-redesign FR-061, X12, X15).
 *
 * Resolves the four themes from the device (colour scheme, high contrast) and the person's
 * override, picks the density (compact on a desktop-sized web window, comfortable elsewhere), and
 * gives both to the tree through {@link useTheme}. Also tells the browser the colour scheme, so
 * form controls and scrollbars match.
 */
import { createContext, useContext, useEffect, useMemo, useState, useSyncExternalStore, type ReactNode } from "react";
import { AccessibilityInfo, Platform, useColorScheme, useWindowDimensions } from "react-native";

import { component, themes, type Density, type ThemeColors, type ThemeName } from "./theme.generated";
import {
  getThemeState,
  setDensityPreference,
  setThemePreference,
  subscribeTheme,
  type ThemePreference,
} from "./themeStore";

/** The window width from which the web lays out as a desktop. */
export const DESKTOP_MIN_WIDTH = 768;

/** What {@link useTheme} gives. */
export interface Theme {
  name: ThemeName;
  colors: ThemeColors;
  /** Dark and high-contrast dark. */
  isDark: boolean;
  /** Either high-contrast theme. */
  isHighContrast: boolean;
  density: Density;
  /** The row height and gutter of the density. */
  spacing: (typeof component.density)[Density];
  /** A desktop-sized web window. */
  isDesktop: boolean;
  preference: ThemePreference;
  setPreference: (preference: ThemePreference) => void;
  setDensity: (density: Density | null) => void;
}

const ThemeContext = createContext<Theme | null>(null);

/** Whether the device asks for more contrast (web media query; Android and iOS settings). */
export function useHighContrastSetting(): boolean {
  const [high, setHigh] = useState(false);
  useEffect(() => {
    if (Platform.OS === "web") {
      if (typeof window === "undefined" || typeof window.matchMedia !== "function") return undefined;
      const query = window.matchMedia("(prefers-contrast: more)");
      const update = (): void => setHigh(query.matches);
      update();
      query.addEventListener?.("change", update);
      return () => query.removeEventListener?.("change", update);
    }
    const info = AccessibilityInfo as typeof AccessibilityInfo & {
      isHighTextContrastEnabled?: () => Promise<boolean>;
      isDarkerSystemColorsEnabled?: () => Promise<boolean>;
    };
    const read = Platform.OS === "android" ? info.isHighTextContrastEnabled : info.isDarkerSystemColorsEnabled;
    if (typeof read !== "function") return undefined;
    let live = true;
    read.call(info).then((value) => live && setHigh(value), () => undefined);
    const event = Platform.OS === "android" ? "highTextContrastChanged" : "darkerSystemColorsChanged";
    const subscription = AccessibilityInfo.addEventListener(event as "reduceMotionChanged", (value: boolean) =>
      setHigh(value),
    );
    return () => {
      live = false;
      subscription.remove();
    };
  }, []);
  return high;
}

/** The theme name for a preference and the device's settings. */
export function resolveThemeName(preference: ThemePreference, dark: boolean, highContrast: boolean): ThemeName {
  if (preference !== "system") return preference;
  if (dark) return highContrast ? "highContrastDark" : "dark";
  return highContrast ? "highContrastLight" : "light";
}

/** Give the tree the resolved theme. */
export function ThemeProvider({ children }: { children: ReactNode }) {
  const state = useSyncExternalStore(subscribeTheme, getThemeState, getThemeState);
  const scheme = useColorScheme();
  const highContrast = useHighContrastSetting();
  const { width } = useWindowDimensions();
  const isDesktop = Platform.OS === "web" && width >= DESKTOP_MIN_WIDTH;
  const name = resolveThemeName(state.preference, scheme === "dark", highContrast);
  const density: Density = state.density ?? (isDesktop ? "compact" : "comfortable");

  useEffect(() => {
    if (Platform.OS !== "web" || typeof document === "undefined") return;
    const dark = name === "dark" || name === "highContrastDark";
    document.documentElement.style.colorScheme = dark ? "dark" : "light";
    document.body.style.backgroundColor = themes[name].bg.canvas;
  }, [name]);

  const value = useMemo<Theme>(
    () => ({
      name,
      colors: themes[name],
      isDark: name === "dark" || name === "highContrastDark",
      isHighContrast: name === "highContrastLight" || name === "highContrastDark",
      density,
      spacing: component.density[density],
      isDesktop,
      preference: state.preference,
      setPreference: setThemePreference,
      setDensity: setDensityPreference,
    }),
    [name, density, isDesktop, state.preference],
  );
  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>;
}

/** The theme; outside a provider, the light theme at comfortable density (tests, storybook-like use). */
export function useTheme(): Theme {
  const theme = useContext(ThemeContext);
  if (theme) return theme;
  return {
    name: "light",
    colors: themes.light,
    isDark: false,
    isHighContrast: false,
    density: "comfortable",
    spacing: component.density.comfortable,
    isDesktop: false,
    preference: "system",
    setPreference: setThemePreference,
    setDensity: setDensityPreference,
  };
}
