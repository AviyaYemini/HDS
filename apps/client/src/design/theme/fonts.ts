/**
 * IBM Plex Sans and IBM Plex Sans Hebrew, weights 400, 500 and 600 (docs/plans/ux-working-plan.md
 * section 3; specs/007-ux-redesign task T015).
 *
 * Each family is registered once with its three faces, so `fontWeight` picks the cut on every
 * platform (Android needs API 29 or later for more than the regular face; older phones fall back to
 * it). On the web the family name is followed by the token file's fallbacks.
 */
import {
  IBMPlexSans_400Regular,
  IBMPlexSans_500Medium,
  IBMPlexSans_600SemiBold,
} from "@expo-google-fonts/ibm-plex-sans";
import {
  IBMPlexSansHebrew_400Regular,
  IBMPlexSansHebrew_500Medium,
  IBMPlexSansHebrew_600SemiBold,
} from "@expo-google-fonts/ibm-plex-sans-hebrew";
import { useFonts, type FontFamilyDefinition } from "expo-font";
import { Platform } from "react-native";

import { font } from "./theme.generated";

/** The three weights the design uses. */
export type FontWeight = 400 | 500 | 600;

/** The family names, as the token file spells them. */
export const LATIN_FAMILY = font.family.latin[0];
export const HEBREW_FAMILY = font.family.hebrew[0];

/** Everything `useAppFonts` loads. */
export const FONT_FAMILIES: FontFamilyDefinition[] = [
  {
    fontFamily: LATIN_FAMILY,
    fontDefinitions: [
      { path: IBMPlexSans_400Regular, weight: 400 },
      { path: IBMPlexSans_500Medium, weight: 500 },
      { path: IBMPlexSans_600SemiBold, weight: 600 },
    ],
  },
  {
    fontFamily: HEBREW_FAMILY,
    fontDefinitions: [
      { path: IBMPlexSansHebrew_400Regular, weight: 400 },
      { path: IBMPlexSansHebrew_500Medium, weight: 500 },
      { path: IBMPlexSansHebrew_600SemiBold, weight: 600 },
    ],
  },
];

// Unquoted: React Native Web quotes family names itself, and double quoting makes the browser drop
// the whole declaration and fall back to its default serif.
function stack(families: readonly string[]): string {
  return families.join(", ");
}

/**
 * The `fontFamily` for a weight in Hebrew or Latin: the family name alone on phones (a stack is
 * not understood there), the family and its fallbacks on the web.
 */
export function fontFamilyFor(_weight: FontWeight, hebrew: boolean): string {
  const families = hebrew ? font.family.hebrew : font.family.latin;
  return Platform.OS === "web" ? stack(families) : families[0];
}

/**
 * Load the families; true once they are in, or when loading failed (the system font then stands
 * in rather than a blank screen).
 */
export function useAppFonts(): boolean {
  const [loaded, error] = useFonts(FONT_FAMILIES);
  return loaded || error !== null;
}
