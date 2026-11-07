/**
 * Option B's type scale (docs/plans/ux-working-plan.md section 3; specs/007-ux-redesign FR-061).
 *
 * IBM Plex Sans for Latin and IBM Plex Sans Hebrew for Hebrew, weights 400, 500 and 600. Sizes
 * differ between a desktop web window and a phone; Hebrew gets a generous line height and never
 * gets letter-spacing except the two places the plan names. Numbers use tabular figures.
 */
import type { TextStyle } from "react-native";

import { fontFamilyFor, type FontWeight } from "./fonts";

/** The named text styles. */
export type TextRole = "display" | "section" | "body" | "meta" | "eyebrow" | "label" | "numeric";

/** What the scale needs to know about where the text is shown. */
export interface TypeContext {
  /** `he` or `en`. */
  language: string;
  /** A desktop-sized web window (the plan's "web" column); otherwise the "phone" column. */
  desktop: boolean;
}

interface Scale {
  size: number;
  weight: FontWeight;
  lineHeight: number;
  /** Tracking in em; Hebrew has its own or none. */
  tracking?: number;
  trackingHebrew?: number;
  uppercase?: boolean;
}

function scale(role: TextRole, desktop: boolean, hebrew: boolean): Scale {
  switch (role) {
    case "display":
      return desktop
        ? { size: 48, weight: 500, lineHeight: 1.05, tracking: -0.045, trackingHebrew: -0.01 }
        : { size: 30, weight: 500, lineHeight: 1.1, tracking: -0.03, trackingHebrew: -0.01 };
    case "section":
      return { size: desktop ? 24 : 22, weight: 500, lineHeight: 1.25, tracking: -0.02 };
    case "body":
      return { size: 16, weight: 400, lineHeight: hebrew ? 1.55 : 1.45 };
    case "label":
      return { size: desktop ? 15 : 16, weight: 500, lineHeight: 1.3 };
    case "meta":
      return { size: desktop ? 13 : 14, weight: 400, lineHeight: hebrew ? 1.5 : 1.4 };
    case "eyebrow":
      return hebrew
        ? { size: 13, weight: 500, lineHeight: 1.3, trackingHebrew: 0.04 }
        : { size: 12, weight: 500, lineHeight: 1.3, tracking: 0.13, uppercase: true };
    case "numeric":
      return { size: 16, weight: 400, lineHeight: 1.45 };
  }
}

/** Whether `language` is written in the Hebrew script. */
export function isHebrew(language: string): boolean {
  return language.toLowerCase().startsWith("he");
}

/** The style for `role` in `context`; `weight` overrides the role's. */
export function textStyle(role: TextRole, context: TypeContext, weight?: FontWeight): TextStyle {
  const hebrew = isHebrew(context.language);
  const s = scale(role, context.desktop, hebrew);
  const chosen = weight ?? s.weight;
  const tracking = hebrew ? s.trackingHebrew : s.tracking;
  return {
    fontFamily: fontFamilyFor(chosen, hebrew),
    fontWeight: String(chosen) as TextStyle["fontWeight"],
    fontSize: s.size,
    lineHeight: Math.round(s.size * s.lineHeight),
    ...(tracking !== undefined ? { letterSpacing: Number((s.size * tracking).toFixed(2)) } : {}),
    ...(s.uppercase ? { textTransform: "uppercase" as const } : {}),
    ...(role === "numeric" ? { fontVariant: ["tabular-nums"] as TextStyle["fontVariant"] } : {}),
  };
}

/** Tabular figures for any text that holds times, counts or money. */
export const tabular: TextStyle = { fontVariant: ["tabular-nums"] };
