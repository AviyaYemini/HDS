/**
 * Text in the design's type scale and colour roles (specs/007-ux-redesign FR-061).
 *
 * `Txt` picks the family by language, the size by role and window, and the colour by tone and by
 * the surface it sits on: inside a `FeaturePanel` the default tones become the inverse pair, so
 * body copy on a dark panel needs no extra props. Headings carry their level for assistive
 * technology.
 */
import { createContext, useContext, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Text, type Role, type StyleProp, type TextProps, type TextStyle } from "react-native";

import { isRtlLanguage } from "../../i18n";
import { useTheme } from "../theme/ThemeProvider";
import { webProps } from "../web";
import type { FontWeight } from "../theme/fonts";
import { tabular, textStyle, type TextRole } from "../theme/typography";

/** The surface text sits on; `inverse` inside feature panels. */
export type Surface = "default" | "inverse";

/** Tells `Txt` below it which surface it is on. */
export const SurfaceContext = createContext<Surface>("default");

/** A colour role for text. */
export type Tone =
  | "primary"
  | "secondary"
  | "accent"
  | "cta"
  | "warning"
  | "success"
  | "danger"
  | "info"
  | "inherit";

export interface TxtProps extends Omit<TextProps, "style" | "role"> {
  role?: TextRole;
  tone?: Tone;
  weight?: FontWeight;
  /** Tabular figures, for times, counts and money inside running text. */
  numeric?: boolean;
  /** A heading of this level. */
  heading?: 1 | 2 | 3 | 4;
  /** The accessibility role (`status`, `alert`); headings use `heading` instead. */
  a11yRole?: Role;
  style?: StyleProp<TextStyle>;
  children?: ReactNode;
  /**
   * The text's direction on the web. By default the UI language's (rtl for Hebrew), not React
   * Native Web's `auto`, which takes the direction from the first strong letter: "HDS / ניהול"
   * would then read as left to right and sit at the wrong edge. `auto` for text a person typed,
   * whose language is not known; Latin or numbers inside Hebrew keep theirs through `isolate`.
   */
  dir?: "ltr" | "rtl" | "auto";
}

/** The colour `tone` means on `surface`. */
export function toneColor(colors: ReturnType<typeof useTheme>["colors"], tone: Tone, surface: Surface): string | undefined {
  switch (tone) {
    case "primary":
      return surface === "inverse" ? colors.inverse.text : colors.text.primary;
    case "secondary":
      return surface === "inverse" ? colors.inverse.textSecondary : colors.text.secondary;
    case "accent":
      return surface === "inverse" ? colors.cta.bg : colors.accent.default;
    case "cta":
      return colors.cta.text;
    case "warning":
    case "success":
    case "danger":
    case "info":
      return colors.status[tone];
    case "inherit":
      return undefined;
  }
}

/** The `dir` a text gets: the one asked for, else the UI language's; none (RNW's auto) for `auto`. */
export function textDirection(dir: "ltr" | "rtl" | "auto" | undefined, rtl: boolean): "ltr" | "rtl" | undefined {
  if (dir === "auto") return undefined;
  return dir ?? (rtl ? "rtl" : "ltr");
}

/** Text in the type scale. */
export function Txt({ role = "body", tone = "primary", weight, numeric, heading, a11yRole, style, children, dir, ...rest }: TxtProps) {
  const theme = useTheme();
  const surface = useContext(SurfaceContext);
  const { i18n } = useTranslation();
  const base = textStyle(role, { language: i18n.language, desktop: theme.isDesktop }, weight);
  const color = toneColor(theme.colors, tone, surface);
  return (
    <Text
      // From i18n's current language, so a switch never leaves text in the old direction (R122).
      {...webProps({ dir: textDirection(dir, isRtlLanguage(i18n.language)) })}
      {...rest}
      role={heading ? "heading" : a11yRole}
      aria-level={heading}
      style={[base, color ? { color } : null, numeric ? tabular : null, style]}
    >
      {children}
    </Text>
  );
}
