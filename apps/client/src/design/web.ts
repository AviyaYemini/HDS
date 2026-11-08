/**
 * Props that exist only on React Native Web (specs/007-ux-redesign FR-052, FR-057).
 *
 * React Native's types do not know `dataSet` (rendered as `data-*` attributes), `dir`, `tabIndex`
 * on arbitrary views or the keyboard events, yet the web needs them for the e2e contract
 * (`data-mirrored`, `data-date`), for logical-style direction and for the W3C keyboard patterns.
 * {@link webProps} returns them on the web and nothing on phones, typed loosely on purpose, so a
 * component spreads them without a cast of its own.
 */
import { Platform } from "react-native";

/** A keyboard event as the browser gives it (React Native Web passes it through). */
export interface WebKeyEvent {
  key: string;
  shiftKey: boolean;
  altKey: boolean;
  ctrlKey: boolean;
  metaKey: boolean;
  preventDefault: () => void;
  stopPropagation: () => void;
}

/** The web-only props a component may want. */
export interface WebOnlyProps {
  /** `data-*` attributes: `{ mirrored: "true" }` renders `data-mirrored="true"`. */
  dataSet?: Record<string, string>;
  /** The writing direction of this element and, through context, its descendants. */
  dir?: "ltr" | "rtl";
  /** A WAI-ARIA role React Native's `Role` type lacks (`gridcell`). */
  role?: string;
  /** The language of this element's text, when it differs from the page's. */
  lang?: string;
  /** `tabIndex` for an element React Native does not make focusable on its own. */
  tabIndex?: 0 | -1;
  onKeyDown?: (event: WebKeyEvent) => void;
  /** Attributes React Native has no prop for (`aria-pressed`, `aria-describedby`, `title`). */
  [attribute: `aria-${string}`]: string | boolean | undefined;
  title?: string;
}

/** `props` on the web; `{}` on phones. The result spreads into any View, Text or Pressable. */
export function webProps(props: WebOnlyProps): Record<string, unknown> {
  return Platform.OS === "web" ? (props as Record<string, unknown>) : {};
}

/** Whether this is the web, where keyboard and pointer patterns apply. */
export const isWeb = Platform.OS === "web";
