/**
 * The icon set (specs/007-ux-redesign FR-057, X6; O13): thin square strokes on a 24-unit grid,
 * drawn with react-native-svg so they stay crisp and take the text colour.
 *
 * Directional icons (arrows, chevrons that point forward or back) mirror in Hebrew; the clock, the
 * calendar and every symbol do not. On the web each icon carries `data-mirrored` so a test can tell.
 * An icon is decorative unless it is given a label.
 */
import { useTranslation } from "react-i18next";
import { StyleSheet, View, type StyleProp, type ViewStyle } from "react-native";
import Svg, { Circle, Line, Path, Polyline, Rect } from "react-native-svg";

import { isRtlLanguage } from "../../i18n";
import { webProps } from "../web";

/** The icons the design uses. */
export type IconName =
  | "arrow-next"
  | "arrow-back"
  | "chevron-next"
  | "chevron-back"
  | "chevron-down"
  | "chevron-up"
  | "clock"
  | "calendar"
  | "check"
  | "close"
  | "warning"
  | "info"
  | "minus"
  | "plus"
  | "search"
  | "undo"
  | "sort"
  | "sort-asc"
  | "sort-desc"
  | "menu"
  | "more"
  | "filter"
  | "sun"
  | "moon"
  | "user"
  | "pin";

/** Icons that point forward or back, and so mirror in a right-to-left layout. */
const MIRRORS: ReadonlySet<IconName> = new Set(["arrow-next", "arrow-back", "chevron-next", "chevron-back", "undo"]);

export interface IconProps {
  name: IconName;
  /** In px; 20 by default. */
  size?: number;
  /** Defaults to the surrounding text colour (`currentColor`). */
  color?: string;
  /** A label makes the icon meaningful to assistive technology; without one it is decorative. */
  label?: string;
  testID?: string;
  style?: StyleProp<ViewStyle>;
}

function glyph(name: IconName) {
  switch (name) {
    case "arrow-next":
      return (
        <>
          <Line x1="4" y1="12" x2="20" y2="12" />
          <Polyline points="13 5 20 12 13 19" />
        </>
      );
    case "arrow-back":
      return (
        <>
          <Line x1="20" y1="12" x2="4" y2="12" />
          <Polyline points="11 5 4 12 11 19" />
        </>
      );
    case "chevron-next":
      return <Polyline points="9 5 16 12 9 19" />;
    case "chevron-back":
      return <Polyline points="15 5 8 12 15 19" />;
    case "chevron-down":
      return <Polyline points="5 9 12 16 19 9" />;
    case "chevron-up":
      return <Polyline points="5 15 12 8 19 15" />;
    case "clock":
      return (
        <>
          <Circle cx="12" cy="12" r="8.5" />
          <Polyline points="12 7 12 12 15.5 14" />
        </>
      );
    case "calendar":
      return (
        <>
          <Rect x="3.5" y="5" width="17" height="15.5" />
          <Line x1="3.5" y1="10" x2="20.5" y2="10" />
          <Line x1="8" y1="3" x2="8" y2="7" />
          <Line x1="16" y1="3" x2="16" y2="7" />
        </>
      );
    case "check":
      return <Polyline points="5 12.5 10 17.5 19 7" />;
    case "close":
      return (
        <>
          <Line x1="6" y1="6" x2="18" y2="18" />
          <Line x1="18" y1="6" x2="6" y2="18" />
        </>
      );
    case "warning":
      return (
        <>
          <Path d="M12 3.5 L21.5 20.5 H2.5 Z" />
          <Line x1="12" y1="9.5" x2="12" y2="14.5" />
          <Line x1="12" y1="17" x2="12" y2="17.6" />
        </>
      );
    case "info":
      return (
        <>
          <Rect x="3.5" y="3.5" width="17" height="17" />
          <Line x1="12" y1="11" x2="12" y2="16.5" />
          <Line x1="12" y1="7.5" x2="12" y2="8.1" />
        </>
      );
    case "minus":
      return <Line x1="5" y1="12" x2="19" y2="12" />;
    case "plus":
      return (
        <>
          <Line x1="5" y1="12" x2="19" y2="12" />
          <Line x1="12" y1="5" x2="12" y2="19" />
        </>
      );
    case "search":
      return (
        <>
          <Circle cx="10.5" cy="10.5" r="6.5" />
          <Line x1="15.5" y1="15.5" x2="21" y2="21" />
        </>
      );
    case "undo":
      return (
        <>
          <Polyline points="8 6 3.5 10.5 8 15" />
          <Path d="M3.5 10.5 H15 a5 5 0 0 1 0 10 H9" />
        </>
      );
    case "sort":
      return (
        <>
          <Polyline points="8 9 12 5 16 9" />
          <Polyline points="8 15 12 19 16 15" />
        </>
      );
    case "sort-asc":
      return (
        <>
          <Polyline points="8 9 12 5 16 9" />
          <Line x1="12" y1="5" x2="12" y2="19" />
        </>
      );
    case "sort-desc":
      return (
        <>
          <Polyline points="8 15 12 19 16 15" />
          <Line x1="12" y1="5" x2="12" y2="19" />
        </>
      );
    case "menu":
      return (
        <>
          <Line x1="4" y1="7" x2="20" y2="7" />
          <Line x1="4" y1="12" x2="20" y2="12" />
          <Line x1="4" y1="17" x2="20" y2="17" />
        </>
      );
    case "more":
      return (
        <>
          <Rect x="4" y="10.5" width="3" height="3" />
          <Rect x="10.5" y="10.5" width="3" height="3" />
          <Rect x="17" y="10.5" width="3" height="3" />
        </>
      );
    case "filter":
      return <Path d="M3.5 5 H20.5 L14 12.5 V20 L10 18 V12.5 Z" />;
    case "sun":
      return (
        <>
          <Circle cx="12" cy="12" r="4" />
          <Line x1="12" y1="3" x2="12" y2="5.5" />
          <Line x1="12" y1="18.5" x2="12" y2="21" />
          <Line x1="3" y1="12" x2="5.5" y2="12" />
          <Line x1="18.5" y1="12" x2="21" y2="12" />
        </>
      );
    case "moon":
      return <Path d="M19 14.5 A7.5 7.5 0 1 1 9.5 5 A6 6 0 0 0 19 14.5 Z" />;
    case "user":
      return (
        <>
          <Circle cx="12" cy="8.5" r="4" />
          <Path d="M4.5 20.5 a7.5 7.5 0 0 1 15 0" />
        </>
      );
    case "pin":
      return (
        <>
          <Path d="M12 21 s-6.5-6.2-6.5-11 a6.5 6.5 0 0 1 13 0 c0 4.8-6.5 11-6.5 11 Z" />
          <Circle cx="12" cy="10" r="2.2" />
        </>
      );
  }
}

/** Whether `name` mirrors in a right-to-left layout. */
export function iconMirrors(name: IconName): boolean {
  return MIRRORS.has(name);
}

/** One icon. */
export function Icon({ name, size = 20, color = "currentColor", label, testID, style }: IconProps) {
  const { i18n } = useTranslation();
  const mirrored = iconMirrors(name) && isRtlLanguage(i18n.language);
  const hidden = label === undefined;
  const web = webProps({ dataSet: { mirrored: String(mirrored) }, "aria-hidden": hidden ? "true" : undefined });
  return (
    <View
      testID={testID}
      role={hidden ? undefined : "img"}
      accessibilityLabel={label}
      accessibilityElementsHidden={hidden}
      importantForAccessibility={hidden ? "no-hide-descendants" : "yes"}
      {...web}
      style={[styles.box, { width: size, height: size }, mirrored && styles.mirrored, style]}
    >
      <Svg
        width={size}
        height={size}
        viewBox="0 0 24 24"
        fill="none"
        stroke={color}
        strokeWidth={1.6}
        strokeLinecap="square"
        strokeLinejoin="miter"
        color={color}
      >
        {glyph(name)}
      </Svg>
    </View>
  );
}

const styles = StyleSheet.create({
  box: { alignItems: "center", justifyContent: "center" },
  mirrored: { transform: [{ scaleX: -1 }] },
});
