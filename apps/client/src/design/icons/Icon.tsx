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
  | "pin"
  | "home"
  | "wallet"
  | "people"
  | "building"
  | "briefcase"
  | "layers"
  | "target"
  | "star"
  | "chart"
  | "grid"
  | "play"
  | "inbox"
  | "settings"
  | "mail"
  | "phone"
  | "help"
  | "accessibility"
  | "lock"
  | "globe"
  | "list"
  | "sign-out";

/** Icons that point forward or back, and so mirror in a right-to-left layout. */
const MIRRORS: ReadonlySet<IconName> = new Set(["arrow-next", "arrow-back", "chevron-next", "chevron-back", "undo", "sign-out", "play"]);

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
    case "home":
      return (
        <>
          <Path d="M3.5 11.5 L12 4 L20.5 11.5" />
          <Path d="M6 10 V20 H18 V10" />
          <Rect x="10" y="14" width="4" height="6" />
        </>
      );
    case "wallet":
      return (
        <>
          <Rect x="3.5" y="6" width="17" height="13" />
          <Path d="M3.5 9.5 H20.5" />
          <Rect x="14" y="12.5" width="4" height="3" />
        </>
      );
    case "people":
      return (
        <>
          <Circle cx="9" cy="8.5" r="3.2" />
          <Path d="M3 20 a6 6 0 0 1 12 0" />
          <Circle cx="16.5" cy="9.5" r="2.5" />
          <Path d="M15.5 14.5 a5 5 0 0 1 5.5 5.5" />
        </>
      );
    case "building":
      return (
        <>
          <Rect x="5" y="3.5" width="14" height="17" />
          <Line x1="8.5" y1="7.5" x2="10.5" y2="7.5" />
          <Line x1="13.5" y1="7.5" x2="15.5" y2="7.5" />
          <Line x1="8.5" y1="11.5" x2="10.5" y2="11.5" />
          <Line x1="13.5" y1="11.5" x2="15.5" y2="11.5" />
          <Rect x="10" y="15.5" width="4" height="5" />
        </>
      );
    case "briefcase":
      return (
        <>
          <Rect x="3.5" y="7.5" width="17" height="12" />
          <Path d="M9 7.5 V5 H15 V7.5" />
          <Line x1="3.5" y1="12.5" x2="20.5" y2="12.5" />
        </>
      );
    case "layers":
      return (
        <>
          <Path d="M12 4 L20.5 8.5 L12 13 L3.5 8.5 Z" />
          <Path d="M3.5 13 L12 17.5 L20.5 13" />
        </>
      );
    case "target":
      return (
        <>
          <Circle cx="12" cy="12" r="8.5" />
          <Circle cx="12" cy="12" r="4.5" />
          <Circle cx="12" cy="12" r="1" />
        </>
      );
    case "star":
      return <Path d="M12 3.5 L14.6 9 L20.5 9.7 L16.2 13.8 L17.3 19.8 L12 16.9 L6.7 19.8 L7.8 13.8 L3.5 9.7 L9.4 9 Z" />;
    case "chart":
      return (
        <>
          <Line x1="4" y1="20" x2="20" y2="20" />
          <Rect x="6" y="11" width="3" height="9" />
          <Rect x="11" y="6" width="3" height="14" />
          <Rect x="16" y="14" width="3" height="6" />
        </>
      );
    case "grid":
      return (
        <>
          <Rect x="3.5" y="3.5" width="17" height="17" />
          <Line x1="3.5" y1="9" x2="20.5" y2="9" />
          <Line x1="3.5" y1="14.5" x2="20.5" y2="14.5" />
          <Line x1="9" y1="3.5" x2="9" y2="20.5" />
          <Line x1="14.5" y1="3.5" x2="14.5" y2="20.5" />
        </>
      );
    case "play":
      return <Path d="M7 4.5 L19 12 L7 19.5 Z" />;
    case "inbox":
      return (
        <>
          <Path d="M3.5 13 L6 5 H18 L20.5 13 V19.5 H3.5 Z" />
          <Path d="M3.5 13 H9 L10.5 15.5 H13.5 L15 13 H20.5" />
        </>
      );
    case "settings":
      return (
        <>
          <Circle cx="12" cy="12" r="3" />
          <Path d="M12 3.5 V6 M12 18 V20.5 M3.5 12 H6 M18 12 H20.5 M6 6 L7.8 7.8 M16.2 16.2 L18 18 M18 6 L16.2 7.8 M7.8 16.2 L6 18" />
        </>
      );
    case "mail":
      return (
        <>
          <Rect x="3.5" y="5.5" width="17" height="13" />
          <Path d="M3.5 6.5 L12 13 L20.5 6.5" />
        </>
      );
    case "phone":
      return <Path d="M6 3.5 H9.5 L11 8 L8.8 9.6 A10 10 0 0 0 14.4 15.2 L16 13 L20.5 14.5 V18 A2.5 2.5 0 0 1 18 20.5 A14.5 14.5 0 0 1 3.5 6 A2.5 2.5 0 0 1 6 3.5 Z" />;
    case "help":
      return (
        <>
          <Circle cx="12" cy="12" r="8.5" />
          <Path d="M9.5 9.5 a2.5 2.5 0 1 1 3.8 2.1 c-1 .6-1.3 1.1-1.3 2.1" />
          <Line x1="12" y1="16.5" x2="12" y2="17.1" />
        </>
      );
    case "accessibility":
      return (
        <>
          <Circle cx="12" cy="5" r="1.6" />
          <Path d="M5 9 H19 M12 9 V14 M12 14 L8.5 20 M12 14 L15.5 20" />
        </>
      );
    case "lock":
      return (
        <>
          <Rect x="5" y="10.5" width="14" height="10" />
          <Path d="M8 10.5 V7.5 a4 4 0 0 1 8 0 V10.5" />
        </>
      );
    case "globe":
      return (
        <>
          <Circle cx="12" cy="12" r="8.5" />
          <Path d="M3.5 12 H20.5 M12 3.5 c3 3 3 14 0 17 M12 3.5 c-3 3 -3 14 0 17" />
        </>
      );
    case "list":
      return (
        <>
          <Line x1="8" y1="6.5" x2="20" y2="6.5" />
          <Line x1="8" y1="12" x2="20" y2="12" />
          <Line x1="8" y1="17.5" x2="20" y2="17.5" />
          <Rect x="3.5" y="5.5" width="2" height="2" />
          <Rect x="3.5" y="11" width="2" height="2" />
          <Rect x="3.5" y="16.5" width="2" height="2" />
        </>
      );
    case "sign-out":
      return (
        <>
          <Path d="M10 4.5 H4.5 V19.5 H10" />
          <Line x1="9" y1="12" x2="20" y2="12" />
          <Polyline points="16 8 20 12 16 16" />
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
