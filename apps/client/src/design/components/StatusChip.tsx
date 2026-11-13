/**
 * Status chips (docs/plans/ux-working-plan.md section 3; specs/007-ux-redesign FR-030, FR-064, X6).
 *
 * Square, a 1 px border, a 6 px mark and a 12 px uppercase label (13 px in Hebrew). Status is
 * carried three ways at once: the colour, the label and the mark's shape (a square for neutral and
 * ready, a tick for success, a bar for warning, a cross for danger, a dot-and-bar for info), so it
 * still reads in grayscale. `ready` is the one filled chip, in `cta.bg`.
 */
import { StyleSheet, View, type StyleProp, type ViewStyle } from "react-native";
import Svg, { Line, Polyline, Rect } from "react-native-svg";

import { useTheme } from "../theme/ThemeProvider";
import { webProps } from "../web";
import { component } from "../theme/theme.generated";
import { Txt } from "./Text";

/** The statuses a chip shows. */
export type ChipStatus = "neutral" | "ready" | "warning" | "success" | "danger" | "info";

export interface StatusChipProps {
  status: ChipStatus;
  label: string;
  testID?: string;
  style?: StyleProp<ViewStyle>;
}

function Mark({ status, color }: { status: ChipStatus; color: string }) {
  const size = component.chip.dot;
  const common = { stroke: color, strokeWidth: 1.6, strokeLinecap: "square" as const, fill: "none" };
  // Hidden from screen readers on its wrapper, as Icon does: react-native-svg passes unknown props
  // to the web's <svg>, so the native ones stay off it (they logged a React error per chip), and
  // aria-hidden reaches the web. The chip's own label speaks for it (R56).
  return (
    <View accessibilityElementsHidden importantForAccessibility="no-hide-descendants" {...webProps({ "aria-hidden": String(true) })}>
      <Svg width={size + 2} height={size + 2} viewBox="0 0 8 8">
        {status === "neutral" || status === "ready" ? <Rect x="1" y="1" width="6" height="6" fill={color} /> : null}
        {status === "success" ? <Polyline points="1 4.5 3.2 6.7 7 1.5" {...common} /> : null}
        {status === "warning" ? (
          <>
            <Line x1="4" y1="0.8" x2="4" y2="4.8" {...common} />
            <Rect x="3.2" y="6.2" width="1.6" height="1.6" fill={color} />
          </>
        ) : null}
        {status === "danger" ? (
          <>
            <Line x1="1" y1="1" x2="7" y2="7" {...common} />
            <Line x1="7" y1="1" x2="1" y2="7" {...common} />
          </>
        ) : null}
        {status === "info" ? (
          <>
            <Rect x="3.2" y="0.4" width="1.6" height="1.6" fill={color} />
            <Line x1="4" y1="3.4" x2="4" y2="7.6" {...common} />
          </>
        ) : null}
      </Svg>
    </View>
  );
}

/** A status chip. */
export function StatusChip({ status, label, testID, style }: StatusChipProps) {
  const { colors } = useTheme();
  const color =
    status === "neutral" ? colors.text.secondary : status === "ready" ? colors.cta.text : colors.status[status];
  const border = status === "neutral" ? colors.border.strong : status === "ready" ? colors.cta.bg : color;
  return (
    <View
      testID={testID}
      accessibilityLabel={label}
      {...webProps({ dataSet: { status } })}
      style={[
        styles.chip,
        { borderColor: border, borderWidth: component.chip.border },
        // Its own surface, so the status colour keeps its contrast on a selected or hovered row.
        { backgroundColor: status === "ready" ? colors.cta.bg : colors.bg.surface },
        style,
      ]}
    >
      <Mark status={status} color={color} />
      <Txt role="eyebrow" tone="inherit" style={{ color }} numberOfLines={1}>
        {label}
      </Txt>
    </View>
  );
}

const styles = StyleSheet.create({
  chip: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
    paddingHorizontal: 8,
    // At least a line's height; a label that wraps makes the chip taller, never spills (FR-079).
    minHeight: 24,
    alignSelf: "flex-start",
  },
});
