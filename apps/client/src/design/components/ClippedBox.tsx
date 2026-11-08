/**
 * A box with one clipped corner, Option B's signature shape (docs/plans/ux-working-plan.md
 * section 3): feature panels clip 20 px off the leading top corner, the primary action 14 px.
 *
 * The box has a real background colour (so the browser and the tests see it as such) and the cut
 * is a small triangle in the colour of the surface behind, read from {@link BackgroundContext}:
 * the canvas by default, whatever a sheet, panel or table row provides. In Hebrew the leading
 * corner is the top right one; the triangle uses logical borders, so it follows the direction.
 */
import { createContext, useContext, type ReactNode } from "react";
import { StyleSheet, View, type StyleProp, type ViewProps, type ViewStyle } from "react-native";

import { useTheme } from "../theme/ThemeProvider";

/** The colour of the surface the children sit on; surfaces provide it for the clipped corners. */
export const BackgroundContext = createContext<string | null>(null);

/** The surface colour behind the current element: the context's, or the theme's canvas. */
export function useBackgroundColor(): string {
  const { colors } = useTheme();
  return useContext(BackgroundContext) ?? colors.bg.canvas;
}

export interface ClippedBoxProps extends Omit<ViewProps, "style"> {
  /** The size of the cut, in px. */
  clip: number;
  /** The fill. */
  color: string;
  /** A 1 px edge in this colour, when given (the straight edges only). */
  borderColor?: string;
  /** Which corner is cut; `leading-top` by default, `trailing-bottom` for mirrored pairs. */
  corner?: "leading-top" | "trailing-bottom";
  /** The colour behind the box, when it is not the surrounding surface's. */
  cutColor?: string;
  style?: StyleProp<ViewStyle>;
  children?: ReactNode;
}

/** The polygon's points for a `width` × `height` box with `clip` cut off `corner`, left to right unless `rtl` (for SVG uses). */
export function clippedPoints(width: number, height: number, clip: number, rtl: boolean, corner: "leading-top" | "trailing-bottom" = "leading-top"): string {
  const c = Math.min(clip, width, height);
  const cutTopLeft = corner === "leading-top" ? !rtl : false;
  const cutTopRight = corner === "leading-top" ? rtl : false;
  const cutBottomRight = corner === "trailing-bottom" ? !rtl : false;
  const cutBottomLeft = corner === "trailing-bottom" ? rtl : false;
  const points: [number, number][] = [];
  if (cutTopLeft) points.push([c, 0]);
  else points.push([0, 0]);
  if (cutTopRight) points.push([width - c, 0], [width, c]);
  else points.push([width, 0]);
  if (cutBottomRight) points.push([width, height - c], [width - c, height]);
  else points.push([width, height]);
  if (cutBottomLeft) points.push([c, height], [0, height - c]);
  else points.push([0, height]);
  if (cutTopLeft) points.push([0, c]);
  return points.map(([x, y]) => `${x},${y}`).join(" ");
}

/** A box with one clipped corner. */
export function ClippedBox({ clip, color, borderColor, corner = "leading-top", cutColor, style, children, ...rest }: ClippedBoxProps) {
  const behind = useBackgroundColor();
  const cut = cutColor ?? behind;
  const triangle: ViewStyle =
    corner === "leading-top"
      ? { top: 0, start: 0, borderTopWidth: clip, borderTopColor: cut, borderEndWidth: clip, borderEndColor: "transparent" }
      : { bottom: 0, end: 0, borderBottomWidth: clip, borderBottomColor: cut, borderStartWidth: clip, borderStartColor: "transparent" };
  return (
    <View
      {...rest}
      style={[styles.box, { backgroundColor: color }, borderColor ? { borderWidth: 1, borderColor } : null, style]}
    >
      <BackgroundContext.Provider value={color}>{children}</BackgroundContext.Provider>
      <View pointerEvents="none" style={[styles.triangle, triangle]} />
    </View>
  );
}

const styles = StyleSheet.create({
  box: { position: "relative", overflow: "hidden" },
  triangle: { position: "absolute", width: 0, height: 0 },
});
