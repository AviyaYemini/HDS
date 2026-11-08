/**
 * A feature panel (docs/plans/ux-working-plan.md section 3): the dark `inverse.bg` block with one
 * 20 px clipped corner on the leading top, used for the next shift, the submission window and a new
 * run. Text inside takes the inverse tones on its own, and the panel reveals its clipped corner
 * when it first appears (FR-071): the cut grows from the corner while the panel settles into place.
 * Nothing fades, so the text keeps its contrast from the first frame.
 */
import { useEffect, useState, type ReactNode } from "react";
import { Animated, StyleSheet, type StyleProp, type ViewStyle } from "react-native";

import { animateTo, useMotion } from "../motion";
import { useTheme } from "../theme/ThemeProvider";
import { component } from "../theme/theme.generated";
import { ClippedBox } from "./ClippedBox";
import { SurfaceContext } from "./Text";

export interface FeaturePanelProps {
  children: ReactNode;
  testID?: string;
  style?: StyleProp<ViewStyle>;
  /** A 1 px edge in `inverse.border`. */
  bordered?: boolean;
}

/** A feature panel. */
export function FeaturePanel({ children, testID, style, bordered = false }: FeaturePanelProps) {
  const { colors } = useTheme();
  const motion = useMotion();
  const [reveal] = useState(() => new Animated.Value(0));
  useEffect(() => {
    void animateTo(reveal, 1, motion.transition("surface"));
  }, [reveal, motion]);
  return (
    <Animated.View style={{ transform: [{ translateY: reveal.interpolate({ inputRange: [0, 1], outputRange: [8, 0] }) }] }}>
      <ClippedBox
        testID={testID}
        clip={component.clip.panel}
        color={colors.inverse.bg}
        borderColor={bordered ? colors.inverse.border : undefined}
        cutStyle={{ transform: [{ scale: reveal }] }}
        style={[styles.panel, style]}
      >
        <SurfaceContext.Provider value="inverse">{children}</SurfaceContext.Provider>
      </ClippedBox>
    </Animated.View>
  );
}

const styles = StyleSheet.create({
  panel: { padding: 20, gap: 12 },
});
