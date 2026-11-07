/**
 * Loading placeholders (specs/007-ux-redesign FR-047, FR-071): blocks in `bg.selected` with a
 * shimmer that sweeps across, still under reduce-motion. A group of skeleton rows is one busy
 * region for assistive technology, never "No records".
 */
import { useEffect, useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Animated, StyleSheet, View, type DimensionValue, type StyleProp, type ViewStyle } from "react-native";

import { isRtlLanguage } from "../../i18n";
import { useReducedMotion } from "../motion";
import { useTheme } from "../theme/ThemeProvider";

export interface SkeletonProps {
  width?: DimensionValue;
  height?: number;
  style?: StyleProp<ViewStyle>;
  testID?: string;
}

const SWEEP_MS = 1400;

/** One placeholder block. */
export function Skeleton({ width = "100%", height = 16, style, testID }: SkeletonProps) {
  const { colors } = useTheme();
  const reduced = useReducedMotion();
  const { i18n } = useTranslation();
  const rtl = isRtlLanguage(i18n.language);
  const [sweep] = useState(() => new Animated.Value(0));
  useEffect(() => {
    if (reduced) {
      sweep.setValue(0);
      return undefined;
    }
    const loop = Animated.loop(Animated.timing(sweep, { toValue: 1, duration: SWEEP_MS, useNativeDriver: true }));
    loop.start();
    return () => loop.stop();
  }, [reduced, sweep]);
  const translateX = sweep.interpolate({ inputRange: [0, 1], outputRange: rtl ? [300, -300] : [-300, 300] });
  return (
    <View
      testID={testID}
      accessibilityElementsHidden
      importantForAccessibility="no-hide-descendants"
      style={[styles.block, { width, height, backgroundColor: colors.bg.selected }, style]}
    >
      {reduced ? null : (
        <Animated.View style={[styles.shine, { backgroundColor: colors.bg.surface, transform: [{ translateX }] }]} />
      )}
    </View>
  );
}

export interface SkeletonRowsProps {
  rows?: number;
  /** Row height; the density's by default. */
  rowHeight?: number;
  testID?: string;
  children?: ReactNode;
}

/** Placeholder rows for a list's first load, announced once as "Loading…". */
export function SkeletonRows({ rows = 5, rowHeight, testID }: SkeletonRowsProps) {
  const { t } = useTranslation();
  const { spacing } = useTheme();
  const height = rowHeight ?? spacing.rowHeight;
  return (
    <View testID={testID} accessibilityLabel={t("common.loading")} aria-busy accessibilityRole="progressbar" style={styles.rows}>
      {Array.from({ length: rows }, (_, i) => (
        <View key={i} style={[styles.row, { height }]}>
          <Skeleton width="38%" height={12} />
          <Skeleton width="22%" height={12} />
          <Skeleton width="16%" height={12} />
        </View>
      ))}
    </View>
  );
}

const styles = StyleSheet.create({
  block: { overflow: "hidden" },
  shine: { position: "absolute", top: 0, bottom: 0, width: 120, opacity: 0.6 },
  rows: { gap: 0 },
  row: { flexDirection: "row", alignItems: "center", gap: 16, paddingHorizontal: 4 },
});
