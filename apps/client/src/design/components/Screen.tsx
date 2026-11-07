/**
 * The page frame every screen sits in (docs/plans/ux-working-plan.md section 3; specs/007 FR-057).
 *
 * Draws the canvas, sets the writing direction for the whole tree (React Native Web resolves every
 * start/end style from it), keeps content to 78 rem with 32 px gutters on desktop and 20 px on
 * phones, and scrolls. Group B adds the header, navigation and loading bar around it.
 */
import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { ScrollView, StyleSheet, View, type StyleProp, type ViewStyle } from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";

import { isRtlLanguage } from "../../i18n";
import { useTheme } from "../theme/ThemeProvider";
import { component } from "../theme/theme.generated";
import { webProps } from "../web";

/** 78 rem at 16 px. */
export const CONTENT_MAX_WIDTH = 78 * 16;

export interface ScreenProps {
  children: ReactNode;
  testID?: string;
  /** No ScrollView: the screen scrolls on its own (a roster, a table). */
  fixed?: boolean;
  style?: StyleProp<ViewStyle>;
  contentStyle?: StyleProp<ViewStyle>;
}

/** The direction props for a root view, so logical styles below it resolve for the language. */
export function useDirectionProps(): { style: ViewStyle; web: Record<string, unknown> } {
  const { i18n } = useTranslation();
  const rtl = isRtlLanguage(i18n.language);
  return { style: { direction: rtl ? "rtl" : "ltr" }, web: webProps({ dir: rtl ? "rtl" : "ltr" }) };
}

/** A screen. */
export function Screen({ children, testID, fixed = false, style, contentStyle }: ScreenProps) {
  const { colors, isDesktop, spacing } = useTheme();
  const direction = useDirectionProps();
  const insets = useSafeAreaInsets();
  const gutter = isDesktop ? 32 : spacing.gutter;
  const content: ViewStyle = {
    paddingHorizontal: gutter,
    paddingTop: 24,
    paddingBottom: Math.max(insets.bottom, 24) + component.tabBar.height,
    maxWidth: CONTENT_MAX_WIDTH,
    width: "100%",
    alignSelf: "center",
    gap: 24,
  };
  return (
    <View
      testID={testID}
      {...direction.web}
      style={[styles.root, direction.style, { backgroundColor: colors.bg.canvas }, style]}
    >
      {fixed ? (
        <View style={[content, styles.fill, contentStyle]}>{children}</View>
      ) : (
        <ScrollView style={styles.fill} contentContainerStyle={[content, contentStyle]} keyboardShouldPersistTaps="handled">
          {children}
        </ScrollView>
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  root: { flex: 1 },
  fill: { flex: 1 },
});
