/**
 * A side panel for a record's details beside a table (specs/007-ux-redesign FR-043, A4; FR-071).
 *
 * On a desktop window it sits inline on the trailing side and slides in from its own edge; on a
 * phone it becomes a full sheet. Not modal on desktop, so the table stays usable; it is a
 * `complementary` landmark with its title as its name.
 */
import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Animated, ScrollView, StyleSheet, View, type StyleProp, type ViewStyle } from "react-native";

import { isRtlLanguage } from "../../i18n";
import { usePresence } from "../motion";
import { useTheme } from "../theme/ThemeProvider";
import { Button } from "./Button";
import { BackgroundContext } from "./ClippedBox";
import { Sheet } from "./Sheet";
import { Txt } from "./Text";

export interface SidePanelProps {
  open: boolean;
  onClose: () => void;
  title: string;
  children: ReactNode;
  width?: number;
  footer?: ReactNode;
  testID?: string;
  style?: StyleProp<ViewStyle>;
}

/** A side panel. */
export function SidePanel({ open, onClose, title, children, width = 400, footer, testID, style }: SidePanelProps) {
  const { colors, isDesktop } = useTheme();
  const { t, i18n } = useTranslation();
  const rtl = isRtlLanguage(i18n.language);
  const { shown, progress } = usePresence(open, "surface");

  if (!isDesktop) {
    return (
      <Sheet open={open} onClose={onClose} title={title} footer={footer} testID={testID} placement="bottom">
        {children}
      </Sheet>
    );
  }
  if (!shown) return null;
  // The panel's own edge is the trailing one: the right in English, the left in Hebrew.
  const offset = progress.interpolate({ inputRange: [0, 1], outputRange: [(rtl ? -1 : 1) * 40, 0] });
  return (
    <Animated.View
      testID={testID}
      role="complementary"
      accessibilityLabel={title}
      style={[
        styles.panel,
        { width, backgroundColor: colors.bg.surface, borderColor: colors.border.subtle, opacity: progress },
        { transform: [{ translateX: offset }] },
        style,
      ]}
    >
      <View style={styles.header}>
        <Txt role="section" heading={2} style={styles.title}>
          {title}
        </Txt>
        <Button
          variant="quiet"
          icon="close"
          label=""
          accessibilityLabel={t("common.close")}
          onPress={onClose}
          testID={testID ? `${testID}-close` : undefined}
        />
      </View>
      <ScrollView contentContainerStyle={styles.content}>
        <BackgroundContext.Provider value={colors.bg.surface}>{children}</BackgroundContext.Provider>
      </ScrollView>
      {footer ? <View style={[styles.footer, { borderTopColor: colors.border.subtle }]}>{footer}</View> : null}
    </Animated.View>
  );
}

const styles = StyleSheet.create({
  panel: { alignSelf: "stretch", borderStartWidth: 1, flexShrink: 0 },
  header: { flexDirection: "row", alignItems: "center", justifyContent: "space-between", paddingHorizontal: 20, paddingTop: 16 },
  title: { flexShrink: 1 },
  content: { padding: 20, gap: 12 },
  footer: { flexDirection: "row", gap: 12, justifyContent: "flex-end", padding: 20, borderTopWidth: 1 },
});
