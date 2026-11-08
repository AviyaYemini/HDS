/**
 * Sheets and side panels (specs/007-ux-redesign FR-063, FR-071; research R14).
 *
 * One modal surface with two placements: a bottom sheet on phones, and on a desktop window a panel
 * that slides in from the leading edge (the right in Hebrew). Both are dialogs: labelled by their
 * title, closed by Escape, the backdrop or the close button, with focus kept inside on the web by
 * React Native's Modal. The surface animates with `Animated` so reduce-motion stops it dead and
 * input is never blocked.
 */
import { useEffect, useRef, type ComponentRef, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import {
  Animated,
  Modal,
  Pressable,
  ScrollView,
  StyleSheet,
  useWindowDimensions,
  View,
  type StyleProp,
  type ViewStyle,
} from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";

import { isRtlLanguage } from "../../i18n";
import { usePresence } from "../motion";
import { useTheme } from "../theme/ThemeProvider";
import { webProps } from "../web";
import { Button } from "./Button";
import { BackgroundContext } from "./ClippedBox";
import { Txt } from "./Text";

/** Where the surface comes from. */
export type SheetPlacement = "bottom" | "leading";

export interface SheetProps {
  open: boolean;
  onClose: () => void;
  title: string;
  children: ReactNode;
  /** Decided by the window by default: `bottom` on phones, `leading` on a desktop window. */
  placement?: SheetPlacement;
  /** The panel's width on the leading edge; 420 by default. */
  width?: number;
  /** Buttons along the bottom of the sheet. */
  footer?: ReactNode;
  testID?: string;
  style?: StyleProp<ViewStyle>;
}

/** A sheet or side panel. */
export function Sheet({ open, onClose, title, children, placement, width = 420, footer, testID, style }: SheetProps) {
  const { colors, isDesktop } = useTheme();
  const { t, i18n } = useTranslation();
  const insets = useSafeAreaInsets();
  const window = useWindowDimensions();
  const where: SheetPlacement = placement ?? (isDesktop ? "leading" : "bottom");
  const rtl = isRtlLanguage(i18n.language);
  const { shown, progress } = usePresence(open, "surface");
  // React Native Web's Modal is the dialog; it takes the name. On phones the inner view is modal.
  const dialogWebProps = webProps({ "aria-label": title });
  const innerWebProps = webProps({ tabIndex: -1 });
  const surfaceRef = useRef<ComponentRef<typeof View>>(null);

  useEffect(() => {
    if (open) (surfaceRef.current as unknown as { focus?: () => void } | null)?.focus?.();
  }, [open]);

  if (!shown) return null;

  const distance = where === "bottom" ? Math.min(window.height, 640) : width + 40;
  const sign = where === "bottom" ? 1 : rtl ? 1 : -1;
  const offset = progress.interpolate({ inputRange: [0, 1], outputRange: [sign * distance, 0] });
  const surfaceStyle: ViewStyle =
    where === "bottom"
      ? { ...styles.bottom, paddingBottom: Math.max(insets.bottom, 16), maxHeight: window.height * 0.9 }
      : { ...styles.leading, width: Math.min(width, window.width - 40), paddingTop: insets.top };

  return (
    <Modal visible transparent animationType="none" onRequestClose={onClose} statusBarTranslucent {...dialogWebProps}>
      <View style={styles.root}>
        <Pressable
          accessibilityLabel={t("common.close")}
          accessibilityRole="button"
          onPress={onClose}
          style={styles.backdropPress}
          {...webProps({ tabIndex: -1 })}
        >
          <Animated.View style={[styles.backdrop, { opacity: progress }]} />
        </Pressable>
        <Animated.View
          style={[
            styles.surface,
            surfaceStyle,
            { backgroundColor: colors.bg.canvas, borderColor: colors.border.strong },
            where === "bottom" ? { transform: [{ translateY: offset }] } : { transform: [{ translateX: offset }] },
            style,
          ]}
        >
          <View
            ref={surfaceRef}
            testID={testID}
            accessibilityLabel={title}
            accessibilityViewIsModal
            {...innerWebProps}
            style={styles.dialog}
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
            <ScrollView contentContainerStyle={styles.content} keyboardShouldPersistTaps="handled">
              <BackgroundContext.Provider value={colors.bg.canvas}>{children}</BackgroundContext.Provider>
            </ScrollView>
            {footer ? <View style={[styles.footer, { borderTopColor: colors.border.subtle }]}>{footer}</View> : null}
          </View>
        </Animated.View>
      </View>
    </Modal>
  );
}

const styles = StyleSheet.create({
  root: { flex: 1 },
  backdropPress: { position: "absolute", top: 0, bottom: 0, start: 0, end: 0 },
  backdrop: { flex: 1, backgroundColor: "rgba(16, 20, 19, 0.45)" },
  surface: { position: "absolute", borderWidth: 1 },
  bottom: { left: 0, right: 0, bottom: 0, borderBottomWidth: 0 },
  leading: { top: 0, bottom: 0, start: 0, borderStartWidth: 0 },
  dialog: { flexShrink: 1, flexGrow: 1 },
  header: { flexDirection: "row", alignItems: "center", justifyContent: "space-between", paddingHorizontal: 20, paddingTop: 16 },
  title: { flexShrink: 1 },
  content: { padding: 20, gap: 12 },
  footer: { flexDirection: "row", gap: 12, justifyContent: "flex-end", padding: 20, borderTopWidth: 1 },
});
