/**
 * An inline banner (specs/007-ux-redesign FR-007, FR-047): a message that stays in the page, for
 * refusals, warnings and anything critical that must never time out. A 1 px border and an icon in
 * the status colour, a title, body text and at most one action; warnings and dangers are announced
 * as alerts, the rest as status.
 */
import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View, type StyleProp, type ViewStyle } from "react-native";

import { Icon, type IconName } from "../icons/Icon";
import { useTheme } from "../theme/ThemeProvider";
import { Button } from "./Button";
import { Txt } from "./Text";

/** The banner's meaning. */
export type BannerTone = "info" | "warning" | "danger" | "success";

export interface InlineBannerProps {
  tone?: BannerTone;
  title?: string;
  children?: ReactNode;
  action?: { label: string; onPress: () => void; testID?: string };
  /** Shows a close button; critical banners have none. */
  onDismiss?: () => void;
  testID?: string;
  style?: StyleProp<ViewStyle>;
}

const ICONS: Record<BannerTone, IconName> = { info: "info", warning: "warning", danger: "warning", success: "check" };

/** An inline banner. */
export function InlineBanner({ tone = "info", title, children, action, onDismiss, testID, style }: InlineBannerProps) {
  const { colors } = useTheme();
  const { t } = useTranslation();
  const color = colors.status[tone];
  const urgent = tone === "warning" || tone === "danger";
  return (
    <View
      testID={testID}
      role={urgent ? "alert" : "status"}
      accessibilityLiveRegion={urgent ? "assertive" : "polite"}
      style={[styles.banner, { borderColor: color, backgroundColor: colors.bg.surface }, style]}
    >
      <Icon name={ICONS[tone]} size={20} color={color} style={styles.icon} />
      <View style={styles.body}>
        {title ? (
          <Txt role="label" style={{ color }}>
            {title}
          </Txt>
        ) : null}
        {typeof children === "string" ? <Txt role="body">{children}</Txt> : children}
        {action ? (
          <Button variant="quiet" label={action.label} onPress={action.onPress} testID={action.testID} style={styles.action} />
        ) : null}
      </View>
      {onDismiss ? (
        <Button variant="quiet" icon="close" label="" accessibilityLabel={t("common.dismiss")} onPress={onDismiss} />
      ) : null}
    </View>
  );
}

const styles = StyleSheet.create({
  banner: { flexDirection: "row", gap: 12, padding: 16, borderWidth: 1, alignItems: "flex-start" },
  icon: { marginTop: 2 },
  body: { flex: 1, gap: 6 },
  action: { marginTop: 4, marginStart: -12 },
});
