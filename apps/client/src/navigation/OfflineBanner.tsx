/**
 * The offline banner (specs/007-ux-redesign FR-077): a thin inline notice under the header while
 * the network is down, gone as soon as it is back. Announced once, never timed out.
 */
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { useOnline } from "../connectivity";
import { Txt } from "../design/components";
import { Icon } from "../design/icons/Icon";
import { useTheme } from "../design/theme/ThemeProvider";

/** The banner; renders nothing while online. */
export function OfflineBanner() {
  const online = useOnline();
  const { t } = useTranslation();
  const { colors } = useTheme();
  if (online) return null;
  return (
    <View
      testID="banner-offline"
      role="status"
      accessibilityLiveRegion="polite"
      style={[styles.banner, { backgroundColor: colors.inverse.bg, borderBottomColor: colors.inverse.border }]}
    >
      <Icon name="globe" size={16} color={colors.inverse.text} />
      <Txt role="meta" tone="inherit" style={{ color: colors.inverse.text }}>
        {t("offline.banner")}
      </Txt>
    </View>
  );
}

const styles = StyleSheet.create({
  banner: { flexDirection: "row", alignItems: "center", gap: 8, paddingHorizontal: 16, paddingVertical: 8, borderBottomWidth: 1 },
});
