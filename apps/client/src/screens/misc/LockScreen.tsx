/**
 * The lock screen a phone shows when biometrics were declined (specs/007-ux-redesign FR-010): try
 * again, or sign out and forget the kept session.
 */
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { Button, Txt } from "../../design/components";
import { Screen } from "../../design/components/Screen";
import { Icon } from "../../design/icons/Icon";
import { useTheme } from "../../design/theme/ThemeProvider";

export function LockScreen({ onUnlock, onSignOut, busy }: { onUnlock: () => void; onSignOut: () => void; busy: boolean }) {
  const { t } = useTranslation();
  const { colors } = useTheme();
  return (
    <Screen testID="screen-locked">
      <View style={styles.card}>
        <Icon name="lock" size={32} color={colors.text.secondary} />
        <Txt role="display" heading={1}>
          {t("lock.title")}
        </Txt>
        <Txt role="body" tone="secondary">
          {t("lock.body")}
        </Txt>
        <Button variant="primary" label={t("lock.unlock")} onPress={onUnlock} busy={busy} block testID="action-unlock" />
        <Button variant="link" label={t("auth.sign_out")} onPress={onSignOut} testID="action-forget-session" />
      </View>
    </Screen>
  );
}

const styles = StyleSheet.create({
  card: { gap: 16, maxWidth: 420, width: "100%", alignSelf: "center", paddingTop: 24 },
});
