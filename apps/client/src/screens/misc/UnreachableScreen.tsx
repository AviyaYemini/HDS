/**
 * The start screen when HDS cannot be reached (specs/007-ux-redesign FR-010; PR 2 review): kept
 * tokens may still be good, so the person is offered Retry first, with signing in again as the
 * second way. A retry also runs on its own when the app comes back to the foreground.
 */
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { Button, Txt } from "../../design/components";
import { Screen } from "../../design/components/Screen";
import { Icon } from "../../design/icons/Icon";
import { useTheme } from "../../design/theme/ThemeProvider";

export function UnreachableScreen({ onRetry, onSignIn, busy }: { onRetry: () => void; onSignIn: () => void; busy: boolean }) {
  const { t } = useTranslation();
  const { colors } = useTheme();
  return (
    <Screen testID="screen-unreachable">
      <View style={styles.card} role="status">
        <Icon name="globe" size={32} color={colors.text.secondary} />
        <Txt role="display" heading={1}>
          {t("unreachable.title")}
        </Txt>
        <Txt role="body" tone="secondary">
          {t("unreachable.body")}
        </Txt>
        <Button variant="primary" label={t("common.retry")} onPress={onRetry} busy={busy} block testID="action-retry" />
        <Button variant="link" label={t("unreachable.sign_in")} onPress={onSignIn} testID="action-sign-in-again" />
      </View>
    </Screen>
  );
}

const styles = StyleSheet.create({
  card: { gap: 16, maxWidth: 420, width: "100%", alignSelf: "center", paddingTop: 24 },
});
