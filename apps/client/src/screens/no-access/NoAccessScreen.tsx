/**
 * No access (specs/007-ux-redesign FR-007, N8): a full page for a saved link or a stale permission,
 * with Go home and Contact your manager, naming no record. Opening it reads the account again, so
 * a role that changed elsewhere redraws the navigation. `RefusalBanner` is the inline form for an
 * action the API refused.
 */
import { useRouter } from "expo-router";
import { useEffect } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { useSession } from "../../api/SessionContext";
import { useCapabilities } from "../../capabilities";
import { Button, InlineBanner, Txt } from "../../design/components";
import { Screen } from "../../design/components/Screen";
import { Icon } from "../../design/icons/Icon";
import { useTheme } from "../../design/theme/ThemeProvider";
import { homeFor, shellFor } from "../../navigation/destinations";

/** The no-access page. */
export function NoAccessScreen() {
  const { t } = useTranslation();
  const { colors } = useTheme();
  const router = useRouter();
  const session = useSession();
  const capabilities = useCapabilities();
  const home = homeFor(capabilities);
  const contact = shellFor(capabilities) === "client" ? "/client/contact" : "/contact";

  useEffect(() => {
    void session.reloadMe();
  }, [session]);

  return (
    <Screen testID="screen-no-access">
      <View style={styles.body}>
        <Icon name="lock" size={32} color={colors.text.secondary} />
        <Txt role="display" heading={1}>
          {t("access.no_access_title")}
        </Txt>
        <Txt role="body" tone="secondary">
          {t("access.no_access_body")}
        </Txt>
        <View style={styles.actions}>
          <Button variant="primary" label={t("access.go_home")} icon="home" onPress={() => router.replace(home as never)} testID="action-go-home" />
          <Button variant="secondary" label={t("access.contact_manager")} icon="phone" onPress={() => router.push(contact as never)} testID="action-contact-manager" />
        </View>
      </View>
    </Screen>
  );
}

/** The inline refusal for an action: what happened and what to do, never the record (FR-007). */
export function RefusalBanner({ testID }: { testID?: string }) {
  const { t } = useTranslation();
  return (
    <InlineBanner tone="warning" title={t("access.refused_title")} testID={testID}>
      {t("access.refused_body")}
    </InlineBanner>
  );
}

const styles = StyleSheet.create({
  body: { gap: 16, maxWidth: 560 },
  actions: { flexDirection: "row", flexWrap: "wrap", gap: 12, marginTop: 8 },
});
