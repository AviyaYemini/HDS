/**
 * Help and the accessibility statement (specs/007-ux-redesign FR-009, FR-066; X10, L15): short
 * pages in one consistent place. The statement's text is a placeholder the owner replaces in group
 * H; the page exists now so every screen can link to it.
 */
import { Link } from "expo-router";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { useCapabilities } from "../../capabilities";
import { Button, PageTitle, Txt } from "../../design/components";
import { Screen } from "../../design/components/Screen";
import { shellFor } from "../../navigation/destinations";

/** Help: what the app is for and where to turn. */
export function HelpScreen() {
  const { t } = useTranslation();
  const capabilities = useCapabilities();
  const contact = shellFor(capabilities) === "client" ? "/client/contact" : "/contact";
  return (
    <Screen testID="screen-help">
      <PageTitle eyebrow={[t("app.name"), t("nav.help")]} title={t("help.title")} />
      <View style={styles.section}>
        <Txt role="section" heading={2}>
          {t("help.signing_in_title")}
        </Txt>
        <Txt role="body">{t("help.signing_in_body")}</Txt>
      </View>
      <View style={styles.section}>
        <Txt role="section" heading={2}>
          {t("help.screens_title")}
        </Txt>
        <Txt role="body">{t("help.screens_body")}</Txt>
      </View>
      <View style={styles.section}>
        <Txt role="section" heading={2}>
          {t("help.contact_title")}
        </Txt>
        <Txt role="body">{t("help.contact_body")}</Txt>
        <View style={styles.row}>
          <Link href={contact as never} asChild>
            <Button variant="secondary" label={t("nav.contact")} icon="phone" />
          </Link>
          <Link href="/accessibility" asChild>
            <Button variant="link" label={t("nav.accessibility")} />
          </Link>
        </View>
      </View>
    </Screen>
  );
}

/** The accessibility statement page; its text comes from the owner (FR-066). */
export function AccessibilityScreen() {
  const { t } = useTranslation();
  return (
    <Screen testID="screen-accessibility">
      <PageTitle eyebrow={[t("app.name"), t("nav.accessibility")]} title={t("accessibility.title")} />
      <View style={styles.section}>
        <Txt role="body">{t("accessibility.commitment")}</Txt>
        <Txt role="body">{t("accessibility.standard")}</Txt>
        <Txt role="body" tone="secondary">
          {t("accessibility.statement_pending")}
        </Txt>
      </View>
      <View style={styles.section}>
        <Txt role="section" heading={2}>
          {t("accessibility.feedback_title")}
        </Txt>
        <Txt role="body">{t("accessibility.feedback_body")}</Txt>
      </View>
    </Screen>
  );
}

const styles = StyleSheet.create({
  section: { gap: 10, maxWidth: 640 },
  row: { flexDirection: "row", flexWrap: "wrap", gap: 12, alignItems: "center" },
});
