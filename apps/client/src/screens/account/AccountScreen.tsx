/**
 * The Account screen (specs/007-ux-redesign FR-009, FR-012, FR-059; N12, D12): profile, the
 * language switch (immediate direction on the web, a restart notice on phones), the theme, active
 * sessions with revoke and "sign out of other devices", and, for admins, the company's contact
 * details. Sign-out and the help, contact and accessibility links are the shell's, so they sit in
 * the same place here as everywhere else.
 */
import { Link, useRouter } from "expo-router";
import { useTranslation } from "react-i18next";
import { Platform, View } from "react-native";

import { useSession } from "../../api/SessionContext";
import { Gate } from "../../capabilities";
import { previewsEnabled } from "../../config/previews";
import { Button, PageTitle, SegmentedControl, Txt } from "../../design/components";
import { Screen } from "../../design/components/Screen";
import { THEME_NAMES, type ThemePreference } from "../../design/theme/themeStore";
import { useTheme } from "../../design/theme/ThemeProvider";
import { DestinationRow } from "../misc/HubScreen";
import { ContactDetailsForm } from "./ContactDetailsForm";
import { LanguageSwitch } from "./LanguageSwitch";
import { ProfileForm } from "./ProfileForm";
import { Section } from "./Section";
import { Sessions } from "./Sessions";
import { styles } from "./accountStyles";

const THEME_KEYS: Record<ThemePreference, string> = {
  system: "theme.system",
  light: "theme.light",
  dark: "theme.dark",
  highContrastLight: "theme.high_contrast_light",
  highContrastDark: "theme.high_contrast_dark",
};

/** The Account screen. */
export function AccountScreen() {
  const { t } = useTranslation();
  const theme = useTheme();
  const router = useRouter();
  const session = useSession();
  const signOut = async (): Promise<void> => {
    await session.signOut();
    router.replace("/sign-in");
  };
  return (
    <Screen testID="screen-account">
      <PageTitle eyebrow={[t("app.name"), t("nav.account")]} title={session.user?.full_name ?? t("nav.account")} />
      <Section titleKey="profile.my_profile" testID="account-profile">
        <ProfileForm />
      </Section>
      <Section titleKey="language.label" testID="account-language">
        <LanguageSwitch />
        {Platform.OS !== "web" ? (
          <Txt role="meta" tone="secondary">
            {t("language.restart_notice")}
          </Txt>
        ) : null}
      </Section>
      <Section titleKey="theme.label" testID="account-theme">
        <SegmentedControl
          label={t("theme.label")}
          options={(["system", ...THEME_NAMES] as ThemePreference[]).map((value) => ({ value, label: t(THEME_KEYS[value]) }))}
          value={theme.preference}
          onChange={theme.setPreference}
          testID="account-theme-control"
        />
      </Section>
      <Section titleKey="account.sessions" testID="account-sessions">
        <Sessions />
      </Section>
      {previewsEnabled ? (
        <Section titleKey="nav.notifications" testID="account-notifications">
          <DestinationRow item={{ key: "notifications", href: "/account/notifications", labelKey: "nav.notifications", icon: "inbox", group: "my-work", need: ["account.self"], preview: true }} />
        </Section>
      ) : null}
      <Gate need="organisation.contact.edit">
        <Section titleKey="account.contact_details" testID="account-contact">
          <ContactDetailsForm />
        </Section>
      </Gate>
      <Section titleKey="account.session" testID="account-sign-out">
        <View style={styles.row}>
          <Button variant="secondary" label={t("auth.sign_out")} icon="sign-out" onPress={() => void signOut()} testID="action-sign-out-account" />
          <Link href="/help" asChild>
            <Button variant="link" label={t("nav.help")} />
          </Link>
        </View>
      </Section>
    </Screen>
  );
}

