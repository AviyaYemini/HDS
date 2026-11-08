/**
 * The 404 page (specs/007-ux-redesign FR-077): an unknown address, in the person's language, with
 * a way home: the role's home when signed in, sign-in otherwise.
 */
import { useRouter } from "expo-router";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { useSession, useSignedIn } from "../../api/SessionContext";
import { capabilitiesOf } from "../../capabilities";
import { Button, Txt } from "../../design/components";
import { Screen } from "../../design/components/Screen";
import { Icon } from "../../design/icons/Icon";
import { useTheme } from "../../design/theme/ThemeProvider";
import { homeFor } from "../../navigation/destinations";

/** The 404 page. */
export function NotFoundScreen() {
  const { t } = useTranslation();
  const { colors } = useTheme();
  const router = useRouter();
  const session = useSession();
  const signedIn = useSignedIn();
  const home = signedIn ? homeFor(capabilitiesOf(session.user)) : "/sign-in";
  return (
    <Screen testID="screen-not-found">
      <View style={styles.body}>
        <Icon name="search" size={32} color={colors.text.secondary} />
        <Txt role="display" heading={1}>
          {t("not_found.title")}
        </Txt>
        <Txt role="body" tone="secondary">
          {t("not_found.body")}
        </Txt>
        <Button variant="primary" label={t(signedIn ? "access.go_home" : "auth.sign_in")} icon="home" onPress={() => router.replace(home as never)} testID="action-go-home" />
      </View>
    </Screen>
  );
}

const styles = StyleSheet.create({
  body: { gap: 16, maxWidth: 560 },
});
