/**
 * Set a password from an invitation (specs/007-ux-redesign FR-011, N11): the link carries the
 * token and the invitee's language; a good answer sends the person to sign in.
 */
import { useLocalSearchParams, useRouter } from "expo-router";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { call, codeOf, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { Button, InlineBanner, TextField, Txt, showToast } from "../../design/components";
import { Screen } from "../../design/components/Screen";
import { useTheme } from "../../design/theme/ThemeProvider";

const MIN_LENGTH = 8;

export function SetPasswordScreen() {
  const { t } = useTranslation();
  const router = useRouter();
  const session = useSession();
  const errorText = useErrorText();
  const { colors } = useTheme();
  const { token } = useLocalSearchParams<{ token?: string }>();
  const [password, setPassword] = useState("");
  const [again, setAgain] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const mismatch = again.length > 0 && again !== password;
  const tooShort = password.length > 0 && password.length < MIN_LENGTH;
  // Both fields filled, long enough and the same: only then can the password be set.
  const ready = Boolean(token) && password.length >= MIN_LENGTH && again === password;

  const submit = async (): Promise<void> => {
    if (!ready || busy || !token) return;
    setBusy(true);
    setError(null);
    try {
      await call(session.api.POST("/auth/invite/accept", { body: { token, password } }));
      showToast(t("set_password.done"));
      router.replace("/sign-in");
    } catch (failure) {
      setError(errorText(codeOf(failure)));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Screen testID="screen-set-password">
      <View style={styles.card}>
        <View style={styles.brand}>
          <View style={[styles.brandMark, { backgroundColor: colors.cta.bg }]} />
          <Txt role="eyebrow" tone="secondary">
            {t("app.name")}
          </Txt>
        </View>
        <Txt role="display" heading={1}>
          {t("set_password.title")}
        </Txt>
        <Txt role="body" tone="secondary">
          {t("set_password.intro")}
        </Txt>
        {!token ? <InlineBanner tone="warning" title={t("set_password.no_token")} /> : null}
        <TextField
          testID="new-password"
          label={t("set_password.password")}
          value={password}
          onChangeText={setPassword}
          secureTextEntry
          autoComplete="new-password"
          textContentType="newPassword"
          helper={t("set_password.rule", { count: MIN_LENGTH })}
          error={tooShort ? t("set_password.too_short", { count: MIN_LENGTH }) : null}
        />
        <TextField
          testID="new-password-again"
          label={t("set_password.again")}
          value={again}
          onChangeText={setAgain}
          secureTextEntry
          autoComplete="new-password"
          textContentType="newPassword"
          error={mismatch ? t("set_password.mismatch") : null}
          onSubmitEditing={() => void submit()}
        />
        {error ? <InlineBanner tone="danger" title={error} testID="error-message" /> : null}
        <Button
          testID="action-set-password"
          variant="primary"
          label={t("set_password.submit")}
          onPress={() => void submit()}
          busy={busy}
          disabled={!ready}
          block
        />
      </View>
    </Screen>
  );
}

const styles = StyleSheet.create({
  card: { gap: 16, maxWidth: 420, width: "100%", alignSelf: "center", paddingTop: 24 },
  brand: { flexDirection: "row", alignItems: "center", gap: 10 },
  brandMark: { width: 14, height: 14 },
});
