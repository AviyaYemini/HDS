/**
 * Sign-in (001 US1; specs/007-ux-redesign FR-010, FR-064, FR-077): the branded sign-in in Option B.
 * Paste and password managers work (autocomplete hints, no puzzle), a refusal shows the API's code
 * translated, a session that ended says so, and a good sign-in opens the role's home.
 */
import { useLocalSearchParams, useRouter } from "expo-router";
import { useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { useErrorText } from "../api/errors";
import { useSession } from "../api/SessionContext";
import { safeReturnPath } from "../auth/returnPath";
import { forgetLocation } from "../auth/SignInRedirect";
import { clearSessionEnded, useSessionEnded } from "../auth/sessionEnded";
import { capabilitiesOf } from "../capabilities";
import { Button, FeaturePanel, InlineBanner, SurfaceContext, TextField, Txt } from "../design/components";
import { useDocumentTitle } from "../design/components/PageTitle";
import { Screen, useDirectionProps } from "../design/components/Screen";
import { useTheme } from "../design/theme/ThemeProvider";
import { homeFor } from "../navigation/destinations";
import { LanguageToggle } from "../navigation/DisplayControls";

export function SignInScreen() {
  const { t } = useTranslation();
  const errorText = useErrorText();
  const session = useSession();
  const router = useRouter();
  const { colors, isDesktop } = useTheme();
  const direction = useDirectionProps();
  useDocumentTitle(t("auth.sign_in"));
  const ended = useSessionEnded();
  const { next } = useLocalSearchParams<{ next?: string | string[] }>();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  // One sign-in at a time: a second Enter while the first is in flight would open a second session.
  const inFlight = useRef(false);

  async function submit(): Promise<void> {
    if (inFlight.current) return;
    inFlight.current = true;
    setBusy(true);
    setError(null);
    const refused = await session.signIn(email.trim(), password);
    inFlight.current = false;
    setBusy(false);
    if (refused === null) {
      clearSessionEnded();
      forgetLocation();
      // Back to the address the person was sent from, when it is one of this app's (ui-routes.md).
      router.replace((safeReturnPath(next) ?? homeFor(capabilitiesOf(session.user))) as never);
    } else {
      setError(refused);
    }
  }

  const lines =
    error === "auth.account_locked"
      ? [t("auth.account_locked"), t("auth.account_locked_detail")]
      : error !== null
        ? [errorText(error)]
        : [];

  const form = (
    <View style={styles.card}>
      <View style={styles.brand}>
        <View style={[styles.brandMark, { backgroundColor: colors.cta.bg }]} />
        <Txt role="eyebrow" tone="secondary">
          {t("app.name")}
        </Txt>
      </View>
      <Txt role="display" heading={1}>
        {t("auth.sign_in")}
      </Txt>
      <Txt role="body" tone="secondary">
        {t("auth.sign_in_intro")}
      </Txt>
      {ended ? <InlineBanner tone="info" testID="banner-session-ended">{t("auth.token_expired")}</InlineBanner> : null}
      <TextField
        testID="email"
        label={t("auth.email")}
        value={email}
        onChangeText={setEmail}
        direction="ltr"
        keyboardType="email-address"
        autoCapitalize="none"
        autoCorrect={false}
        autoComplete="email"
        textContentType="emailAddress"
        returnKeyType="next"
      />
      <TextField
        testID="password"
        label={t("auth.password")}
        value={password}
        onChangeText={setPassword}
        secureTextEntry
        autoCapitalize="none"
        autoCorrect={false}
        autoComplete="current-password"
        textContentType="password"
        returnKeyType="go"
        onSubmitEditing={() => void submit()}
      />
      {lines.length > 0 ? (
        <InlineBanner tone="danger" testID="error-message" title={lines[0]}>
          {lines.slice(1).join(" ")}
        </InlineBanner>
      ) : null}
      <Button testID="sign-in-btn" variant="primary" label={t("auth.sign_in")} onPress={() => void submit()} busy={busy} block />
    </View>
  );

  if (isDesktop) {
    // The deck's title slide: the form on the reading side, the dark brand panel beside it.
    return (
      <View testID="screen-sign-in" {...direction.web} style={[styles.split, direction.style, { backgroundColor: colors.bg.canvas }]}>
        <View style={styles.formSide}>
          <View style={styles.display}>
            <LanguageToggle />
          </View>
          <View style={styles.formCentre}>{form}</View>
          <Txt role="meta" tone="secondary">
            {t("brand.company")}
          </Txt>
        </View>
        <View testID="brand-panel" style={[styles.panel, { backgroundColor: colors.inverse.bg }]}>
          <SurfaceContext.Provider value="inverse">
            <Txt role="eyebrow" tone="accent">
              {t("brand.eyebrow")}
            </Txt>
            <Txt role="display" style={styles.tagline}>
              {t("brand.tagline")}
            </Txt>
            <View style={[styles.quiet, { borderTopColor: colors.inverse.border }]}>
              <Txt role="body" tone="secondary">
                {t("brand.line_product")}
              </Txt>
              <Txt role="body" tone="secondary">
                {t("brand.line_platforms")}
              </Txt>
            </View>
          </SurfaceContext.Provider>
        </View>
      </View>
    );
  }

  return (
    <Screen testID="screen-sign-in">
      {/* Where someone first needs it: the language before signing in (the owner, PR 4). */}
      <View style={styles.display}>
        <LanguageToggle />
      </View>
      {/* A phone: the brand as a compact dark block above the form (the deck's slide 2). */}
      <FeaturePanel testID="brand-panel">
        <Txt role="eyebrow" tone="accent">
          {t("brand.eyebrow")}
        </Txt>
        <Txt role="section">{t("brand.tagline")}</Txt>
      </FeaturePanel>
      {form}
    </Screen>
  );
}

const styles = StyleSheet.create({
  display: { flexDirection: "row", justifyContent: "flex-end" },
  card: { gap: 16, maxWidth: 420, width: "100%", alignSelf: "center", paddingTop: 24 },
  split: { flex: 1, flexDirection: "row" },
  formSide: { flex: 1.15, padding: 48, justifyContent: "space-between", gap: 24 },
  formCentre: { flex: 1, justifyContent: "center" },
  panel: { flex: 1, padding: 64, justifyContent: "center", gap: 20 },
  tagline: { fontSize: 44, lineHeight: 50 },
  quiet: { borderTopWidth: 1, paddingTop: 20, marginTop: 16, gap: 6 },
  brand: { flexDirection: "row", alignItems: "center", gap: 10 },
  brandMark: { width: 14, height: 14 },
});
