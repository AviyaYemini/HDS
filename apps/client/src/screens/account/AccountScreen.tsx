/**
 * The Account screen (specs/007-ux-redesign FR-009, FR-012, FR-059; N12, D12): profile, the
 * language switch (immediate direction on the web, a restart notice on phones), the theme, active
 * sessions with revoke and "sign out of other devices", and, for admins, the company's contact
 * details. Sign-out and the help, contact and accessibility links are the shell's, so they sit in
 * the same place here as everywhere else.
 */
import { Link, useRouter } from "expo-router";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Platform, Pressable, StyleSheet, View } from "react-native";

import { call, codeOf, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { Gate } from "../../capabilities";
import { previewsEnabled } from "../../config/previews";
import { Button, InlineBanner, PageTitle, SegmentedControl, SkeletonRows, StatusChip, TextField, Txt, focusRing, showToast, type PressState } from "../../design/components";
import { Screen } from "../../design/components/Screen";
import { useKeyboardMode } from "../../design/focus";
import { Icon } from "../../design/icons/Icon";
import { THEME_NAMES, type ThemePreference } from "../../design/theme/themeStore";
import { useTheme } from "../../design/theme/ThemeProvider";
import { component } from "../../design/theme/theme.generated";
import { webProps } from "../../design/web";
import { rememberLanguage, setupI18n } from "../../i18n";
import { DEFAULT_TIME_ZONE, formatHeaderDate, dateOf, formatTime, isolateLtr, weekdayNames } from "../../locale";
import { useLoad } from "../useLoad";
import { DestinationRow } from "../misc/HubScreen";

const LANGUAGES = [
  { code: "he", labelKey: "language.he" },
  { code: "en", labelKey: "language.en" },
] as const;

const THEME_KEYS: Record<ThemePreference, string> = {
  system: "theme.system",
  light: "theme.light",
  dark: "theme.dark",
  highContrastLight: "theme.high_contrast_light",
  highContrastDark: "theme.high_contrast_dark",
};

function Section({ titleKey, children, testID }: { titleKey: string; children: React.ReactNode; testID?: string }) {
  const { t } = useTranslation();
  const { colors } = useTheme();
  return (
    <View testID={testID} style={[styles.section, { borderTopColor: colors.border.subtle }]}>
      <Txt role="section" heading={2}>
        {t(titleKey)}
      </Txt>
      {children}
    </View>
  );
}

/** The language switch: a radio group, applied at once (FR-059). */
export function LanguageSwitch() {
  const { t, i18n } = useTranslation();
  const { colors } = useTheme();
  const keyboard = useKeyboardMode();
  const choose = async (code: string): Promise<void> => {
    if (code === i18n.language) return;
    await setupI18n(code);
    await rememberLanguage(code);
    if (Platform.OS !== "web") showToast(t("language.restart_notice"), { tone: "info", duration: 6000 });
  };
  return (
    <View role="radiogroup" accessibilityLabel={t("language.label")} style={[styles.radios, { borderColor: colors.border.strong }]}>
      {LANGUAGES.map((language) => {
        const checked = i18n.language.startsWith(language.code);
        return (
          <Pressable
            key={language.code}
            testID={`language-${language.code}`}
            role="radio"
            accessibilityState={{ checked }}
            aria-checked={checked}
            onPress={() => void choose(language.code)}
            {...webProps({ tabIndex: checked ? 0 : -1, lang: language.code })}
            style={(state) => [
              styles.radio,
              { minHeight: component.target.phone, borderTopColor: checked ? colors.accent.default : "transparent" },
              checked ? { backgroundColor: colors.bg.selected } : null,
              keyboard && (state as PressState).focused ? focusRing(colors.focus.ring) : null,
            ]}
          >
            <Txt role="label" tone="inherit" style={{ color: checked ? colors.accent.onSelected : colors.text.primary }}>
              {t(language.labelKey)}
            </Txt>
          </Pressable>
        );
      })}
    </View>
  );
}

function ProfileForm() {
  const { t } = useTranslation();
  const session = useSession();
  const errorText = useErrorText();
  const [fullName, setFullName] = useState(session.user?.full_name ?? "");
  const [phone, setPhone] = useState(session.user?.phone ?? "");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const save = async (): Promise<void> => {
    setBusy(true);
    setError(null);
    try {
      await call(session.api.PATCH("/me", { body: { full_name: fullName.trim(), phone: phone.trim() || null } }));
      await session.reloadMe();
      showToast(t("account.saved"));
    } catch (failure) {
      setError(errorText(codeOf(failure)));
    } finally {
      setBusy(false);
    }
  };
  return (
    <View style={styles.form}>
      <TextField label={t("profile.full_name")} value={fullName} onChangeText={setFullName} autoComplete="name" textContentType="name" testID="profile-full-name" />
      <TextField label={t("profile.phone")} value={phone} onChangeText={setPhone} direction="ltr" keyboardType="phone-pad" autoComplete="tel" textContentType="telephoneNumber" testID="profile-phone" />
      <TextField label={t("auth.email")} value={session.user?.email ?? ""} onChangeText={() => undefined} readOnly direction="ltr" testID="profile-email" />
      {error ? <InlineBanner tone="danger" title={error} /> : null}
      <Button variant="primary" label={t("common.save")} onPress={() => void save()} busy={busy} testID="action-save-profile" />
    </View>
  );
}

function Sessions() {
  const { t, i18n } = useTranslation();
  const session = useSession();
  const errorText = useErrorText();
  const { colors } = useTheme();
  const sessions = useLoad(() => call(session.api.GET("/me/sessions")));
  const [busy, setBusy] = useState<string | null>(null);
  const [bulkError, setBulkError] = useState<string | null>(null);
  const items = [...(sessions.data ?? [])].sort((a, b) => b.last_refreshed_at.localeCompare(a.last_refreshed_at));
  const revoke = async (id: string): Promise<void> => {
    setBusy(id);
    try {
      await call(session.api.DELETE("/me/sessions/{id}", { params: { path: { id } } }));
      showToast(t("account.sessions_ended", { count: 1 }));
      await sessions.reload();
    } catch (failure) {
      showToast(errorText(codeOf(failure)), { tone: "warning" });
    } finally {
      setBusy(null);
    }
  };
  // The server keeps the caller's session and ends the rest; when it cannot tell which session is
  // the caller's (409 session.current_unknown) it ends none and says so here.
  const signOutOthers = async (): Promise<void> => {
    setBusy("others");
    setBulkError(null);
    try {
      await call(session.api.POST("/auth/logout-others"));
      showToast(t("account.others_ended"));
      await sessions.reload();
    } catch (failure) {
      setBulkError(errorText(codeOf(failure)));
    } finally {
      setBusy(null);
    }
  };
  if (sessions.error) {
    return <InlineBanner tone="warning" title={errorText(sessions.error)} action={{ label: t("common.retry"), onPress: () => void sessions.reload() }} />;
  }
  if (sessions.data === null) return <SkeletonRows rows={2} testID="sessions-loading" />;
  const others = items.filter((item) => !item.current);
  return (
    <View style={styles.form}>
      {items.map((item) => (
        <View key={item.id} testID={`session-${item.id}`} style={[styles.sessionRow, { borderBottomColor: colors.border.subtle }]}>
          <Icon name={item.client_kind === "native" ? "phone" : "globe"} size={18} color={colors.text.secondary} />
          <View style={styles.sessionText}>
            <View style={styles.row}>
              <Txt role="label">{t(item.client_kind === "native" ? "account.device_phone" : "account.device_web")}</Txt>
              {item.current ? <StatusChip status="ready" label={t("account.this_device")} /> : null}
            </View>
            <Txt role="meta" tone="secondary" numeric>
              {t("account.last_used", {
                date: formatHeaderDate(dateOf(item.last_refreshed_at, DEFAULT_TIME_ZONE), i18n.language),
                time: formatTime(item.last_refreshed_at, DEFAULT_TIME_ZONE),
              })}
            </Txt>
          </View>
          {!item.current ? (
            <Button variant="quiet" label={t("account.end_session")} onPress={() => void revoke(item.id)} busy={busy === item.id} testID={`action-end-session-${item.id}`} />
          ) : null}
        </View>
      ))}
      {bulkError ? <InlineBanner tone="warning" title={bulkError} testID="sign-out-others-error" /> : null}
      <Button
        variant="secondary"
        label={t("account.sign_out_others")}
        icon="sign-out"
        onPress={() => void signOutOthers()}
        disabled={others.length === 0}
        disabledReason={others.length === 0 ? t("account.no_other_devices") : undefined}
        busy={busy === "others"}
        testID="action-sign-out-others"
      />
    </View>
  );
}

interface HoursRow {
  weekday: number;
  open: boolean;
  opens: string;
  closes: string;
}

/** The admin's form for the company's contact details (FR-012). */
export function ContactDetailsForm() {
  const { t, i18n } = useTranslation();
  const session = useSession();
  const errorText = useErrorText();
  const { colors } = useTheme();
  const contact = useLoad(() => call(session.api.GET("/api/v1/organisation/contact")));
  const [phone, setPhone] = useState("");
  const [email, setEmail] = useState("");
  const [hours, setHours] = useState<HoursRow[]>(() =>
    Array.from({ length: 7 }, (_, weekday) => ({ weekday, open: false, opens: "08:00", closes: "17:00" })),
  );
  const [loadedFrom, setLoadedFrom] = useState<unknown>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const days = weekdayNames(i18n.language, "long");

  // The form follows what the server holds each time it is (re)loaded: derived while rendering,
  // React's pattern for state that follows a prop.
  if (contact.data !== null && contact.data !== loadedFrom) {
    const loaded = contact.data;
    setLoadedFrom(loaded);
    setPhone(loaded.phone ?? "");
    setEmail(loaded.email ?? "");
    setHours((rows) =>
      rows.map((row) => {
        const found = loaded.opening_hours.find((entry) => entry.weekday === row.weekday);
        return found ? { weekday: row.weekday, open: true, opens: found.opens, closes: found.closes } : { ...row, open: false };
      }),
    );
  }

  const update = (weekday: number, change: Partial<HoursRow>): void =>
    setHours((rows) => rows.map((row) => (row.weekday === weekday ? { ...row, ...change } : row)));

  const save = async (): Promise<void> => {
    setBusy(true);
    setError(null);
    try {
      await call(
        session.api.PATCH("/api/v1/organisation/contact", {
          body: {
            phone: phone.trim() || null,
            email: email.trim() || null,
            opening_hours: hours.filter((row) => row.open).map(({ weekday, opens, closes }) => ({ weekday, opens, closes })),
          },
        }),
      );
      showToast(t("account.contact_saved"));
      await contact.reload();
    } catch (failure) {
      setError(errorText(codeOf(failure)));
    } finally {
      setBusy(false);
    }
  };

  if (contact.error) {
    return <InlineBanner tone="warning" title={errorText(contact.error)} action={{ label: t("common.retry"), onPress: () => void contact.reload() }} />;
  }
  if (contact.data === null) return <SkeletonRows rows={3} />;
  return (
    <View style={styles.form}>
      <Txt role="body" tone="secondary">
        {t("account.contact_intro")}
      </Txt>
      <TextField label={t("contact.phone")} value={phone} onChangeText={setPhone} direction="ltr" keyboardType="phone-pad" testID="org-contact-phone" />
      <TextField label={t("contact.email")} value={email} onChangeText={setEmail} direction="ltr" keyboardType="email-address" autoCapitalize="none" testID="org-contact-email" />
      <Txt role="eyebrow" tone="secondary">
        {t("contact.opening_hours")}
      </Txt>
      {hours.map((row) => (
        <View key={row.weekday} style={[styles.hoursRow, { borderBottomColor: colors.border.subtle }]} testID={`org-hours-${row.weekday}`}>
          <Pressable
            role="switch"
            accessibilityLabel={days[row.weekday]}
            accessibilityState={{ checked: row.open }}
            aria-checked={row.open}
            onPress={() => update(row.weekday, { open: !row.open })}
            testID={`org-hours-open-${row.weekday}`}
            style={styles.daySwitch}
          >
            <View style={[styles.switchTrack, { borderColor: colors.border.strong, backgroundColor: row.open ? colors.accent.default : "transparent" }]}>
              <View style={[styles.switchThumb, { backgroundColor: row.open ? colors.bg.canvas : colors.border.strong, alignSelf: row.open ? "flex-end" : "flex-start" }]} />
            </View>
            <Txt role="label" style={styles.dayName}>
              {days[row.weekday]}
            </Txt>
          </Pressable>
          {row.open ? (
            <View style={styles.hoursFields}>
              <TextField label={t("contact.opens")} value={row.opens} onChangeText={(opens) => update(row.weekday, { opens })} direction="ltr" placeholder={t("contact.time_placeholder")} containerStyle={styles.timeField} testID={`org-hours-opens-${row.weekday}`} />
              <TextField label={t("contact.closes")} value={row.closes} onChangeText={(closes) => update(row.weekday, { closes })} direction="ltr" placeholder={t("contact.time_placeholder")} containerStyle={styles.timeField} testID={`org-hours-closes-${row.weekday}`} />
            </View>
          ) : (
            <Txt role="meta" tone="secondary">
              {t("contact.closed")}
            </Txt>
          )}
        </View>
      ))}
      {error ? <InlineBanner tone="danger" title={error} testID="org-contact-error" /> : null}
      <Button variant="primary" label={t("common.save")} onPress={() => void save()} busy={busy} testID="action-save-contact" />
      <Txt role="meta" tone="secondary" numeric>
        {isolateLtr(DEFAULT_TIME_ZONE)}
      </Txt>
    </View>
  );
}

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

const styles = StyleSheet.create({
  section: { gap: 14, paddingTop: 20, borderTopWidth: 1 },
  form: { gap: 14, maxWidth: 560 },
  row: { flexDirection: "row", flexWrap: "wrap", gap: 12, alignItems: "center" },
  radios: { flexDirection: "row", borderWidth: 1, alignSelf: "flex-start" },
  radio: { paddingHorizontal: 18, justifyContent: "center", borderTopWidth: component.header.activeRule },
  sessionRow: { flexDirection: "row", alignItems: "center", gap: 12, paddingVertical: 10, borderBottomWidth: 1 },
  sessionText: { flex: 1, gap: 2 },
  hoursRow: { gap: 8, paddingVertical: 10, borderBottomWidth: 1 },
  daySwitch: { flexDirection: "row", alignItems: "center", gap: 12, minHeight: component.target.phone },
  dayName: { flex: 1 },
  switchTrack: { width: 40, height: 22, borderWidth: 1, padding: 2, justifyContent: "center" },
  switchThumb: { width: 16, height: 16 },
  hoursFields: { flexDirection: "row", gap: 12 },
  timeField: { flex: 1, maxWidth: 160 },
});
