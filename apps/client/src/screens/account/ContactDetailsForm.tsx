/**
 * The company's contact details and opening hours, for admins (specs/007-ux-redesign N12, D12).
 */
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Pressable, View } from "react-native";

import { call, codeOf, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { Button, InlineBanner, SkeletonRows, TextField, Txt, showToast } from "../../design/components";
import { useTheme } from "../../design/theme/ThemeProvider";
import { DEFAULT_TIME_ZONE, isolateLtr, weekdayNames } from "../../locale";
import { useLoad } from "../useLoad";
import { styles } from "./accountStyles";

/** One weekday's opening hours in the form. */
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
