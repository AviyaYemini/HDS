/**
 * The person's own profile (specs/007-ux-redesign FR-009).
 */
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { View } from "react-native";

import { call, codeOf, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { Button, InlineBanner, TextField, showToast } from "../../design/components";
import { styles } from "./accountStyles";

export function ProfileForm() {
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
