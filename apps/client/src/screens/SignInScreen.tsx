/** Sign-in: email and password; a refusal is shown with the API's code translated. */
import { useRouter } from "expo-router";
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { useErrorText } from "../api/errors";
import { useSession } from "../api/SessionContext";
import { Button, ErrorText, Field, Screen, Title } from "../components/ui";

export function SignInScreen() {
  const { t } = useTranslation();
  const errorText = useErrorText();
  const session = useSession();
  const router = useRouter();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function submit(): Promise<void> {
    setBusy(true);
    setError(null);
    const refused = await session.signIn(email.trim(), password);
    setBusy(false);
    if (refused === null) router.replace("/availability");
    else setError(refused);
  }

  const lines =
    error === "auth.account_locked"
      ? [t("auth.account_locked"), t("auth.account_locked_detail")]
      : error !== null
        ? [errorText(error)]
        : [];

  return (
    <Screen>
      <Title>{t("auth.sign_in")}</Title>
      <Field testID="email" label={t("auth.email")} value={email} onChange={setEmail} email />
      <Field testID="password" label={t("auth.password")} value={password} onChange={setPassword} secure />
      {lines.length > 0 ? <ErrorText lines={lines} /> : null}
      <Button testID="sign-in-btn" label={t("auth.sign_in")} onPress={() => void submit()} disabled={busy} />
    </Screen>
  );
}
