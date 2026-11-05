/** The employee bar: My availability, My shifts and sign-out. */
import { useRouter } from "expo-router";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { useSession } from "../api/SessionContext";
import { isRtl } from "../i18n";
import { Button, Row } from "./ui";

export function EmployeeNav() {
  const { t } = useTranslation();
  const router = useRouter();
  const session = useSession();

  async function signOut(): Promise<void> {
    await session.signOut();
    router.replace("/sign-in");
  }

  return (
    <View
      accessibilityRole="navigation"
      style={[styles.bar, { direction: isRtl() ? "rtl" : "ltr" }]}
    >
      <Row>
        <Button testID="nav-availability" label={t("nav.availability")} onPress={() => router.push("/availability")} quiet />
        <Button testID="nav-shifts" label={t("nav.shifts")} onPress={() => router.push("/shifts")} quiet />
        <Button testID="sign-out-btn" label={t("auth.sign_out")} onPress={() => void signOut()} quiet />
      </Row>
    </View>
  );
}

const styles = StyleSheet.create({
  bar: { paddingHorizontal: 16, paddingVertical: 8, borderBottomWidth: 1, borderBottomColor: "#d1d9e0" },
});
