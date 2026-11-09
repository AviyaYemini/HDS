/**
 * Pay settings: one form, read-only for whoever may not edit it (specs/007-ux-redesign FR-043 to FR-049).
 */
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { call, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { useCapabilities } from "../../capabilities";
import { InlineBanner, PageTitle, SkeletonRows, Txt, showToast } from "../../design/components";
import { Screen } from "../../design/components/Screen";
import { useLoad } from "../useLoad";
import { RecordForm, type FieldSpec, type Values } from "./RecordForm";

export function PaySettingsScreen() {
  const { t } = useTranslation();
  const { api } = useSession();
  const errorText = useErrorText();
  const capabilities = useCapabilities();
  const settings = useLoad(async () => call(api.GET("/api/v1/organisation/pay-settings")), []);
  const canEdit = capabilities.has("pay_settings.edit");
  const fields: FieldSpec[] = [
    { name: "day_normal_minutes", label: t("pay_settings.day_normal_minutes"), kind: "minutes", required: true, helper: t("pay_settings.minutes_help") },
    { name: "night_normal_minutes", label: t("pay_settings.night_normal_minutes"), kind: "minutes", required: true, helper: t("pay_settings.minutes_help") },
    { name: "rest_pct", label: t("pay_settings.rest_pct"), kind: "percent", required: true, scope: t("scope.rest_pct") },
  ];
  return (
    <Screen testID="screen-admin-pay-settings">
      <PageTitle eyebrow={[t("app.name"), t("nav.group.admin")]} title={t("nav.pay_settings")} />
      {settings.error ? (
        <InlineBanner tone="warning" title={errorText(settings.error)} action={{ label: t("common.retry"), onPress: () => void settings.reload() }} />
      ) : settings.data === null ? (
        <SkeletonRows rows={3} />
      ) : !canEdit ? (
        <View style={styles.block}>
          {fields.map((f) => (
            <Txt key={f.name} role="body">{`${f.label}: ${String((settings.data as unknown as Values)[f.name] ?? "")}`}</Txt>
          ))}
        </View>
      ) : (
        <RecordForm
          existing
          fields={fields}
          initial={{
            day_normal_minutes: String(settings.data.day_normal_minutes),
            night_normal_minutes: String(settings.data.night_normal_minutes),
            rest_pct: settings.data.rest_pct,
          }}
          onSave={async (v) => {
            await call(api.PATCH("/api/v1/organisation/pay-settings", { body: { day_normal_minutes: Number(v.day_normal_minutes), night_normal_minutes: Number(v.night_normal_minutes), rest_pct: v.rest_pct! } }));
            await settings.reload();
            showToast(t("admin.saved"));
          }}
        />
      )}
    </Screen>
  );
}

const styles = StyleSheet.create({
  block: { gap: 8 },
});
