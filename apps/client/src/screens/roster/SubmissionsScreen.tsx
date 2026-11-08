/**
 * Who has submitted availability for a window (specs/007-ux-redesign FR-040, T074): every staff
 * member with their status, whether a missed deadline counts them as unavailable, and an Admin's hand
 * change beside the submission (T101).
 */
import { useLocalSearchParams } from "expo-router";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { call, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { EmptyState, InlineBanner, PageTitle, SkeletonRows, StatusChip, Txt } from "../../design/components";
import { Screen } from "../../design/components/Screen";
import { useTheme } from "../../design/theme/ThemeProvider";
import { webProps } from "../../design/web";
import { addDays, formatNumericDate, DEFAULT_TIME_ZONE, dateOf, todayIn } from "../../locale";
import { nearestOpenWindow } from "../availability";
import { useLoad } from "../useLoad";

const TONE = { not_submitted: "warning", draft: "neutral", submitted: "success", locked: "neutral" } as const;

export function SubmissionsScreen() {
  const { t } = useTranslation();
  const { window } = useLocalSearchParams<{ window?: string }>();
  const session = useSession();
  const errorText = useErrorText();
  const { colors } = useTheme();
  // Without ?window=, the window whose deadline comes next, as the home's card follows (R117).
  const rows = useLoad(async () => {
    let id: string | null = window ?? null;
    if (id === null) {
      const today = todayIn(DEFAULT_TIME_ZONE);
      const windows = (await call(session.api.GET("/api/v1/availability-windows", { params: { query: { from: today, to: addDays(today, 60) } } }))).items;
      id = nearestOpenWindow(windows, Date.now())?.id ?? null;
    }
    if (id === null) return { items: null };
    return { items: (await call(session.api.GET("/api/v1/availability-windows/{id}/submissions", { params: { path: { id } } }))).items };
  }, [window]);
  return (
    <Screen testID="screen-submissions">
      <PageTitle eyebrow={[t("app.name"), t("nav.group.manage")]} title={t("nav.submissions")} />
      {rows.error ? (
        <InlineBanner tone="warning" title={errorText(rows.error)} action={{ label: t("common.retry"), onPress: () => void rows.reload() }} />
      ) : rows.data === null ? (
        <SkeletonRows rows={5} />
      ) : rows.data.items === null ? (
        <EmptyState icon="calendar" title={t("submissions.no_window")} body={t("submissions.no_window_body")} />
      ) : (
        <View style={styles.list}>
          {rows.data.items.map((row) => (
            <View key={row.user_id} testID="submission-row" {...webProps({ dataSet: { userId: row.user_id, status: row.status } })} style={[styles.row, { borderColor: colors.border.subtle }]}>
              <Txt role="label" style={styles.name}>
                {row.full_name}
              </Txt>
              <StatusChip status={TONE[row.status]} label={t(`availability.status.${row.status}`)} />
              {row.counted_unavailable ? (
                <Txt role="meta" tone="warning">
                  {t("submissions.counted_unavailable")}
                </Txt>
              ) : null}
              {row.submitted_at ? (
                <Txt role="meta" tone="secondary" numeric>
                  {formatNumericDate(dateOf(row.submitted_at, DEFAULT_TIME_ZONE))}
                </Txt>
              ) : null}
              {row.changed_at ? (
                <Txt role="meta" tone="secondary" testID="submission-changed">
                  {t("submissions.changed_by_admin", { date: formatNumericDate(dateOf(row.changed_at, DEFAULT_TIME_ZONE)) })}
                </Txt>
              ) : null}
            </View>
          ))}
        </View>
      )}
    </Screen>
  );
}

const styles = StyleSheet.create({
  list: { gap: 8 },
  row: { flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 12, padding: 12, borderWidth: 1 },
  name: { flexGrow: 1, minWidth: 160 },
});
