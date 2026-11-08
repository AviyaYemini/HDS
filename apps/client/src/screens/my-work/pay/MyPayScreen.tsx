/**
 * My pay (specs/007-ux-redesign FR-024; task T059): a pay-period picker, an Hours view (scheduled
 * and worked, approved hours as a preview) and a Pay view (the estimated gross, the breakdown by
 * rate that adds up exactly, the change since the last period), labelled an estimate before tax.
 * Money is a decimal string from the API all the way to `formatMoney`.
 */
import { useLocalSearchParams } from "expo-router";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { call, useErrorText } from "../../../api/errors";
import { useSession } from "../../../api/SessionContext";
import { previewsEnabled } from "../../../config/previews";
import { Button, EmptyState, FeaturePanel, InlineBanner, PageTitle, SegmentedControl, SkeletonRows, Txt } from "../../../design/components";
import { Screen } from "../../../design/components/Screen";
import { useTheme } from "../../../design/theme/ThemeProvider";
import { webProps } from "../../../design/web";
import { DEFAULT_TIME_ZONE, durationParts, endOfMonth, formatMoney, formatMonthYear, minutesBetween, todayIn } from "../../../locale";
import { useLoad } from "../../useLoad";
import { breakdownByRate, changeSince, nextPeriod, previousPeriod, shortHours } from "./model";

type Mode = "hours" | "pay";
const PERIOD = /^\d{4}-(0[1-9]|1[0-2])$/;

export function MyPayScreen() {
  const { t, i18n } = useTranslation();
  const session = useSession();
  const errorText = useErrorText();
  const { colors, isDesktop } = useTheme();
  const { period: askedPeriod, view: askedView } = useLocalSearchParams<{ period?: string; view?: string }>();
  const current = todayIn(DEFAULT_TIME_ZONE).slice(0, 7);
  const [period, setPeriod] = useState(askedPeriod && PERIOD.test(askedPeriod) ? askedPeriod : current);
  const [view, setView] = useState<Mode>(askedView === "hours" ? "hours" : "pay");

  const pay = useLoad(() => call(session.api.GET("/me/pay", { params: { query: { month: period } } })), [period]);
  const previous = useLoad(() => call(session.api.GET("/me/pay", { params: { query: { month: previousPeriod(period) } } })), [period]);
  const scheduled = useLoad(async () => {
    // The API takes dates at the organisation's zone, so the month's own last day bounds it (R56).
    const from = `${period}-01`;
    const answer = await call(session.api.GET("/me/shifts", { params: { query: { from, to: endOfMonth(from) } } }));
    return answer.items.reduce((sum, shift) => sum + minutesBetween(shift.starts_at, shift.ends_at), 0);
  }, [period]);

  const options = [
    { value: "hours" as Mode, label: t("pay.view_hours"), testID: "view-hours" },
    { value: "pay" as Mode, label: t("pay.view_pay"), testID: "view-pay" },
  ];
  const money = (amount: string) => formatMoney(amount, i18n.language);

  return (
    <Screen testID="screen-my-pay">
      <PageTitle eyebrow={[t("app.name"), t("nav.group.my_work")]} title={t("pay.title")}>
        <SegmentedControl options={options} value={view} onChange={setView} label={t("pay.view_label")} testID="pay-view" />
      </PageTitle>
      <View testID="pay-period-picker" role="group" accessibilityLabel={t("pay.period_label")} style={styles.picker}>
        <Button variant="quiet" icon="chevron-back" label={isDesktop ? t("pay.prev_period") : ""} accessibilityLabel={t("pay.prev_period")} onPress={() => setPeriod(previousPeriod(period))} testID="pay-period-prev" />
        <Txt role="section" heading={2} style={styles.pickerTitle} testID="pay-period">
          {formatMonthYear(`${period}-01`, i18n.language)}
        </Txt>
        <Button variant="quiet" icon="chevron-next" iconPosition="trailing" label={isDesktop ? t("pay.next_period") : ""} accessibilityLabel={t("pay.next_period")} onPress={() => setPeriod(nextPeriod(period))} disabled={period >= current} testID="pay-period-next" />
      </View>
      {pay.error ? (
        <InlineBanner tone="warning" title={errorText(pay.error)} action={{ label: t("common.retry"), onPress: () => void pay.reload() }} />
      ) : pay.data === null ? (
        <SkeletonRows rows={4} testID="pay-loading" />
      ) : view === "pay" ? (
        <View style={styles.stack}>
          <FeaturePanel testID="pay-panel">
            <Txt role="eyebrow" tone="secondary" testID="pay-estimate-label">
              {t("pay.estimate_label")}
            </Txt>
            <Txt role="display" heading={2} numeric testID="pay-total" {...webProps({ dataSet: { amount: pay.data.pay ?? "0" } })}>
              {money(pay.data.pay ?? "0")}
            </Txt>
            <Txt role="body" tone="secondary" numeric>
              {t("pay.worked_hours", { hours: shortHours(pay.data.hours) })}
            </Txt>
            {previous.data ? (
              <Txt role="meta" tone="secondary" numeric testID="pay-change">
                {changeText(changeSince(pay.data.pay ?? "0", previous.data.pay ?? "0"))}
              </Txt>
            ) : null}
          </FeaturePanel>
          {pay.data.shifts_missing_rate > 0 ? <InlineBanner tone="warning" title={t("pay.missing_count", { count: pay.data.shifts_missing_rate })} /> : null}
          <View style={[styles.table, { borderColor: colors.border.subtle }]}>
            <Txt role="eyebrow" tone="secondary">
              {t("pay.by_rate")}
            </Txt>
            {breakdownByRate(pay.data).length === 0 ? (
              <Txt role="body" tone="secondary">
                {t("pay.empty")}
              </Txt>
            ) : (
              breakdownByRate(pay.data).map((row) => (
                <View key={row.ratePct} testID="pay-breakdown-row" {...webProps({ dataSet: { amount: row.amount, rate: row.ratePct } })} style={[styles.tableRow, { borderTopColor: colors.border.subtle }]}>
                  <Txt role="label" numeric style={styles.rate}>
                    {t("pay.rate_pct", { rate: row.ratePct })}
                  </Txt>
                  <Txt role="body" tone="secondary" numeric style={styles.hours}>
                    {t("pay.hours", { hours: row.hours })}
                  </Txt>
                  <Txt role="body" numeric style={styles.amount}>
                    {money(row.amount)}
                  </Txt>
                </View>
              ))
            )}
          </View>
          <Txt role="meta" tone="secondary">
            {t("pay.gross_note")}
          </Txt>
        </View>
      ) : (
        <View style={styles.stack}>
          <View style={[styles.table, { borderColor: colors.border.subtle }]}>
            <HoursRow label={t("pay.hours_scheduled")} minutes={scheduled.data} testID="hours-scheduled" />
            <HoursRow label={t("pay.hours_worked")} minutes={Math.round(Number(pay.data.hours) * 60)} testID="hours-worked" />
          </View>
          <Txt role="meta" tone="secondary">
            {t("pay.hours_note")}
          </Txt>
          {previewsEnabled ? (
            <EmptyState testID="preview-approved-hours" icon="check" badge={t("preview.badge")} title={t("preview.approved_hours_title")} body={t("preview.approved_hours_body")} feature={t("preview.comes_with", { feature: "011" })} />
          ) : null}
        </View>
      )}
    </Screen>
  );

  function changeText(change: string | null): string {
    if (change === null) return "";
    const up = !change.startsWith("-");
    const abs = up ? change : change.slice(1);
    return up ? t("pay.change_up", { amount: money(abs) }) : t("pay.change_down", { amount: money(abs) });
  }
}

function HoursRow({ label, minutes, testID }: { label: string; minutes: number | null; testID: string }) {
  const { colors } = useTheme();
  const parts = minutes === null ? null : durationParts(minutes);
  return (
    <View testID={testID} style={[styles.tableRow, { borderTopColor: colors.border.subtle }]}>
      <Txt role="label" style={styles.rate}>
        {label}
      </Txt>
      <Txt role="body" numeric style={styles.amount}>
        {parts === null ? "…" : `${parts.hours}:${String(parts.minutes).padStart(2, "0")}`}
      </Txt>
    </View>
  );
}

const styles = StyleSheet.create({
  picker: { flexDirection: "row", alignItems: "center", gap: 8 },
  pickerTitle: { flex: 1, minWidth: 0, flexShrink: 1, textAlign: "center" },
  stack: { gap: 16 },
  table: { gap: 0, padding: 14, borderWidth: 1 },
  tableRow: { flexDirection: "row", alignItems: "center", gap: 12, paddingVertical: 10, borderTopWidth: 1 },
  rate: { flex: 1 },
  hours: { width: 90, textAlign: "end" },
  amount: { minWidth: 110, textAlign: "end" },
});
