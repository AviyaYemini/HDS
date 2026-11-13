/**
 * Who has submitted availability for a window (specs/007-ux-redesign FR-040, T074): the window, its
 * deadline and the days left, how many submitted of how many with a progress bar, and every staff
 * member with their status (not submitted first, searchable), whether a missed deadline counts them
 * as unavailable, and an Admin's hand change beside the submission (T101).
 */
import { useLocalSearchParams } from "expo-router";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { call, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { EmptyState, InlineBanner, PageTitle, StatusChip, TextField, Txt } from "../../design/components";
import { DashboardSkeleton, KeyFigures, type KeyFigure } from "../deck/parts";
import { useEntrance } from "../deck/useEntrance";
import { daysLeft, orderedRows, submittedOf } from "./submissionsModel";
import { Screen } from "../../design/components/Screen";
import { useTheme } from "../../design/theme/ThemeProvider";
import { webProps } from "../../design/web";
import { addDays, formatNumericDate, DEFAULT_TIME_ZONE, dateOf, isolateLtr, todayIn } from "../../locale";
import { nearestOpenWindow } from "../availability";
import { useLoad } from "../useLoad";

// Not submitted reads as neutral, not a warning: it is a state, not a fault (the owner, 2026-11-13).
const TONE = { not_submitted: "neutral", draft: "info", submitted: "success", locked: "success" } as const;

export function SubmissionsScreen() {
  const { t } = useTranslation();
  const { window } = useLocalSearchParams<{ window?: string }>();
  const session = useSession();
  const errorText = useErrorText();
  const { colors } = useTheme();
  const today = todayIn(DEFAULT_TIME_ZONE);
  const [query, setQuery] = useState("");
  // Without ?window=, the window whose deadline comes next, as the home's card follows (R117).
  const rows = useLoad(async () => {
    const windows = (await call(session.api.GET("/api/v1/availability-windows", { params: { query: { from: addDays(today, -60), to: addDays(today, 60) } } }))).items;
    const chosen = window ? (windows.find((w) => w.id === window) ?? null) : nearestOpenWindow(windows, Date.now());
    const id = window ?? chosen?.id ?? null;
    if (id === null) return { items: null, window: null };
    return { items: (await call(session.api.GET("/api/v1/availability-windows/{id}/submissions", { params: { path: { id } } }))).items, window: chosen };
  }, [window]);
  const progress = useEntrance(rows.data?.items ? `${rows.data.window?.id ?? ""}` : null);

  const items = rows.data?.items ?? [];
  const count = submittedOf(items);
  const open = rows.data?.window ?? null;
  const deadline = open ? dateOf(open.deadline, DEFAULT_TIME_ZONE) : null;
  const left = deadline ? daysLeft(today, deadline) : null;
  const short = (date: string) => `${Number(date.slice(8))}.${Number(date.slice(5, 7))}`;
  const leftText = left === null ? "" : left > 0 ? t("subs.days_left", { count: left }) : left === 0 ? t("subs.deadline_today") : t("subs.deadline_passed");
  const figures: KeyFigure[] = [
    { key: "submitted", label: t("subs.figure.submitted"), value: `${count.submitted}/${count.total}`, detail: count.submitted === 0 ? t("subs.none_yet", { total: count.total, left: leftText }) : undefined },
    { key: "share", label: t("subs.figure.share"), value: `${count.pct}%`, count: { to: count.pct, format: (n: number) => `${Math.round(n)}%` } },
    { key: "drafts", label: t("subs.figure.drafts"), value: String(count.drafts), detail: t("subs.figure.drafts_detail") },
  ];
  const shown = orderedRows(items, query);

  return (
    <Screen testID="screen-submissions">
      <PageTitle eyebrow={[t("app.name"), t("nav.group.manage")]} title={t("nav.submissions")} />
      {rows.error ? (
        <InlineBanner tone="warning" title={errorText(rows.error)} action={{ label: t("common.retry"), onPress: () => void rows.reload() }} />
      ) : rows.data === null ? (
        <DashboardSkeleton figures={3} charts={0} rows={8} />
      ) : rows.data.items === null ? (
        <EmptyState icon="calendar" title={t("submissions.no_window")} body={t("submissions.no_window_body")} />
      ) : (
        <View style={styles.body}>
          {/* The open window: its dates, its deadline and the days left (the owner, 2026-11-13). */}
          {open && deadline ? (
            <Txt role="section" numeric testID="subs-window">
              {t("subs.window", { range: isolateLtr(`${short(open.starts_on)}–${short(open.ends_on)}`), deadline: isolateLtr(short(deadline)), left: leftText })}
            </Txt>
          ) : null}
          <KeyFigures figures={figures} progress={progress} />
          <View
            role="progressbar"
            aria-valuemin={0}
            aria-valuemax={100}
            aria-valuenow={Math.round(count.pct)}
            style={[styles.track, { backgroundColor: colors.bg.selected }]}
            accessibilityLabel={t("subs.progress", { submitted: count.submitted, total: count.total })}
            testID="subs-progress"
          >
            <View style={[styles.fill, { width: `${count.pct * progress}%`, backgroundColor: colors.text.primary }]} />
          </View>
          <TextField label={t("subs.search")} hideLabel placeholder={t("subs.search")} value={query} onChangeText={setQuery} testID="subs-search" />
          <View style={styles.list}>
            {shown.map((row) => (
              <View key={row.user_id} testID="submission-row" {...webProps({ dataSet: { userId: row.user_id, status: row.status } })} style={[styles.row, { borderColor: colors.border.subtle }]}>
                <Txt role="label" style={styles.name}>
                  {row.full_name}
                </Txt>
                <StatusChip status={TONE[row.status]} label={t(`availability.status.${row.status}`)} />
                {row.counted_unavailable ? (
                  <Txt role="meta" tone="secondary">
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
        </View>
      )}
    </Screen>
  );
}

const styles = StyleSheet.create({
  body: { gap: 20 },
  track: { height: 8 },
  fill: { height: 8 },
  list: { gap: 0 },
  row: { flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 12, paddingVertical: 10, borderBottomWidth: 1 },
  name: { flexGrow: 1, minWidth: 160 },
});
