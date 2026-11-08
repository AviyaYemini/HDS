/**
 * My shifts (specs/007-ux-redesign FR-021, FR-022, FR-019; task T058): Upcoming by week with the
 * next shift leading as a card, Past by pay period with the hours and the estimated pay, published
 * shifts only, readable offline with the time the list was last loaded. Test ids:
 * contracts/ui-routes.md ("My work test ids").
 */
import { useLocalSearchParams } from "expo-router";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { ApiError, call, useErrorText } from "../../../api/errors";
import { useSession } from "../../../api/SessionContext";
import { previewsEnabled } from "../../../config/previews";
import { useOnline } from "../../../connectivity";
import { Button, EmptyState, FeaturePanel, InlineBanner, LinkPressable, PageTitle, SegmentedControl, SkeletonRows, StatusChip, Txt, focusRing } from "../../../design/components";
import { Screen } from "../../../design/components/Screen";
import { useKeyboardMode } from "../../../design/focus";
import { Icon } from "../../../design/icons/Icon";
import { useTheme } from "../../../design/theme/ThemeProvider";
import { webProps } from "../../../design/web";
import { DEFAULT_TIME_ZONE, addDays, durationParts, formatLongDate, formatMoney, formatMonthYear, formatNumericDate, formatNumericRange, formatTime, formatTimeRange, todayIn, weekdayNames } from "../../../locale";
import { useLoad } from "../../useLoad";
import { byPeriod, byWeek, countdown, endsNextDay, nextShift, past, pastRange, shiftDate, upcoming, upcomingRange, type Shift } from "./model";
import { loadCached, saveCached, type CachedShifts } from "./shiftsCache";

type Mode = "upcoming" | "past";

export function MyShiftsScreen() {
  const { t, i18n } = useTranslation();
  const session = useSession();
  const errorText = useErrorText();
  const online = useOnline();
  const { view: askedView } = useLocalSearchParams<{ view?: string }>();
  const [view, setView] = useState<Mode>(askedView === "past" ? "past" : "upcoming");
  const [periodsBack, setPeriodsBack] = useState(3);
  const userId = session.user?.id ?? "";
  const today = todayIn(DEFAULT_TIME_ZONE);
  const now = new Date();

  const range: { from: string; to?: string } = view === "upcoming" ? upcomingRange(today) : pastRange(today, periodsBack);
  const shifts = useLoad(async () => {
    try {
      const answer = await call(session.api.GET("/me/shifts", { params: { query: range } }));
      void saveCached(userId, view, answer.items);
      return { items: answer.items, updatedAt: null as string | null };
    } catch (failure) {
      // Out of reach: the last list the device loaded, with when (FR-019).
      if (failure instanceof ApiError && failure.status === 0) {
        const cached: CachedShifts | null = await loadCached(userId, view);
        if (cached) return { items: cached.items, updatedAt: cached.updatedAt };
      }
      throw failure;
    }
  }, [view, range.from, range.to, userId]);

  // Pay per past period, for the group headers (FR-021).
  const [pay, setPay] = useState<Record<string, string | null>>({});
  const periods = view === "past" && shifts.data ? byPeriod(past(shifts.data.items, now)).map((group) => group.period) : [];
  const periodsKey = periods.join(",");
  useEffect(() => {
    if (!periodsKey) return;
    let live = true;
    void Promise.all(
      periodsKey.split(",").map(async (period) => {
        try {
          const answer = await call(session.api.GET("/me/pay", { params: { query: { month: period } } }));
          return [period, answer.pay ?? null] as const;
        } catch {
          return [period, null] as const;
        }
      }),
    ).then((entries) => {
      if (live) setPay(Object.fromEntries(entries));
    });
    return () => {
      live = false;
    };
  }, [periodsKey, session.api]);

  const options = [
    { value: "upcoming" as Mode, label: t("shifts.upcoming"), testID: "view-upcoming" },
    { value: "past" as Mode, label: t("shifts.past"), testID: "view-past" },
  ];

  return (
    <Screen testID="screen-my-shifts">
      <PageTitle eyebrow={[t("app.name"), t("nav.group.my_work")]} title={t("shifts.title")}>
        <SegmentedControl options={options} value={view} onChange={setView} label={t("shifts.view_label")} testID="shifts-view" />
      </PageTitle>
      {shifts.data?.updatedAt || !online ? (
        <InlineBanner tone="info" testID="shifts-updated" title={t("shifts.updated_at", { time: formatTime(shifts.data?.updatedAt ?? now, DEFAULT_TIME_ZONE) })} />
      ) : null}
      {shifts.error ? (
        <InlineBanner tone="warning" title={errorText(shifts.error)} action={{ label: t("common.retry"), onPress: () => void shifts.reload() }} />
      ) : shifts.data === null ? (
        <SkeletonRows rows={4} testID="shifts-loading" />
      ) : view === "upcoming" ? (
        <Upcoming items={upcoming(shifts.data.items, now)} now={now} />
      ) : (
        <Past items={past(shifts.data.items, now)} pay={pay} />
      )}
    </Screen>
  );

  function Upcoming({ items, now }: { items: Shift[]; now: Date }) {
    const next = nextShift(items, now);
    const groups = byWeek(items);
    return (
      <View style={styles.stack}>
        {next ? <NextShiftCard shift={next} now={now} /> : <EmptyState icon="calendar" title={t("shifts.none_upcoming_title")} body={t("shifts.none_upcoming_body")} testID="shifts-none-upcoming" />}
        {groups.map((group) => (
          <View key={group.weekStart} testID="week-group" {...webProps({ dataSet: { weekStart: group.weekStart } })} style={styles.group}>
            <Txt role="section" heading={2} numeric>
              {t("shifts.week_of", { range: formatNumericRange(group.weekStart, addDays(group.weekStart, 6)) })}
            </Txt>
            {group.items.map((shift) => (
              <ShiftRow key={shift.id} shift={shift} />
            ))}
          </View>
        ))}
        {previewsEnabled ? (
          <EmptyState testID="preview-report-hours" icon="clock" badge={t("preview.badge")} title={t("preview.report_hours_title")} body={t("preview.report_hours_body")} feature={t("preview.comes_with", { feature: "011" })} />
        ) : null}
      </View>
    );
  }

  function Past({ items, pay }: { items: Shift[]; pay: Record<string, string | null> }) {
    const groups = byPeriod(items);
    const earlier = <Button variant="secondary" label={t("shifts.show_earlier")} icon="chevron-down" onPress={() => setPeriodsBack((count) => count + 3)} testID="action-show-earlier" />;
    if (groups.length === 0) {
      return (
        <View style={styles.stack}>
          <EmptyState icon="clock" title={t("shifts.none_past_title")} body={t("shifts.none_past_body", { months: periodsBack })} testID="shifts-none-past" />
          {earlier}
        </View>
      );
    }
    return (
      <View style={styles.stack}>
        {groups.map((group) => {
          const { hours, minutes } = durationParts(group.minutes);
          const estimate = pay[group.period];
          return (
            <View key={group.period} testID="period-group" {...webProps({ dataSet: { period: group.period } })} style={styles.group}>
              <View style={styles.groupHead}>
                <Txt role="section" heading={2}>
                  {formatMonthYear(`${group.period}-01`, i18n.language)}
                </Txt>
                <Txt role="meta" tone="secondary" numeric>
                  {t("shifts.period_totals", { count: group.items.length, hours: minutes > 0 ? `${hours}:${String(minutes).padStart(2, "0")}` : String(hours) })}
                  {estimate ? ` · ${t("shifts.period_pay", { pay: formatMoney(estimate, i18n.language) })}` : ""}
                </Txt>
              </View>
              {group.items.map((shift) => (
                <ShiftRow key={shift.id} shift={shift} />
              ))}
            </View>
          );
        })}
        {earlier}
      </View>
    );
  }
}

function NextShiftCard({ shift, now }: { shift: Shift; now: Date }) {
  const { t, i18n } = useTranslation();
  const left = countdown(shift, now);
  const date = shiftDate(shift);
  return (
    <FeaturePanel testID="next-shift-card">
      <Txt role="eyebrow" tone="secondary">
        {t("shifts.next_shift")}
      </Txt>
      <Txt role="display" heading={2}>
        {formatLongDate(date, i18n.language)}
      </Txt>
      <Txt role="body" numeric>
        {formatTimeRange(shift.starts_at, shift.ends_at, shift.timezone)}
      </Txt>
      <Txt role="body">{`${shift.site.name} · ${shift.project.name} · ${shift.shift_type}`}</Txt>
      <Txt role="meta" tone="secondary" numeric testID="next-shift-countdown">
        {left === "started" ? t("shifts.started") : t("shifts.in_days_hours", { days: left.days, hours: left.hours })}
      </Txt>
    </FeaturePanel>
  );
}

function ShiftRow({ shift }: { shift: Shift }) {
  const { t, i18n } = useTranslation();
  const { colors } = useTheme();
  const keyboard = useKeyboardMode();
  const date = shiftDate(shift);
  const dayName = weekdayNames(i18n.language, "short")[new Date(`${date}T12:00:00Z`).getUTCDay()];
  return (
    <LinkPressable
      href={`/my/shifts/${shift.id}`}
      role="link"
      testID="shift-row"
      {...webProps({ dataSet: { shiftId: shift.id } })}
      style={(state) => [
        styles.row,
        { borderColor: colors.border.subtle, backgroundColor: colors.bg.surface },
        state.hovered || state.pressed ? { borderColor: colors.border.strong } : null,
        keyboard && state.focused ? focusRing(colors.focus.ring) : null,
      ]}
    >
      <View style={styles.rowDate}>
        <Txt role="label">{dayName}</Txt>
        <Txt role="meta" tone="secondary" numeric>
          {formatNumericDate(date)}
        </Txt>
      </View>
      <View style={styles.rowBody}>
        <Txt role="body" numeric testID="shift-time">
          {formatTimeRange(shift.starts_at, shift.ends_at, shift.timezone)}
          {endsNextDay(shift) ? "" : ""}
        </Txt>
        <Txt role="meta" tone="secondary" numberOfLines={1}>
          {`${shift.project.name} · ${shift.site.name}`}
        </Txt>
      </View>
      <StatusChip status="neutral" label={shift.shift_type} />
      <Icon name="chevron-next" size={18} color={colors.text.secondary} label={t("common.open")} />
    </LinkPressable>
  );
}

const styles = StyleSheet.create({
  stack: { gap: 20 },
  group: { gap: 8 },
  groupHead: { gap: 2 },
  row: { flexDirection: "row", alignItems: "center", gap: 12, padding: 12, borderWidth: 1, minHeight: 56 },
  rowDate: { width: 72, gap: 2 },
  rowBody: { flex: 1, gap: 2 },
});
