/**
 * My availability (specs/007-ux-redesign FR-013 to FR-020, FR-054, FR-073; tasks T055 to T057):
 * one week at a time of the open window, Sunday to Saturday, a cell for each shift type and one
 * for the whole day. A tap cycles unmarked, open, close, blocked; a long press offers the choices
 * in a sheet. The status banner, Review and Submit, Undo, Copy last week, absences as bands and the
 * holidays follow the spec; the test ids are in contracts/ui-routes.md ("My work test ids").
 */
import * as Haptics from "expo-haptics";
import { useLocalSearchParams } from "expo-router";
import { useCallback, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Platform, Pressable, StyleSheet, View } from "react-native";

import { ApiError, call, codeOf, useErrorText } from "../../../api/errors";
import { useSession } from "../../../api/SessionContext";
import { useOnline } from "../../../connectivity";
import { Button, EmptyState, InlineBanner, PageTitle, Sheet, SkeletonRows, StatusChip, Txt, focusRing, showToast, type ChipStatus, type PressState } from "../../../design/components";
import { Screen } from "../../../design/components/Screen";
import { useKeyboardMode } from "../../../design/focus";
import { Icon } from "../../../design/icons/Icon";
import { useTheme } from "../../../design/theme/ThemeProvider";
import { component } from "../../../design/theme/theme.generated";
import { webProps } from "../../../design/web";
import { DEFAULT_TIME_ZONE, addDays, dateOf, endOfWeek, formatNumericDate, formatNumericRange, formatTime, holidaysBetween, startOfWeek, todayIn, weekdayNames } from "../../../locale";
import { useLoad } from "../../useLoad";
import type { PendingChange } from "./draftQueue";
import {
  ALL_DAY,
  CYCLE,
  absencesOn,
  cellEntry,
  copyPlan,
  daysOfWeek,
  editable,
  initialWeek,
  nextState,
  pickWindow,
  planCellChange,
  reviewRows,
  statusAfterChange,
  targetKey,
  weeksOf,
  type CellKey,
  type CellState,
  type Change,
  type Entry,
  type Mark,
  type PlannedCall,
  type Submission,
} from "./model";
import { usePendingQueue } from "./usePendingQueue";

const STATE_CHIP: Record<CellState, ChipStatus> = { unmarked: "neutral", open: "success", close: "warning", blocked: "danger" };
const STATUS_CHIP: Record<Submission["status"], ChipStatus> = { not_submitted: "warning", draft: "info", submitted: "success", locked: "neutral" };

const keyOf = (key: CellKey) => `${key.date}|${key.shiftType}`;

function lightTap(): void {
  if (Platform.OS !== "web") void Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Light).catch(() => undefined);
}

function successTap(): void {
  if (Platform.OS !== "web") void Haptics.notificationAsync(Haptics.NotificationFeedbackType.Success).catch(() => undefined);
}

export function AvailabilityScreen() {
  const { t, i18n } = useTranslation();
  const session = useSession();
  const errorText = useErrorText();
  const { colors, isDesktop } = useTheme();
  const online = useOnline();
  const { window: askedWindow } = useLocalSearchParams<{ window?: string }>();
  const userId = session.user?.id ?? "";
  const today = todayIn(DEFAULT_TIME_ZONE);

  const setup = useLoad(async () => {
    const [windows, types] = await Promise.all([
      call(session.api.GET("/api/v1/availability-windows", { params: { query: { from: addDays(today, -60) } } })),
      call(session.api.GET("/api/v1/shift-types")),
    ]);
    return { windows: windows.items, types: types.items.filter((type) => type.is_active) };
  });
  const window = useMemo(() => (setup.data ? pickWindow(setup.data.windows, askedWindow) : null), [setup.data, askedWindow]);
  const shiftTypes = setup.data?.types ?? [];

  const range = window ? { from: addDays(startOfWeek(window.starts_on), -7), to: endOfWeek(window.ends_on) } : null;
  const entries = useLoad(
    async () => (range ? (await call(session.api.GET("/api/v1/employees/{id}/constraints", { params: { path: { id: userId }, query: range } }))).items : []),
    [range?.from, range?.to, userId],
  );
  const submission = useLoad(
    async () => (window ? call(session.api.GET("/me/availability-submissions", { params: { query: { window_id: window.id } } })) : null),
    [window?.id],
  );

  const [week, setWeek] = useState<string | null>(null);
  const shownWeek = week ?? (window ? initialWeek(window, today) : null);
  const weeks = window ? weeksOf(window) : [];
  const [overrides, setOverrides] = useState<Map<string, CellState>>(() => new Map());
  const [status, setStatus] = useState<Submission["status"] | null>(null);
  const [undo, setUndo] = useState<Change[]>([]);
  const [chooser, setChooser] = useState<CellKey | null>(null);
  const [reviewOpen, setReviewOpen] = useState(false);
  const [submitting, setSubmitting] = useState(false);

  const shownStatus: Submission["status"] | null = status ?? submission.data?.status ?? null;
  const canEdit = editable(submission.data ? { status: shownStatus ?? submission.data.status } : null, window);
  const serverEntries: Entry[] = useMemo(() => entries.data ?? [], [entries.data]);

  /** Run a plan's calls in order. */
  const runPlan = useCallback(
    async (calls: readonly PlannedCall[]): Promise<void> => {
      for (const step of calls) {
        if (step.call === "create") {
          await call(
            session.api.POST("/api/v1/employees/{id}/constraints", {
              params: { path: { id: userId } },
              body: { kind: step.kind, starts_on: step.starts_on, ends_on: step.ends_on, shift_type_id: step.shift_type_id, weekday: step.weekday },
            }),
          );
        } else if (step.call === "update") {
          const body: { kind?: Mark; starts_on?: string; ends_on?: string } = {};
          if (step.kind) body.kind = step.kind;
          if (step.starts_on) body.starts_on = step.starts_on;
          if (step.ends_on) body.ends_on = step.ends_on;
          await call(session.api.PATCH("/api/v1/constraints/{id}", { params: { path: { id: step.id } }, body }));
        } else {
          await call(session.api.DELETE("/api/v1/constraints/{id}", { params: { path: { id: step.id } } }));
        }
      }
    },
    [session.api, userId],
  );

  const reload = useCallback(async (): Promise<void> => {
    await Promise.all([entries.reload(), submission.reload()]);
    setStatus(null);
  }, [entries, submission]);

  // A saved change is sent against the server's entries of that day, read afresh, so it lands right
  // however the week changed meanwhile (R49, R50).
  const sendPending = useCallback(
    async (change: PendingChange): Promise<void> => {
      if (!window) return;
      try {
        const fresh = await call(session.api.GET("/api/v1/employees/{id}/constraints", { params: { path: { id: userId }, query: { from: change.key.date, to: change.key.date } } }));
        await runPlan(planCellChange(fresh.items, change.key, change.state, window));
      } catch (failure) {
        if (failure instanceof ApiError && failure.status === 0) throw Object.assign(new Error("unreachable"), { retry: true });
        throw failure;
      }
    },
    [window, session.api, userId, runPlan],
  );
  const dropOverride = useCallback((key: CellKey) => {
    setOverrides((map) => {
      const next = new Map(map);
      next.delete(keyOf(key));
      return next;
    });
  }, []);
  const queue = usePendingQueue(
    userId,
    window?.id ?? null,
    sendPending,
    (change, failure) => {
      dropOverride(change.key);
      showToast(errorText(codeOf(failure)), { tone: "warning" });
    },
    () => void reload(),
  );
  const pending = queue.pending;

  // The device draft shows as made while it waits (FR-019).
  const pendingOverrides = useMemo(() => new Map(pending.map((item) => [keyOf(item.key), item.state])), [pending]);
  const stateOf = useCallback(
    (key: CellKey): CellState =>
      overrides.get(keyOf(key)) ?? pendingOverrides.get(keyOf(key)) ?? (cellEntry(serverEntries, key)?.kind as CellState | undefined) ?? "unmarked",
    [overrides, pendingOverrides, serverEntries],
  );

  /**
   * Put a cell into `to`, through the API, and reload unless the caller batches (R57). The change is
   * recorded for Undo on the cell it really lands on (R55). Out of reach, it joins the device draft.
   */
  const applyCell = useCallback(
    async (key: CellKey, to: CellState, record: boolean, batch: boolean): Promise<void> => {
      if (!window) return;
      const target = targetKey(serverEntries, key);
      const before = stateOf(target);
      if (before === to) return;
      setOverrides((current) => new Map(current).set(keyOf(target), to).set(keyOf(key), to));
      if (record) setUndo((stack) => [...stack, { key: target, before, after: to }]);
      if (shownStatus) setStatus(statusAfterChange(shownStatus));
      lightTap();
      try {
        await runPlan(planCellChange(serverEntries, key, to, window));
        if (!batch) {
          await reload();
          dropOverride(target);
          dropOverride(key);
        }
      } catch (failure) {
        if (failure instanceof ApiError && failure.status === 0) {
          // No server: keep the change on the device and send it when the connection is back.
          queue.enqueue({ key: target, state: to });
          dropOverride(target);
          dropOverride(key);
          return;
        }
        dropOverride(target);
        dropOverride(key);
        if (record) setUndo((stack) => stack.slice(0, -1));
        setStatus(null);
        showToast(errorText(codeOf(failure)), { tone: "warning" });
      }
    },
    [window, serverEntries, stateOf, shownStatus, runPlan, reload, dropOverride, queue, errorText],
  );
  const setCell = useCallback((key: CellKey, to: CellState, record: boolean) => applyCell(key, to, record, false), [applyCell]);

  const undoLast = async (): Promise<void> => {
    const last = undo[undo.length - 1];
    if (!last) return;
    setUndo((stack) => stack.slice(0, -1));
    await setCell(last.key, last.before, false);
  };

  // Copy last week writes every cell, then reloads once (R57).
  const copyLastWeek = async (): Promise<void> => {
    if (!shownWeek) return;
    const plan = copyPlan(serverEntries, shownWeek, shiftTypes.map((type) => type.id));
    if (plan.length === 0) {
      showToast(t("availability.copy_nothing"));
      return;
    }
    for (const step of plan) await applyCell(step.key, step.state, true, true);
    await reload();
    setOverrides(new Map());
  };

  const submit = async (): Promise<void> => {
    if (!window) return;
    setSubmitting(true);
    try {
      await call(session.api.POST("/me/availability-submissions", { body: { window_id: window.id } }));
      successTap();
      setReviewOpen(false);
      setUndo([]);
      await reload();
      showToast(t("availability.submitted_toast"));
    } catch (failure) {
      showToast(errorText(codeOf(failure)), { tone: "warning" });
    } finally {
      setSubmitting(false);
    }
  };

  if (setup.error) {
    return (
      <Screen testID="screen-availability">
        <InlineBanner tone="warning" title={errorText(setup.error)} action={{ label: t("common.retry"), onPress: () => void setup.reload() }} />
      </Screen>
    );
  }
  if (setup.data === null) {
    return (
      <Screen testID="screen-availability">
        <SkeletonRows rows={6} testID="availability-loading" />
      </Screen>
    );
  }
  if (window === null || shownWeek === null) {
    return (
      <Screen testID="screen-availability">
        <PageTitle eyebrow={[t("app.name"), t("nav.group.my_work")]} title={t("availability.title")} />
        <EmptyState icon="calendar" title={t("availability.no_window_title")} body={t("availability.no_window_body")} />
      </Screen>
    );
  }

  const days = daysOfWeek(shownWeek);
  const dayNames = weekdayNames(i18n.language, isDesktop ? "long" : "short");
  const holidays = holidaysBetween(days[0]!, days[6]!, i18n.language);
  const weekIndex = weeks.indexOf(shownWeek);
  const columns = [{ id: ALL_DAY, label: t("availability.all_day"), times: null as string | null }, ...shiftTypes.map((type) => ({ id: type.id, label: type.name, times: `${type.starts_at_local.slice(0, 5)}–${type.ends_at_local.slice(0, 5)}` }))];
  const rows = reviewRows(serverEntries, window, shiftTypes.map((type) => type.id));
  const nameOf = (shiftType: string) => (shiftType === ALL_DAY ? t("availability.all_day") : (shiftTypes.find((type) => type.id === shiftType)?.name ?? shiftType));
  const deadlineText = `${formatNumericDate(dateOf(window.deadline, DEFAULT_TIME_ZONE))} ${formatTime(window.deadline, DEFAULT_TIME_ZONE)}`;
  const bannerData: Record<string, string> = { status: shownStatus ?? "" };
  if (shownStatus === "locked" && submission.data) bannerData.countedUnavailable = String(submission.data.counted_unavailable);

  return (
    <Screen testID="screen-availability">
      <PageTitle eyebrow={[t("app.name"), t("nav.group.my_work")]} title={t("availability.title")}>
        <Txt role="meta" tone="secondary" numeric>
          {t("availability.window_dates", { first: formatNumericDate(window.starts_on), last: formatNumericDate(window.ends_on) })}
        </Txt>
      </PageTitle>

      {shownStatus && submission.data ? (
        <View
          testID="availability-banner"
          role="status"
          {...webProps({ dataSet: bannerData })}
          style={[styles.banner, { borderColor: colors.border.subtle, backgroundColor: colors.bg.surface }]}
        >
          <View style={styles.bannerHead}>
            <StatusChip status={STATUS_CHIP[shownStatus]} label={t(`availability.status.${shownStatus}`)} />
            <Txt role="meta" tone="secondary" numeric>
              {shownStatus === "locked" ? t("availability.locked_since", { deadline: deadlineText }) : t("availability.window_deadline", { deadline: deadlineText })}
            </Txt>
          </View>
          {shownStatus === "submitted" && submission.data.submitted_at ? (
            <Txt role="body" tone="secondary" numeric>
              {t("availability.submitted_at", { time: `${formatNumericDate(dateOf(submission.data.submitted_at, DEFAULT_TIME_ZONE))} ${formatTime(submission.data.submitted_at, DEFAULT_TIME_ZONE)}` })}
            </Txt>
          ) : null}
          {shownStatus === "draft" ? (
            <Txt role="body" tone="secondary">
              {t("availability.draft_body")}
            </Txt>
          ) : null}
          {shownStatus === "not_submitted" ? (
            <InlineBanner tone="warning" testID="availability-not-submitted-warning" title={t("availability.not_submitted_warning")} />
          ) : null}
          {shownStatus === "locked" ? (
            <Txt role="body" tone={submission.data.counted_unavailable ? "danger" : "secondary"}>
              {submission.data.counted_unavailable ? t("availability.counted_unavailable") : t("availability.locked_kept")}
            </Txt>
          ) : null}
          {canEdit ? (
            <View style={styles.actions}>
              <Button variant="primary" label={t("availability.review")} icon="check" onPress={() => setReviewOpen(true)} testID="action-review" />
              <Button variant="secondary" label={t("availability.copy_last_week")} icon="undo" onPress={() => void copyLastWeek()} testID="action-copy-last-week" />
            </View>
          ) : null}
        </View>
      ) : null}

      {undo.length > 0 && canEdit ? (
        <InlineBanner
          tone="info"
          testID="availability-undo-notice"
          title={t("availability.changed", { cell: `${formatNumericDate(undo[undo.length - 1]!.key.date)} · ${nameOf(undo[undo.length - 1]!.key.shiftType)}`, state: t(`availability.state.${undo[undo.length - 1]!.after}`) })}
          action={{ label: t("common.undo"), onPress: () => void undoLast(), testID: "action-undo" }}
        />
      ) : null}
      {!online && pending.length > 0 ? <InlineBanner tone="warning" testID="availability-pending" title={t("availability.pending", { count: pending.length })} /> : null}

      <View style={styles.weekBar}>
        <Button variant="quiet" icon="chevron-back" label={t("availability.prev_week")} onPress={() => setWeek(weeks[weekIndex - 1] ?? shownWeek)} disabled={weekIndex <= 0} testID="availability-prev-week" />
        <Txt role="section" heading={2} numeric style={styles.weekTitle}>
          {t("availability.week_of", { range: formatNumericRange(days[0]!, days[6]!) })}
        </Txt>
        <Button variant="quiet" icon="chevron-next" label={t("availability.next_week")} iconPosition="trailing" onPress={() => setWeek(weeks[weekIndex + 1] ?? shownWeek)} disabled={weekIndex >= weeks.length - 1} testID="availability-next-week" />
      </View>

      <View accessibilityLabel={t("availability.grid_label")} style={[styles.grid, { borderColor: colors.border.subtle }]}>
        <View style={[styles.row, styles.headRow, { borderBottomColor: colors.border.strong }]}>
          <View style={styles.dayCol}>
            <Txt role="eyebrow" tone="secondary">
              {t("availability.day")}
            </Txt>
          </View>
          {columns.map((column) => (
            <View key={column.id} style={styles.cellCol}>
              <Txt role="eyebrow" tone="secondary" numberOfLines={1}>
                {column.label}
              </Txt>
              {column.times ? (
                <Txt role="meta" tone="secondary" numeric numberOfLines={1}>
                  {column.times}
                </Txt>
              ) : null}
            </View>
          ))}
        </View>
        {days.map((date, index) => {
          const inWindow = date >= window.starts_on && date <= window.ends_on;
          const holiday = holidays.get(date);
          const absences = absencesOn(serverEntries, date);
          const rest = index === 6;
          return (
            <View key={date} style={[styles.row, { borderBottomColor: colors.border.subtle }, rest ? { backgroundColor: colors.bg.surface } : null]}>
              <View style={styles.dayCol}>
                <Txt role="label" numberOfLines={1}>
                  {dayNames[index]}
                </Txt>
                <Txt role="meta" tone="secondary" numeric>
                  {formatNumericDate(date).slice(0, 5)}
                </Txt>
                {holiday ? (
                  <Txt role="meta" tone="accent" numberOfLines={1} testID="availability-holiday">
                    {holiday.name}
                  </Txt>
                ) : null}
              </View>
              {absences.length > 0 ? (
                <View testID="absence-band" {...webProps({ dataSet: { date } })} style={[styles.band, { backgroundColor: colors.bg.selected, borderColor: colors.border.strong }]}>
                  <Icon name="calendar" size={16} color={colors.text.secondary} />
                  <Txt role="label">{t("availability.absence_band", { kind: t(`availability.absence.${absences[0]!.absence_kind ?? "leave"}`) })}</Txt>
                </View>
              ) : (
                columns.map((column) => {
                  const key = { date, shiftType: column.id };
                  const state = stateOf(key);
                  return (
                    <Cell
                      key={column.id}
                      cellKey={key}
                      state={state}
                      label={`${dayNames[index]} ${formatNumericDate(date)}, ${column.label}: ${t(`availability.state.${state}`)}`}
                      disabled={!canEdit || !inWindow}
                      onPress={() => void setCell(key, nextState(state), true)}
                      onLongPress={() => setChooser(key)}
                    />
                  );
                })
              )}
            </View>
          );
        })}
      </View>

      <View testID="availability-rules" style={[styles.rules, { borderColor: colors.border.subtle }]}>
        <Txt role="eyebrow" tone="secondary">
          {t("availability.rules_title")}
        </Txt>
        <Txt role="body" tone="secondary">
          {t("availability.rules_body")}
        </Txt>
        <View style={styles.legend}>
          {CYCLE.map((state) => (
            <StatusChip key={state} status={STATE_CHIP[state]} label={t(`availability.state.${state}`)} />
          ))}
        </View>
      </View>

      <Sheet open={chooser !== null} onClose={() => setChooser(null)} title={chooser ? `${formatNumericDate(chooser.date)} · ${nameOf(chooser.shiftType)}` : ""} testID="availability-choices">
        <View style={styles.choices}>
          {CYCLE.map((state) => (
            <Button
              key={state}
              variant={chooser && stateOf(chooser) === state ? "primary" : "secondary"}
              label={t(`availability.state.${state}`)}
              onPress={() => {
                if (chooser) void setCell(chooser, state, true);
                setChooser(null);
              }}
              testID={`choice-${state}`}
              block
            />
          ))}
        </View>
      </Sheet>

      <Sheet
        open={reviewOpen}
        onClose={() => setReviewOpen(false)}
        title={t("availability.review_title")}
        testID="availability-review"
        footer={
          <View style={styles.actions}>
            <Button variant="primary" label={t("availability.submit")} onPress={() => void submit()} busy={submitting} testID="action-submit" />
            <Button variant="quiet" label={t("common.cancel")} onPress={() => setReviewOpen(false)} />
          </View>
        }
      >
        <Txt role="body" tone="secondary">
          {t("availability.review_intro", { first: formatNumericDate(window.starts_on), last: formatNumericDate(window.ends_on) })}
        </Txt>
        {rows.length === 0 ? (
          <Txt role="body" tone="secondary" testID="review-empty">
            {t("availability.review_empty")}
          </Txt>
        ) : (
          rows.map((row) => (
            <View key={row.date} testID="review-row" style={[styles.reviewRow, { borderBottomColor: colors.border.subtle }]}>
              <Txt role="label" numeric style={styles.reviewDate}>
                {`${dayNames[new Date(`${row.date}T12:00:00Z`).getUTCDay()]} ${formatNumericDate(row.date)}`}
              </Txt>
              <View style={styles.reviewMarks}>
                {row.marks.map((mark) => (
                  <StatusChip key={mark.shiftType} status={STATE_CHIP[mark.state]} label={`${nameOf(mark.shiftType)}: ${t(`availability.state.${mark.state}`)}`} />
                ))}
              </View>
            </View>
          ))
        )}
      </Sheet>
    </Screen>
  );
}

function Cell({ cellKey, state, label, disabled, onPress, onLongPress }: { cellKey: CellKey; state: CellState; label: string; disabled: boolean; onPress: () => void; onLongPress: () => void }) {
  const { colors } = useTheme();
  const keyboard = useKeyboardMode();
  const { t } = useTranslation();
  const marked = state !== "unmarked";
  const tone = marked ? colors.status[STATE_CHIP[state] as "success" | "warning" | "danger"] : colors.text.secondary;
  return (
    <Pressable
      role="button"
      testID="availability-cell"
      accessibilityLabel={label}
      accessibilityState={{ disabled }}
      aria-disabled={disabled}
      disabled={disabled}
      onPress={onPress}
      onLongPress={onLongPress}
      {...webProps({ dataSet: { date: cellKey.date, shiftType: cellKey.shiftType, state } })}
      style={(pressState) => [
        styles.cell,
        { borderColor: marked ? tone : colors.border.subtle, backgroundColor: marked ? colors.bg.selected : colors.bg.canvas },
        disabled ? styles.cellDisabled : null,
        (pressState as PressState).hovered || pressState.pressed ? { borderColor: colors.border.strong } : null,
        keyboard && (pressState as PressState).focused ? focusRing(colors.focus.ring) : null,
      ]}
    >
      <Txt role="eyebrow" style={{ color: tone }} numberOfLines={1}>
        {t(`availability.state_short.${state}`)}
      </Txt>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  banner: { gap: 10, padding: 16, borderWidth: 1 },
  bannerHead: { flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 12 },
  actions: { flexDirection: "row", flexWrap: "wrap", gap: 12 },
  weekBar: { flexDirection: "row", alignItems: "center", gap: 8 },
  weekTitle: { flex: 1, textAlign: "center" },
  grid: { borderWidth: 1 },
  row: { flexDirection: "row", alignItems: "stretch", borderBottomWidth: 1, minHeight: component.target.phone + 8 },
  headRow: { minHeight: 44 },
  dayCol: { width: 92, padding: 6, justifyContent: "center", gap: 1 },
  cellCol: { flex: 1, padding: 6, justifyContent: "center", alignItems: "center" },
  cell: { flex: 1, minWidth: component.target.phone, minHeight: component.target.phone, margin: 3, borderWidth: 1, alignItems: "center", justifyContent: "center" },
  cellDisabled: { opacity: 0.6 },
  band: { flex: 1, margin: 3, borderWidth: 1, flexDirection: "row", alignItems: "center", gap: 8, paddingHorizontal: 10 },
  rules: { gap: 8, padding: 14, borderWidth: 1 },
  legend: { flexDirection: "row", flexWrap: "wrap", gap: 8 },
  choices: { gap: 10 },
  reviewRow: { gap: 6, paddingVertical: 10, borderBottomWidth: 1 },
  reviewDate: {},
  reviewMarks: { flexDirection: "row", flexWrap: "wrap", gap: 6 },
});
