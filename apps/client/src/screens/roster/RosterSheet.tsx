/**
 * The roster week as the managers' sheet (specs/demo-ceo/plan.md, screen 2; deck slide 7): every
 * project the reader sees as a row, grouped by client, the seven days as columns; names as blocks,
 * empty places dashed, a "temporary" chip on temporary workers, the week switch and "copy last week"
 * (warnings are out of the app's scope, the owner, 2026-11-13). A project's name opens its own week for editing.
 */
import { useRouter } from "expo-router";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Pressable, ScrollView, StyleSheet, View } from "react-native";

import { call, codeOf, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { useCapabilities } from "../../capabilities";
import { Button, EmptyState, InlineBanner, LinkPressable, PageTitle, Sheet, SkeletonRows, Txt, showToast } from "../../design/components";
import { Screen } from "../../design/components/Screen";
import { useTheme } from "../../design/theme/ThemeProvider";
import { webProps } from "../../design/web";
import { DEFAULT_TIME_ZONE, addDays, formatTimeRange, startOfWeek, todayIn } from "../../locale";
import { useLoad } from "../useLoad";
import { allShifts } from "./allShifts";
import { weekDays, type Shift } from "./model";
import { rosterHref } from "./ProjectChooser";
import { buildSheet, isTemporary, rowEmpty, sheetTotals, shortName, withShifts, type ClientGroup, type SheetRow } from "./sheet";

const LABEL_WIDTH = 176;
const DAY_MIN_WIDTH = 124;
/** On a phone the sheet scrolls sideways; narrower columns show more of it at once. */
const PHONE_LABEL_WIDTH = 120;
const PHONE_DAY_MIN_WIDTH = 104;

function useWidths(): { label: { width: number }; day: { minWidth: number }; total: number } {
  const { isDesktop } = useTheme();
  const label = isDesktop ? LABEL_WIDTH : PHONE_LABEL_WIDTH;
  const day = isDesktop ? DAY_MIN_WIDTH : PHONE_DAY_MIN_WIDTH;
  return { label: { width: label }, day: { minWidth: day }, total: label + day * 7 };
}

export function RosterSheet({ weekParam }: { weekParam?: string }) {
  const { t, i18n } = useTranslation();
  const { api } = useSession();
  const router = useRouter();
  const capabilities = useCapabilities();
  const errorText = useErrorText();
  const { colors, isDesktop } = useTheme();
  const widths = useWidths();
  const today = todayIn(DEFAULT_TIME_ZONE);
  const week = startOfWeek(weekParam ?? today);
  const days = weekDays(week);

  const projects = useLoad(async () => (await call(api.GET("/api/v1/projects"))).items, []);
  const clients = useLoad(async () => (await call(api.GET("/api/v1/clients"))).items, []);
  const shifts = useLoad(() => allShifts(api, week, addDays(week, 6)), [week]);

  // Plain values: the React Compiler memoizes them.
  const allGroups = projects.data && clients.data && shifts.data ? buildSheet(projects.data, clients.data, shifts.data, days, i18n.language) : null;
  const totals = allGroups ? sheetTotals(allGroups) : null;
  // Projects without a shift this week stay out of the way unless asked for (most of a real sheet's tasks rest most weeks).
  const [showAll, setShowAll] = useState(false);
  const trimmed = allGroups ? withShifts(allGroups) : null;
  const groups = allGroups && trimmed ? (showAll ? allGroups : trimmed.groups) : null;
  const names = new Map<string, string>();
  for (const s of shifts.data ?? []) for (const a of s.assignments) if (a.full_name) names.set(a.user_id, a.full_name);

  const editable = (projects.data ?? []).filter((p) => p.status === "active" && capabilities.hasOnProject("roster.edit:project", p.id));
  const [copyPlan, setCopyPlan] = useState<{ projects: number; shifts: number } | null>(null);
  const [copying, setCopying] = useState(false);
  const copy = (id: string, dry_run: boolean) =>
    call(api.POST("/api/v1/projects/{id}/copy-week", { params: { path: { id } }, body: { from_week: addDays(week, -7), to_week: week, on_conflict: "unfilled", dry_run } }));
  const planCopy = async () => {
    setCopying(true);
    try {
      let count = 0;
      let touched = 0;
      for (const p of editable) {
        const plan = await copy(p.id, true);
        const n = plan.changes.filter((c) => c.action === "create" || c.action === "replace").length;
        count += n;
        if (n > 0) touched += 1;
      }
      if (count === 0) showToast(t("sheet.copy_nothing"), { tone: "info" });
      else setCopyPlan({ projects: touched, shifts: count });
    } catch (failure) {
      showToast(errorText(codeOf(failure)), { tone: "warning" });
    } finally {
      setCopying(false);
    }
  };
  const runCopy = async () => {
    setCopying(true);
    try {
      for (const p of editable) await copy(p.id, false);
      setCopyPlan(null);
      showToast(t("sheet.copied"), { tone: "success" });
      await shifts.reload();
    } catch (failure) {
      showToast(errorText(codeOf(failure)), { tone: "warning" });
    } finally {
      setCopying(false);
    }
  };

  // A phone shows one day at a time: today when it is in this week, else the week's Sunday.
  const [pickedDay, setPickedDay] = useState<string | null>(null);
  const day = pickedDay && days.includes(pickedDay) ? pickedDay : days.includes(today) ? today : days[0]!;
  const nameOf = (id: string, full?: string | null) => full ?? names.get(id) ?? "";
  const goWeek = (to: string) => router.setParams({ week: to } as never);
  const failure = projects.error ?? clients.error ?? shifts.error;
  const rangeText = `${formatDay(days[0]!)} – ${formatDay(days[6]!)}`;

  return (
    <Screen testID="screen-roster-sheet" fixed={false}>
      <PageTitle eyebrow={[t("app.name"), t("nav.roster")]} title={rangeText}>
        <Button variant="quiet" icon="chevron-back" label={t("sheet.prev_week")} onPress={() => goWeek(addDays(week, -7))} testID="action-prev-week" />
        <Button variant="secondary" label={t("sheet.this_week")} onPress={() => goWeek(startOfWeek(today))} disabled={week === startOfWeek(today)} testID="action-this-week" />
        <Button variant="quiet" icon="chevron-next" iconPosition="trailing" label={t("sheet.next_week")} onPress={() => goWeek(addDays(week, 7))} testID="action-next-week" />
        {editable.length > 0 ? <Button variant="secondary" icon="layers" label={t("sheet.copy_last_week")} busy={copying} onPress={() => void planCopy()} testID="action-copy-last-week" /> : null}
      </PageTitle>
      {totals ? (
        <Txt role="body" tone="secondary" numeric testID="sheet-totals">
          {t("sheet.totals", { places: totals.places, filled: totals.filled, empty: totals.empty, published: totals.published, shifts: totals.shifts })}
          {totals.temporary > 0 ? ` · ${t("sheet.temporary_count", { count: totals.temporary })}` : ""}
        </Txt>
      ) : null}
      {isDesktop && trimmed && trimmed.hidden > 0 && (groups?.length ?? 0) > 0 ? (
        <View style={styles.toggle}>
          <Button
            variant="quiet"
            label={showAll ? t("sheet.hide_quiet") : t("sheet.show_all", { count: trimmed.hidden })}
            onPress={() => setShowAll(!showAll)}
            testID="action-toggle-quiet-projects"
          />
        </View>
      ) : null}
      {failure ? (
        <InlineBanner tone="warning" title={errorText(failure)} action={{ label: t("common.retry"), onPress: () => void Promise.all([projects.reload(), clients.reload(), shifts.reload()]) }} />
      ) : groups === null ? (
        <SkeletonRows rows={8} />
      ) : allGroups!.length === 0 ? (
        <EmptyState icon="calendar" title={t("roster.no_projects")} body={t("roster.no_projects_body")} />
      ) : !isDesktop ? (
        <DayView days={days} day={day} today={today} onDay={setPickedDay} groups={allGroups!} week={week} nameOf={nameOf} />
      ) : groups.length === 0 && !showAll ? (
        <EmptyState icon="calendar" title={t("sheet.no_shifts_title")} body={t("sheet.no_shifts_body")} action={{ label: t("sheet.show_all", { count: trimmed!.hidden }), onPress: () => setShowAll(true) }} />
      ) : (
        <ScrollView horizontal contentContainerStyle={styles.scroller}>
          <View testID="roster-sheet" style={[styles.grid, { borderColor: colors.border.strong, minWidth: widths.total }]}>
            <View style={styles.line}>
              <View style={[styles.head, styles.label, widths.label, { backgroundColor: colors.bg.canvas, borderColor: colors.border.subtle }]} />
              {days.map((day, i) => (
                <View key={day} testID="sheet-day" {...webProps({ dataSet: { date: day } })} style={[styles.head, styles.day, widths.day, { backgroundColor: day === today ? colors.bg.selected : colors.bg.canvas, borderColor: colors.border.subtle }]}>
                  <Txt role="eyebrow" tone="secondary">
                    {t(`sheet.weekday.${i}`)}
                  </Txt>
                  <Txt role="section" numeric>
                    {String(Number(day.slice(8)))}
                  </Txt>
                </View>
              ))}
            </View>
            {groups.map((g) => (
              <View key={g.clientId} testID="sheet-client" {...webProps({ dataSet: { clientId: g.clientId } })}>
                <View style={[styles.clientRow, { backgroundColor: colors.bg.canvas, borderColor: colors.border.subtle }]}>
                  <Txt role="eyebrow" tone="accent">
                    {g.clientName || t("sheet.no_client")}
                  </Txt>
                </View>
                {g.rows.map((row) => (
                  <ProjectRow key={row.project.id} row={row} week={week} nameOf={nameOf} />
                ))}
              </View>
            ))}
          </View>
        </ScrollView>
      )}
      <Sheet open={copyPlan !== null} onClose={() => setCopyPlan(null)} title={t("sheet.copy_last_week")} testID="copy-last-week-confirm">
        <Txt role="body">{t("sheet.copy_plan", { shifts: copyPlan?.shifts ?? 0, projects: copyPlan?.projects ?? 0 })}</Txt>
        <View style={styles.actions}>
          <Button variant="primary" label={t("sheet.copy_confirm")} busy={copying} onPress={() => void runCopy()} testID="action-confirm-copy-last-week" />
          <Button label={t("common.cancel")} onPress={() => setCopyPlan(null)} />
        </View>
      </Sheet>
    </Screen>
  );
}

/** "15.11": the day and month, as the sheet writes dates. */
function formatDay(date: string): string {
  return `⁦${Number(date.slice(8))}.${Number(date.slice(5, 7))}⁩`;
}

function ProjectRow({ row, week, nameOf }: { row: SheetRow; week: string; nameOf: (id: string, full?: string | null) => string }) {
  const { t } = useTranslation();
  const { colors } = useTheme();
  const widths = useWidths();
  const empty = rowEmpty(row);
  return (
    <View style={styles.line} testID="sheet-row" {...webProps({ dataSet: { projectId: row.project.id } })}>
      <LinkPressable href={rosterHref(row.project.id, week)} testID="sheet-project" style={[styles.cell, styles.label, widths.label, { backgroundColor: colors.bg.canvas, borderColor: colors.border.subtle }]}>
        <Txt role="label" numberOfLines={2}>
          {row.project.name}
        </Txt>
        <Txt role="meta" tone={empty > 0 ? "danger" : "secondary"} numeric>
          {row.cells.every((c) => c.length === 0) ? t("sheet.row_none") : empty > 0 ? t("sheet.row_empty", { count: empty }) : t("sheet.row_full")}
        </Txt>
      </LinkPressable>
      {row.cells.map((cell, i) => (
        <View key={i} style={[styles.cell, styles.day, widths.day, { borderColor: colors.border.subtle, backgroundColor: colors.bg.surface }]}>
          {cell.map((shift) => (
            <ShiftBlock key={shift.id} shift={shift} showTime={cell.length > 1 || shift.headcount > 2} nameOf={nameOf} />
          ))}
        </View>
      ))}
    </View>
  );
}

/**
 * The roster on a phone (the owner, 2026-11-13): a Sunday-to-Saturday strip, and for the chosen day
 * the projects with shifts, grouped by client, as cards with their people and empty places.
 */
function DayView({ days, day, today, onDay, groups, week, nameOf }: { days: readonly string[]; day: string; today: string; onDay: (day: string) => void; groups: readonly ClientGroup[]; week: string; nameOf: (id: string, full?: string | null) => string }) {
  const { t } = useTranslation();
  const { colors } = useTheme();
  const index = days.indexOf(day);
  const ofDay = groups
    .map((g) => ({ ...g, rows: g.rows.filter((r) => (r.cells[index] ?? []).length > 0) }))
    .filter((g) => g.rows.length > 0);
  return (
    <View style={styles.dayView} testID="roster-day-view">
      <View role="tablist" style={[styles.strip, { borderColor: colors.border.strong }]} testID="day-strip">
        {days.map((d, i) => {
          const on = d === day;
          return (
            <Pressable
              key={d}
              role="tab"
              aria-selected={on}
              accessibilityState={{ selected: on }}
              accessibilityLabel={`${t(`sheet.weekday.${i}`)} ${Number(d.slice(8))}`}
              testID={`day-${d}`}
              onPress={() => onDay(d)}
              style={[styles.stripDay, { borderTopColor: on ? colors.accent.default : "transparent", backgroundColor: on ? colors.bg.selected : d === today ? colors.bg.surface : "transparent" }]}
            >
              <Txt role="eyebrow" tone={on ? "accent" : "secondary"}>
                {t(`sheet.weekday.${i}`)}
              </Txt>
              <Txt role="label" numeric>
                {String(Number(d.slice(8)))}
              </Txt>
            </Pressable>
          );
        })}
      </View>
      {ofDay.length === 0 ? (
        <Txt role="body" tone="secondary" testID="day-empty">
          {t("sheet.day_none")}
        </Txt>
      ) : (
        ofDay.map((g) => (
          <View key={g.clientId} style={styles.dayClient} testID="sheet-client">
            <Txt role="eyebrow" tone="accent">
              {g.clientName || t("sheet.no_client")}
            </Txt>
            {g.rows.map((row) => {
              const shifts = row.cells[index]!;
              const empty = shifts.reduce((n, s) => n + Math.max(0, s.headcount - s.assignments.length), 0);
              return (
                <View key={row.project.id} testID="day-card" style={[styles.dayCard, { borderColor: colors.border.subtle, backgroundColor: colors.bg.surface }]}>
                  <LinkPressable href={rosterHref(row.project.id, week)} testID="sheet-project" style={styles.dayCardHead}>
                    <Txt role="label" numberOfLines={2} style={styles.name}>
                      {row.project.name}
                    </Txt>
                    <Txt role="meta" tone={empty > 0 ? "danger" : "secondary"} numeric>
                      {empty > 0 ? t("sheet.row_empty", { count: empty }) : t("sheet.row_full")}
                    </Txt>
                  </LinkPressable>
                  {shifts.map((shift) => (
                    <ShiftBlock key={shift.id} shift={shift} showTime nameOf={nameOf} />
                  ))}
                </View>
              );
            })}
          </View>
        ))
      )}
    </View>
  );
}

/** One shift: its times when needed, its people as blocks (with the temporary chip) and its empty places dashed. */
function ShiftBlock({ shift, showTime, nameOf }: { shift: Shift; showTime: boolean; nameOf: (id: string, full?: string | null) => string }) {
  const { t } = useTranslation();
  const { colors } = useTheme();
    const open = Math.max(0, shift.headcount - shift.assignments.length);
    const published = shift.status === "published";
    return (
      <View testID="sheet-shift" {...webProps({ dataSet: { shiftId: shift.id, status: shift.status } })} style={styles.shift}>
        {showTime ? (
          <Txt role="meta" tone="secondary" numeric numberOfLines={1}>
            {formatTimeRange(shift.starts_at, shift.ends_at, shift.timezone)}
          </Txt>
        ) : null}
        {shift.assignments.map((a) => {
          const temp = isTemporary(a);
          return (
            <View
              key={a.id}
              testID="sheet-person"
              {...webProps({ dataSet: { userId: a.user_id, temporary: String(temp) } })}
              style={[
                styles.blk,
                { borderStartColor: colors.text.primary, backgroundColor: published ? colors.bg.surface : colors.bg.selected },
                published ? { borderColor: colors.border.strong, borderWidth: 1, borderStartWidth: 3 } : null,
              ]}
            >
              <Txt role="meta" numberOfLines={1} style={styles.name}>
                {shortName(nameOf(a.user_id, a.full_name))}
              </Txt>
              {temp ? (
                <View testID="chip-temporary" style={[styles.tchip, { borderColor: colors.status.info }]}>
                  <Txt role="eyebrow" style={{ color: colors.status.info }}>
                    {t("sheet.temporary")}
                  </Txt>
                </View>
              ) : null}
            </View>
          );
        })}
        {open > 0 ? (
          <View testID="sheet-empty" style={[styles.blk, styles.empty, { borderColor: colors.status.danger }]}>
            <Txt role="eyebrow" tone="danger">
              {t("sheet.empty_places", { count: open })}
            </Txt>
          </View>
        ) : null}
      </View>
    );
}

const styles = StyleSheet.create({
  scroller: { flexGrow: 1 },
  dayView: { gap: 16 },
  strip: { flexDirection: "row", borderBottomWidth: 1 },
  stripDay: { flex: 1, alignItems: "center", paddingVertical: 6, borderTopWidth: 2, minHeight: 48, justifyContent: "center" },
  dayClient: { gap: 8 },
  dayCard: { borderWidth: 1, padding: 10, gap: 8 },
  dayCardHead: { flexDirection: "row", alignItems: "flex-start", justifyContent: "space-between", gap: 8 },
  grid: { borderWidth: 1, flexGrow: 1 },
  line: { flexDirection: "row" },
  head: { paddingVertical: 4, paddingHorizontal: 6, borderStartWidth: 1, borderTopWidth: 1 },
  label: { flexShrink: 0 },
  day: { flex: 1 },
  toggle: { flexDirection: "row" },
  clientRow: { paddingVertical: 4, paddingHorizontal: 8, borderTopWidth: 1 },
  cell: { padding: 6, borderStartWidth: 1, borderTopWidth: 1, minHeight: 52, gap: 2 },
  shift: { gap: 3, marginBottom: 4 },
  blk: { flexDirection: "row", alignItems: "center", gap: 4, borderStartWidth: 3, paddingVertical: 2, paddingHorizontal: 6 },
  name: { flexShrink: 1 },
  empty: { borderWidth: 1, borderStyle: "dashed", borderStartWidth: 3, backgroundColor: "transparent" },
  tchip: { borderWidth: 1, paddingHorizontal: 4 },
  actions: { flexDirection: "row", flexWrap: "wrap", gap: 8, marginTop: 12 },
});
