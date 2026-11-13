/**
 * The roster week as the managers' sheet (specs/demo-ceo/plan.md, screen 2; deck slide 7): every
 * project the reader sees as a row, grouped by client, the seven days as columns; names as blocks,
 * empty places dashed, a "temporary" chip on temporary workers, the warnings strip, the week switch
 * and "copy last week". A project's name opens its own week for editing.
 */
import { useRouter } from "expo-router";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { ScrollView, StyleSheet, View } from "react-native";

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
import { shiftDays, weekDays, type Shift } from "./model";
import { rosterHref } from "./ProjectChooser";
import { buildSheet, flagKey, isTemporary, rowEmpty, sheetTotals, shortName, weekWarnings, withShifts, type SheetRow } from "./sheet";
import { numbersOf } from "./WarningSheet";

const LABEL_WIDTH = 176;
const DAY_MIN_WIDTH = 124;
/** How many warning lines the strip shows before "and N more". */
const STRIP_LINES = 8;

/** "Tue 10 · 07:00–15:00": a shift's day and times, for the warnings strip. */
function formatDayTime(shift: Shift): string {
  return `${Number(shiftDays(shift).start.slice(8))}.${Number(shiftDays(shift).start.slice(5, 7))} · ${formatTimeRange(shift.starts_at, shift.ends_at, shift.timezone)}`;
}

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
  const { colors } = useTheme();
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
  // The week's warnings across projects, judged as an edit would be (drafts included).
  const warningsLoad = useLoad(async () => (await call(api.GET("/api/v1/roster/warnings", { params: { query: { from: week, to: addDays(week, 6) } } }))).warnings, [week]);
  const warns = weekWarnings(warningsLoad.data ?? []);
  const shiftById = new Map((shifts.data ?? []).map((s) => [s.id, s]));
  const names = new Map<string, string>();
  for (const s of shifts.data ?? []) for (const a of s.assignments) if (a.full_name) names.set(a.user_id, a.full_name);
  const projectName = (id: string) => projects.data?.find((p) => p.id === id)?.name ?? "";

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
          {t("sheet.totals", { places: totals.places, filled: totals.filled, empty: totals.empty, warnings: warns.open.length, published: totals.published, shifts: totals.shifts })}
          {totals.temporary > 0 ? ` · ${t("sheet.temporary_count", { count: totals.temporary })}` : ""}
        </Txt>
      ) : null}
      {trimmed && trimmed.hidden > 0 && (groups?.length ?? 0) > 0 ? (
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
                  <ProjectRow key={row.project.id} row={row} week={week} flagged={warns.flagged} nameOf={(id, full) => full ?? names.get(id) ?? ""} />
                ))}
              </View>
            ))}
          </View>
        </ScrollView>
      )}
      {warns.open.length > 0 || warns.kept > 0 ? (
        <View testID="warnings-strip" style={[styles.warnbox, { borderColor: colors.status.warning, backgroundColor: colors.bg.surface }]}>
          <Txt role="label" tone="warning">
            {t("sheet.warnings_open", { count: warns.open.length })}
            {warns.kept > 0 ? ` · ${t("sheet.warnings_kept", { count: warns.kept })}` : ""}
          </Txt>
          {warns.open.slice(0, STRIP_LINES).map(({ first: w, values }) => {
            const shift = shiftById.get(w.shift_ids[0] ?? "");
            const where = shift ? `${projectName(shift.project_id)} · ${formatDayTime(shift)}` : "";
            const numbers = numbersOf(w);
            return (
              <Txt key={`${w.employee_id ?? w.shift_ids[0]}|${w.code}`} role="meta" testID="sheet-warning">
                <Txt role="meta" weight={500}>
                  {w.employee_id ? names.get(w.employee_id) ?? "" : t("roster.the_shift")}
                </Txt>
                {` · ${where} — ${t(w.code)}${numbers ? ` ${values.map((v) => t(numbers, v)).join(" ")}` : ""}`}
              </Txt>
            );
          })}
          {warns.open.length > STRIP_LINES ? (
            <Txt role="meta" tone="secondary">
              {t("sheet.warnings_more", { count: warns.open.length - STRIP_LINES })}
            </Txt>
          ) : null}
        </View>
      ) : null}
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

function ProjectRow({ row, week, flagged, nameOf }: { row: SheetRow; week: string; flagged: Set<string>; nameOf: (id: string, full?: string | null) => string }) {
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
          {cell.map((shift) => {
            const open = Math.max(0, shift.headcount - shift.assignments.length);
            const published = shift.status === "published";
            return (
              <View key={shift.id} testID="sheet-shift" {...webProps({ dataSet: { shiftId: shift.id, status: shift.status } })} style={styles.shift}>
                {cell.length > 1 || shift.headcount > 2 ? (
                  <Txt role="meta" tone="secondary" numeric numberOfLines={1}>
                    {formatTimeRange(shift.starts_at, shift.ends_at, shift.timezone)}
                  </Txt>
                ) : null}
                {shift.assignments.map((a) => {
                  const temp = isTemporary(a);
                  const warned = flagged.has(flagKey(shift.id, a.user_id));
                  return (
                    <View
                      key={a.id}
                      testID="sheet-person"
                      {...webProps({ dataSet: { userId: a.user_id, temporary: String(temp), warning: String(warned) } })}
                      style={[
                        styles.blk,
                        { borderStartColor: colors.text.primary, backgroundColor: published ? colors.bg.surface : colors.bg.selected },
                        published ? { borderColor: colors.border.strong, borderWidth: 1, borderStartWidth: 3 } : null,
                        // An open warning, as the deck draws it: the warning colour on the block's edge.
                        warned ? { borderColor: colors.status.warning, borderStartColor: colors.status.warning, borderWidth: 1, borderStartWidth: 3 } : null,
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
          })}
        </View>
      ))}
    </View>
  );
}

const styles = StyleSheet.create({
  scroller: { flexGrow: 1 },
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
  warnbox: { borderWidth: 1, padding: 10, gap: 4 },
  actions: { flexDirection: "row", flexWrap: "wrap", gap: 8, marginTop: 12 },
});
