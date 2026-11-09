/**
 * The roster week (specs/007-ux-redesign FR-025 to FR-041, T070 to T074): Sunday–Saturday columns
 * and a row per shift type on a desktop, a day agenda on a phone; each shift a block with its people
 * drawn against it (a half shift narrower, extra hours past its edge), coverage and empty seats;
 * assigning by tapping, warnings with their rule and numbers and an override with a reason; a
 * publish review; Undo for unpublished changes; and the stale-save banner. A Manager edits only the
 * projects they manage (`roster.edit:project`); others read.
 */
import { useLocalSearchParams, useRouter } from "expo-router";
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { ScrollView, StyleSheet, View } from "react-native";

import { useErrorText } from "../../api/errors";
import { useCapabilities } from "../../capabilities";
import { previewsEnabled } from "../../config/previews";
import { Button, EmptyState, InlineBanner, LinkPressable, PageTitle, Sheet, SkeletonRows, Txt, showToast } from "../../design/components";
import { Screen } from "../../design/components/Screen";
import { useTheme } from "../../design/theme/ThemeProvider";
import { webProps } from "../../design/web";
import { addDays, formatHeaderDate, formatTimeRange, startOfWeek, todayIn, DEFAULT_TIME_ZONE } from "../../locale";
import { AssignSheet } from "./AssignSheet";
import { Block } from "./Block";
import { PersonSheet } from "./PersonSheet";
import { ProjectChooser } from "./ProjectChooser";
import { PublishSheet } from "./PublishSheet";
import { WarningSheet } from "./WarningSheet";
import { cellOf, isOvernight, placed, shiftDays, weekDays, type Alert, type Shift } from "./model";
import { useCopyNextWeek } from "./useCopyNextWeek";
import { useAssignInputs, useRosterData } from "./useRosterData";
import { useRosterEdits } from "./useRosterEdits";

export function RosterScreen() {
  const { t, i18n } = useTranslation();
  const params = useLocalSearchParams<{ project?: string; week?: string; day?: string; all?: string }>();
  const capabilities = useCapabilities();
  const errorText = useErrorText();
  const { colors, isDesktop } = useTheme();
  const project = params.project ?? null;
  const week = startOfWeek(params.week ?? todayIn(DEFAULT_TIME_ZONE));
  const days = weekDays(week);
  const router = useRouter();
  const editable = project !== null && capabilities.hasOnProject("roster.edit:project", project);
  // Publishing has its own capability, the one the API checks (roster.publish:project).
  const publishable = project !== null && capabilities.hasOnProject("roster.publish:project", project);
  const runnable = project !== null && capabilities.has("schedule.generate") && editable;

  const { types, shifts, people, nameOf } = useRosterData(project, week);
  const [assignTo, setAssignTo] = useState<Shift | null>(null);
  const [personOn, setPersonOn] = useState<{ shift: Shift; assignmentId: string; userId: string } | null>(null);
  const [warningsOf, setWarningsOf] = useState<Shift | null>(null);
  const [publishing, setPublishing] = useState(false);
  // Replacing someone on a shift opens the assign sheet with them named (T042f).
  const [replacing, setReplacing] = useState<{ shift: Shift; assignmentId: string; userId: string } | null>(null);
  // A word to the person placed, and who hears of the edit (T042g); kept while a sheet is open.
  const [note, setNote] = useState("");
  const [alert, setAlert] = useState<Alert>("none");
  const { warnings, dropWarning, history, clearHistory, stale, dismissStale, assign, unassign, move, undo, lockAssignment } = useRosterEdits({ shifts, note, alert, onBreaks: setWarningsOf });
  const { copyReplaces, setCopyReplaces, copyToNextWeek } = useCopyNextWeek(project, week);
  const { ranked, busy } = useAssignInputs(assignTo);

  const byCell = useMemo(() => placed(shifts.data ?? []), [shifts.data]);
  const hasDrafts = (shifts.data ?? []).some((shift) => shift.status === "draft");
  // The shift whose warnings are open, as read last: its assignments are the ones an override needs.
  const warningShift = warningsOf ? ((shifts.data ?? []).find((s) => s.id === warningsOf.id) ?? warningsOf) : null;
  const lockedNow = personOn ? !!personOn.shift.assignments.find((a) => a.id === personOn.assignmentId)?.locked : false;

  if (!project) {
    return (
      <Screen testID="screen-roster">
        <PageTitle eyebrow={[t("app.name"), t("nav.group.manage")]} title={t("nav.roster")} />
        <ProjectChooser week={params.week} stay={params.all === "1"} />
      </Screen>
    );
  }

  const loading = types.data === null || shifts.data === null;
  const shownDays = !isDesktop && params.day ? [params.day] : days;
  const block = (shift: Shift) => (
    <Block
      key={shift.id}
      shift={shift}
      editable={editable}
      warnings={warnings.get(shift.id) ?? []}
      nameOf={nameOf}
      onOpenSeat={() => editable && setAssignTo(shift)}
      onPerson={(assignmentId, userId) => editable && setPersonOn({ shift, assignmentId, userId })}
      onWarnings={() => setWarningsOf(shift)}
    />
  );

  return (
    <Screen testID="screen-roster" fixed={isDesktop}>
      <PageTitle eyebrow={[t("app.name"), t("nav.group.manage")]} title={t("nav.roster")}>
        {/* Every project, the read-only ones included, even for the manager of one (group O review). */}
        <LinkPressable href="/roster?all=1" testID="action-all-projects" style={styles.allProjects}>
          <Txt role="label">{t("roster.all_projects")}</Txt>
        </LinkPressable>
        <Button variant="secondary" icon="undo" label={t("roster.undo")} onPress={() => void undo()} disabled={history.length === 0} testID="action-undo-roster" />
        {runnable ? (
          // The run form set to this week and this project (008 FR-032).
          <Button variant="secondary" icon="play" label={t("roster.generate_week")} onPress={() => router.push(`/runs?week=${week}&project=${project}` as never)} testID="action-generate-week" />
        ) : null}
        {publishable ? (
          <Button
            variant="primary"
            icon="check"
            label={t("roster.publish")}
            onPress={() => setPublishing(true)}
            // Nothing to publish when every shift of the week already is (T042e): say so, never a sheet of zeros.
            disabled={!hasDrafts}
            disabledReason={hasDrafts ? undefined : t("roster.nothing_to_publish")}
            testID="action-publish"
          />
        ) : null}
      </PageTitle>
      {stale ? <InlineBanner tone="warning" testID="banner-roster-changed" title={t("roster.changed")} onDismiss={dismissStale} /> : null}
      {previewsEnabled ? (
        <EmptyState testID="preview-cost-strip" icon="wallet" badge={t("preview.badge")} title={t("preview.cost_strip_title")} body={t("preview.cost_strip_body")} feature={t("preview.comes_with", { feature: "011" })} />
      ) : null}
      {shifts.error || types.error ? (
        <InlineBanner tone="warning" title={errorText((shifts.error ?? types.error)!)} action={{ label: t("common.retry"), onPress: () => void shifts.reload() }} />
      ) : loading ? (
        <SkeletonRows rows={6} />
      ) : isDesktop ? (
        <ScrollView horizontal contentContainerStyle={styles.week}>
          {days.map((day) => (
            <View key={day} testID="roster-day" {...webProps({ dataSet: { date: day } })} style={[styles.day, { borderColor: colors.border.subtle }]}>
              <Txt role="label" style={styles.dayHead}>
                {formatHeaderDate(day, i18n.language)}
              </Txt>
              {types.data!.map((type) => (
                <View key={type.id} testID="roster-row" {...webProps({ dataSet: { shiftType: type.id } })} style={styles.row}>
                  {(byCell.get(cellOf(day, type.id)) ?? []).map(block)}
                  {(shifts.data ?? [])
                    .filter((s) => s.shift_type_id === type.id && isOvernight(s) && shiftDays(s).end === day)
                    .map((s) => (
                      <View key={`${s.id}-next`} testID="roster-continuation" {...webProps({ dataSet: { shiftId: s.id } })} style={[styles.continuation, { borderColor: colors.border.strong }]}>
                        <Txt role="meta" tone="secondary" numeric>
                          {formatTimeRange(s.starts_at, s.ends_at, s.timezone)}
                        </Txt>
                      </View>
                    ))}
                </View>
              ))}
            </View>
          ))}
        </ScrollView>
      ) : (
        <View testID="roster-agenda" style={styles.agenda}>
          {shownDays.map((day) => {
            const ofDay = (shifts.data ?? []).filter((s) => shiftDays(s).start === day);
            return (
              <View key={day} testID="roster-day" {...webProps({ dataSet: { date: day } })} style={styles.agendaDay}>
                <Txt role="label">{formatHeaderDate(day, i18n.language)}</Txt>
                {ofDay.length === 0 ? (
                  <Txt role="meta" tone="secondary">
                    {t("roster.no_shifts")}
                  </Txt>
                ) : (
                  ofDay.map(block)
                )}
              </View>
            );
          })}
        </View>
      )}

      <AssignSheet
        shift={assignTo}
        people={people.data ?? []}
        busy={busy}
        ranked={ranked}
        replacing={replacing ? nameOf(replacing.userId) : null}
        note={note}
        alert={alert}
        onNote={setNote}
        onAlert={setAlert}
        onClose={() => {
          setAssignTo(null);
          setReplacing(null);
          setNote("");
        }}
        onChoose={(userId, how, busyOn) => {
          const shift = assignTo!;
          const out = replacing;
          setAssignTo(null);
          setReplacing(null);
          void (async () => {
            if (how === "move" && busyOn?.assignment_id && busyOn.shift_id) {
              // A swap trades places with the person being replaced; otherwise a transfer.
              await move(busyOn.assignment_id, userId, busyOn.shift_id, shift, out ? out.assignmentId : null);
            } else if (out) {
              // The removal moved the shift's etag: assign with the new one, never a stale read (R40).
              const etag = await unassign(out.shift, out.assignmentId);
              if (etag === null) return;
              await assign({ ...shift, etag }, userId);
            } else {
              await assign(shift, userId);
            }
            // A choice of who hears of it is for one edit, never carried to the next (R43).
            setNote("");
            setAlert("none");
          })();
        }}
      />
      <PersonSheet
        open={personOn !== null}
        name={nameOf(personOn?.userId ?? null)}
        locked={lockedNow}
        alert={alert}
        onAlert={setAlert}
        onClose={() => setPersonOn(null)}
        onUnassign={() => {
          const target = personOn!;
          setPersonOn(null);
          void unassign(target.shift, target.assignmentId).then(() => setAlert("none"));
        }}
        onLock={() => {
          const target = personOn!;
          setPersonOn(null);
          void lockAssignment(target.assignmentId, !lockedNow);
        }}
        onReplace={() => {
          const target = personOn!;
          setPersonOn(null);
          setReplacing(target);
          setAssignTo(target.shift);
        }}
      />
      <WarningSheet shift={warningShift} warnings={warningsOf ? (warnings.get(warningsOf.id) ?? []) : []} nameOf={nameOf} onClose={() => setWarningsOf(null)} onOverridden={(code, employee) => {
        dropWarning(warningsOf!.id, code, employee);
        setWarningsOf(null);
      }} />
      <PublishSheet open={publishing} project={project} first={week} last={addDays(week, 6)} onClose={() => setPublishing(false)} onPublished={async () => {
        setPublishing(false);
        clearHistory();
        await shifts.reload();
        // After a publish, the next step is next week (008 FR-033): copy this week's shifts there,
        // replacing only drafts, never a published shift, once the manager agrees, and open it.
        showToast(t("roster.published"), {
          tone: "success",
          action: { label: t("roster.copy_next_week"), onPress: () => void copyToNextWeek() },
          testID: "toast-published",
        });
      }} />
      <Sheet open={copyReplaces !== null} onClose={() => setCopyReplaces(null)} title={t("roster.copy_next_week")} testID="copy-confirm">
        <Txt role="body">{t("roster.copy_replaces", { count: copyReplaces ?? 0 })}</Txt>
        <View style={styles.actions}>
          <Button variant="primary" label={t("roster.copy_replace")} onPress={() => void copyToNextWeek(true)} testID="action-confirm-copy" />
          <Button label={t("common.cancel")} onPress={() => setCopyReplaces(null)} testID="action-cancel-copy" />
        </View>
      </Sheet>
    </Screen>
  );
}

const styles = StyleSheet.create({
  actions: { flexDirection: "row", flexWrap: "wrap", gap: 8 },
  allProjects: { minHeight: 44, justifyContent: "center", paddingHorizontal: 8 },
  week: { flexDirection: "row", gap: 8 },
  day: { width: 168, borderWidth: 1, padding: 6, gap: 6 },
  dayHead: { paddingBottom: 4 },
  row: { gap: 6, minHeight: 40 },
  continuation: { borderWidth: 1, borderStyle: "dashed", padding: 6 },
  agenda: { gap: 16 },
  agendaDay: { gap: 8 },
});
