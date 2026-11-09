/**
 * The roster week (specs/007-ux-redesign FR-025 to FR-041, T070 to T074): Sunday–Saturday columns
 * and a row per shift type on a desktop, a day agenda on a phone; each shift a block with its people
 * drawn against it (a half shift narrower, extra hours past its edge), coverage and empty seats;
 * assigning by tapping, warnings with their rule and numbers and an override with a reason; a
 * publish review; Undo for unpublished changes; and the stale-save banner. A Manager edits only the
 * projects they manage (`roster.edit:project`); others read.
 */
import { useLocalSearchParams } from "expo-router";
import { useCallback, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Pressable, ScrollView, StyleSheet, View } from "react-native";

import { ApiError, call, codeOf, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { useCapabilities } from "../../capabilities";
import { previewsEnabled } from "../../config/previews";
import type { components } from "@hds/api-client";

import { Button, EmptyState, InlineBanner, LinkPressable, PageTitle, SegmentedControl, Sheet, SkeletonRows, TextField, Txt, showToast } from "../../design/components";
import { Screen } from "../../design/components/Screen";
import { Icon } from "../../design/icons/Icon";
import { useTheme } from "../../design/theme/ThemeProvider";
import { webProps } from "../../design/web";
import { addDays, formatHeaderDate, formatTimeRange, startOfWeek, todayIn, DEFAULT_TIME_ZONE } from "../../locale";
import { useLoad } from "../useLoad";
import { ProjectChooser } from "./ProjectChooser";
import { barGeometry, cellOf, coverage, isOvernight, placed, shiftDays, weekDays, type RosterChange, type Shift, type Warning } from "./model";

type Person = { id: string; full_name: string };

export function RosterScreen() {
  const { t, i18n } = useTranslation();
  const params = useLocalSearchParams<{ project?: string; week?: string; day?: string; all?: string }>();
  const session = useSession();
  const capabilities = useCapabilities();
  const errorText = useErrorText();
  const { colors, isDesktop } = useTheme();
  const project = params.project ?? null;
  const week = startOfWeek(params.week ?? todayIn(DEFAULT_TIME_ZONE));
  const days = weekDays(week);
  const editable = project !== null && capabilities.hasOnProject("roster.edit:project", project);
  // Publishing has its own capability, the one the API checks (roster.publish:project).
  const publishable = project !== null && capabilities.hasOnProject("roster.publish:project", project);

  const types = useLoad(
    async () => (project ? (await call(session.api.GET("/api/v1/projects/{id}/shift-types", { params: { path: { id: project } } }))).items : []),
    [project],
  );
  // Every page of the week's shifts, so a busy week is never cut short.
  const shifts = useLoad(async () => {
    if (!project) return [];
    const all: Shift[] = [];
    let cursor: string | null = null;
    do {
      const query: { from: string; to: string; project_id: string; limit: number; cursor?: string } = { from: week, to: addDays(week, 6), project_id: project, limit: 200 };
      if (cursor) query.cursor = cursor;
      const page: { items: Shift[]; next_cursor?: string | null } = await call(session.api.GET("/api/v1/shifts", { params: { query } }));
      all.push(...page.items);
      cursor = page.next_cursor ?? null;
    } while (cursor);
    return all;
  }, [project, week]);
  const people = useLoad(async () => {
    const found = await call(session.api.GET("/api/v1/employees", { params: { query: { is_active: true } } }));
    return found.items.filter((u) => u.role !== "client").map((u): Person => ({ id: u.id, full_name: u.full_name }));
  }, []);

  const [warnings, setWarnings] = useState<Map<string, Warning[]>>(() => new Map());
  const [history, setHistory] = useState<RosterChange[]>([]);
  const [stale, setStale] = useState(false);
  const [assignTo, setAssignTo] = useState<Shift | null>(null);
  const [personOn, setPersonOn] = useState<{ shift: Shift; assignmentId: string; userId: string } | null>(null);
  const [warningsOf, setWarningsOf] = useState<Shift | null>(null);
  const [publishing, setPublishing] = useState(false);
  // Replacing someone on a shift opens the assign sheet with them named (T042f).
  const [replacing, setReplacing] = useState<{ shift: Shift; assignmentId: string; userId: string } | null>(null);
  // A word to the person placed, and who hears of the edit (T042g); kept while a sheet is open.
  const [note, setNote] = useState("");
  const [alert, setAlert] = useState<Alert>("none");
  // Who is already on another shift at the sheet's shift's time, in any project (T042f).
  const busy = useLoad(
    async () =>
      assignTo
        ? (await call(session.api.GET("/api/v1/shifts/{id}/busy", { params: { path: { id: assignTo.id } } }))).items
        : [],
    [assignTo?.id],
  );

  // Names from the roster read itself (deactivated people included); the active list fills the rest.
  const names = useMemo(() => {
    const known = new Map((people.data ?? []).map((p) => [p.id, p.full_name]));
    for (const shift of shifts.data ?? []) for (const a of shift.assignments) if (a.full_name) known.set(a.user_id, a.full_name);
    return known;
  }, [people.data, shifts.data]);
  const nameOf = useCallback((id: string | null) => (id ? (names.get(id) ?? "") : ""), [names]);
  const byCell = useMemo(() => placed(shifts.data ?? []), [shifts.data]);
  const hasDrafts = (shifts.data ?? []).some((shift) => shift.status === "draft");
  // The shift whose warnings are open, as read last: its assignments are the ones an override needs.
  const warningShift = warningsOf ? ((shifts.data ?? []).find((s) => s.id === warningsOf.id) ?? warningsOf) : null;

  /** Run one roster change; a stale read (412) shows the banner and the latest version. */
  const change = async (run: () => Promise<{ shift_etag: string; warnings: Warning[] } | null>, shiftId: string): Promise<boolean> => {
    try {
      const answer = await run();
      if (answer) setWarnings((map) => new Map(map).set(shiftId, answer.warnings.filter((w) => w.shift_ids.includes(shiftId))));
      setStale(false);
      await shifts.reload();
      return true;
    } catch (failure) {
      if (failure instanceof ApiError && failure.status === 412) {
        setStale(true);
        await shifts.reload();
      } else {
        showToast(errorText(codeOf(failure)), { tone: "warning" });
      }
      return false;
    }
  };

  const assign = async (shift: Shift, userId: string, times?: { starts_at: string | null; ends_at: string | null }, record = true): Promise<string | null> => {
    let created: string | null = null;
    let breaks = false;
    const done = await change(async () => {
      const answer = await call(
        session.api.POST("/api/v1/shifts/{id}/assignments", {
          params: { path: { id: shift.id }, header: { "If-Match": shift.etag } },
          body: {
            user_id: userId,
            ...(times?.starts_at ? { starts_at: times.starts_at, ends_at: times.ends_at } : {}),
            alert: record ? alert : "none",
            ...(record && note.trim() ? { note: note.trim() } : {}),
          },
        }),
      );
      created = answer.assignment.id;
      breaks = answer.warnings.some((w) => w.employee_id === userId && w.shift_ids.includes(shift.id));
      return answer;
    }, shift.id);
    // The assignment is made; any rule it breaks is then named in a dialog that only informs
    // (T042d). Only for a choice made now, never for undo re-making an assignment.
    if (done && breaks && record) setWarningsOf(shift);
    if (done && record && created) setHistory((h) => [...h, { kind: "assigned", assignmentId: created!, shiftId: shift.id, userId }]);
    return done ? created : null;
  };

  /** Take someone off; the shift's new etag on success (a replace assigns with it, R40), else null. */
  const unassign = async (shift: Shift, assignmentId: string, record = true): Promise<string | null> => {
    const before = shift.assignments.find((a) => a.id === assignmentId);
    let etag: string | null = null;
    const done = await change(async () => {
      const answer = await call(
        session.api.DELETE("/api/v1/assignments/{id}", {
          params: { path: { id: assignmentId }, header: { "If-Match": shift.etag }, query: { alert: record ? alert : "none" } },
        }),
      );
      etag = answer.shift_etag;
      return answer;
    }, shift.id);
    if (done && record && before) {
      setHistory((h) => [
        ...h,
        { kind: "unassigned", shiftId: shift.id, userId: before.user_id, starts_at: before.own_times ? before.starts_at : null, ends_at: before.own_times ? before.ends_at : null },
      ]);
    }
    return done ? etag : null;
  };

  /** Lock an assignment, so later runs keep it, or unlock it (008 FR-018). */
  const lockAssignment = async (assignmentId: string, on: boolean) => {
    try {
      const path = { params: { path: { id: assignmentId } } };
      await call(on ? session.api.PUT("/api/v1/assignments/{id}/lock", path) : session.api.DELETE("/api/v1/assignments/{id}/lock", path));
      await shifts.reload();
    } catch (failure) {
      showToast(errorText(codeOf(failure)), { tone: "warning" });
    }
  };
  const lockedNow = personOn ? !!personOn.shift.assignments.find((a) => a.id === personOn.assignmentId)?.locked : false;

  /** Move an assignment to ``shift``, or swap it with ``swapWith`` there, in one step (T042f). */
  const move = async (assignmentId: string, userId: string, fromShiftId: string, shift: Shift, swapWith: string | null, record = true): Promise<boolean> => {
    try {
      const answer = await call(
        session.api.POST("/api/v1/assignments/{id}/move", {
          params: { path: { id: assignmentId } },
          body: { to_shift_id: shift.id, ...(swapWith ? { swap_with: swapWith } : {}), alert: record ? alert : "none", ...(record && note.trim() ? { note: note.trim() } : {}) },
        }),
      );
      const found = answer.warnings as Warning[];
      setWarnings((map) => {
        const next = new Map(map);
        for (const id of [fromShiftId, shift.id]) next.set(id, found.filter((w) => w.shift_ids.includes(id)));
        return next;
      });
      await shifts.reload();
      if (record) {
        setHistory((h) => [...h, { kind: "moved", assignmentId, shiftId: shift.id, fromShiftId, swapWith }]);
        if (found.some((w) => w.employee_id === userId && w.shift_ids.includes(shift.id))) setWarningsOf(shift);
      }
      return true;
    } catch (failure) {
      showToast(errorText(codeOf(failure)), { tone: "warning" });
      return false;
    }
  };

  // A step leaves the history only once its request has succeeded; re-creating an assignment
  // gives it a new id, which earlier steps for that person then follow (R88).
  const undo = async () => {
    const last = history[history.length - 1];
    if (!last) return;
    if (last.kind === "moved") {
      // Back to the shift it came from (in any project), swapping again when it was a swap.
      const back = { id: last.fromShiftId } as Shift;
      if (await move(last.assignmentId, "", last.shiftId, back, last.swapWith, false)) setHistory((h) => h.slice(0, -1));
      return;
    }
    const shift = shifts.data?.find((s) => s.id === last.shiftId);
    if (!shift) return;
    if (last.kind === "assigned") {
      if (await unassign(shift, last.assignmentId, false)) setHistory((h) => h.slice(0, -1));
      return;
    }
    const id = await assign(shift, last.userId, { starts_at: last.starts_at, ends_at: last.ends_at }, false);
    if (id === null) return;
    setHistory((h) =>
      h.slice(0, -1).map((step) => (step.kind === "assigned" && step.shiftId === last.shiftId && step.userId === last.userId ? { ...step, assignmentId: id } : step)),
    );
  };

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
      {stale ? <InlineBanner tone="warning" testID="banner-roster-changed" title={t("roster.changed")} onDismiss={() => setStale(false)} /> : null}
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
        busy={busy.data ?? []}
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
      <Sheet open={personOn !== null} onClose={() => setPersonOn(null)} title={nameOf(personOn?.userId ?? null)} testID="person-sheet">
        <AlertChoice value={alert} onChange={setAlert} />
        <Button variant="secondary" icon="close" label={t("roster.unassign")} testID="action-unassign" onPress={() => {
          const target = personOn!;
          setPersonOn(null);
          void unassign(target.shift, target.assignmentId).then(() => setAlert("none"));
        }} />
        {personOn ? (
          <Button
            variant="secondary"
            icon="lock"
            label={lockedNow ? t("roster.unlock") : t("roster.lock")}
            testID="action-lock"
            onPress={() => {
              const target = personOn;
              setPersonOn(null);
              void lockAssignment(target.assignmentId, !lockedNow);
            }}
          />
        ) : null}
        <Button variant="secondary" icon="people" label={t("roster.replace")} testID="action-replace" onPress={() => {
          const target = personOn!;
          setPersonOn(null);
          setReplacing(target);
          setAssignTo(target.shift);
        }} />
      </Sheet>
      <WarningSheet shift={warningShift} warnings={warningsOf ? (warnings.get(warningsOf.id) ?? []) : []} nameOf={nameOf} onClose={() => setWarningsOf(null)} onOverridden={(code, employee) => {
        const id = warningsOf!.id;
        setWarnings((map) => new Map(map).set(id, (map.get(id) ?? []).filter((w) => !(w.code === code && w.employee_id === employee))));
        setWarningsOf(null);
      }} />
      <PublishSheet open={publishing} project={project} first={week} last={addDays(week, 6)} onClose={() => setPublishing(false)} onPublished={async () => {
        setPublishing(false);
        setHistory([]);
        await shifts.reload();
      }} />
    </Screen>
  );
}


/** Extra hours draw past the block's edge by design (US3); the overflow check (FR-079) allows it. */
const OVERHANG = webProps({ dataSet: { overflow: "intended" } as const });

function Block({
  shift,
  editable,
  warnings,
  nameOf,
  onOpenSeat,
  onPerson,
  onWarnings,
}: {
  shift: Shift;
  editable: boolean;
  warnings: Warning[];
  nameOf: (id: string) => string;
  onOpenSeat: () => void;
  onPerson: (assignmentId: string, userId: string) => void;
  onWarnings: () => void;
}) {
  const { t } = useTranslation();
  const { colors } = useTheme();
  const { filled, required, open } = coverage(shift);
  return (
    <Pressable
      testID="roster-block"
      {...webProps({ dataSet: { shiftId: shift.id, status: shift.status, editable: String(editable) } })}
      onPress={editable ? onOpenSeat : undefined}
      style={[styles.block, { borderColor: shift.status === "published" ? colors.border.strong : colors.accent.default, backgroundColor: colors.bg.surface }]}
    >
      <View style={styles.blockHead}>
        <Txt role="meta" numeric>
          {/* formatTimeRange marks a later end with "+1" itself, inside the isolate. */}
          {formatTimeRange(shift.starts_at, shift.ends_at, shift.timezone)}
        </Txt>
        <Txt role="meta" numeric testID="block-coverage">
          {`${filled}/${required}`}
        </Txt>
        {warnings.length > 0 ? (
          <Pressable testID="block-warning-badge" role="button" accessibilityLabel={t("roster.warnings", { count: warnings.length })} onPress={onWarnings} style={[styles.badge, { backgroundColor: colors.status.warning }]}>
            <Txt role="meta" style={{ color: colors.inverse.text }}>
              {String(warnings.length)}
            </Txt>
          </Pressable>
        ) : null}
      </View>
      {shift.assignments.map((a) => {
        const bar = barGeometry(shift, a);
        return (
          <Pressable
            key={a.id}
            testID="block-person"
            {...webProps({ dataSet: { userId: a.user_id, ownTimes: String(a.own_times), locked: String(!!a.locked) } })}
            onPress={() => onPerson(a.id, a.user_id)}
            disabled={!editable}
            style={styles.person}
          >
            <View style={styles.nameRow}>
              {/* A lock later runs keep, shown to every Manager and Admin who sees the roster (FR-018). */}
              {a.locked ? <Icon name="lock" size={12} color={colors.text.secondary} label={t("roster.locked")} testID="person-locked" /> : null}
              <Txt role="meta" numberOfLines={1}>
                {nameOf(a.user_id)}
              </Txt>
            </View>
            <View style={styles.track}>
              <View
                testID="person-bar"
                {...OVERHANG}
                style={[styles.bar, { backgroundColor: colors.accent.default, marginStart: `${bar.offset}%`, width: `${bar.width}%` }]} />
            </View>
          </Pressable>
        );
      })}
      {Array.from({ length: open }, (_, i) => (
        <Pressable key={`seat-${i}`} testID="empty-seat" role="button" accessibilityLabel={t("roster.empty_seat")} onPress={editable ? onOpenSeat : undefined} disabled={!editable} style={[styles.seat, { borderColor: colors.border.subtle }]}>
          <Txt role="meta" tone="secondary">
            {t("roster.empty_seat")}
          </Txt>
        </Pressable>
      ))}
    </Pressable>
  );
}

type BusyOn = components["schemas"]["BusyAnswer"]["items"][number];
type Alert = components["schemas"]["AssignRequest"]["alert"];

/** Who hears of a manual edit: everyone on the roster, the people it affects, or no one (T042g). */
function AlertChoice({ value, onChange }: { value: Alert; onChange: (value: Alert) => void }) {
  const { t } = useTranslation();
  return (
    <SegmentedControl
      label={t("roster.alert_label")}
      value={value}
      onChange={onChange}
      options={[
        { value: "none", label: t("roster.alert_none") },
        { value: "affected", label: t("roster.alert_affected") },
        { value: "everyone", label: t("roster.alert_everyone") },
      ]}
      testID="edit-alert"
    />
  );
}

function AssignSheet({
  shift,
  people,
  busy,
  replacing,
  note,
  alert,
  onNote,
  onAlert,
  onClose,
  onChoose,
}: {
  shift: Shift | null;
  people: Person[];
  busy: BusyOn[];
  /** The name of the person being replaced, when the sheet replaces someone (T042f). */
  replacing: string | null;
  note: string;
  alert: Alert;
  onNote: (note: string) => void;
  onAlert: (alert: Alert) => void;
  onClose: () => void;
  onChoose: (userId: string, how: "assign" | "move", busyOn: BusyOn | null) => void;
}) {
  const { t, i18n } = useTranslation();
  const [query, setQuery] = useState("");
  // Someone already on another shift then, chosen: the sheet asks what to do (T042f).
  const [pick, setPick] = useState<{ person: Person; on: BusyOn } | null>(null);
  const taken = new Set(shift?.assignments.map((a) => a.user_id) ?? []);
  const busyBy = new Map(busy.map((b) => [b.user_id, b]));
  // Search by any part of the name, ignoring case and accents (T042c). Later the list shows only the
  // project's own or qualified people, and search still finds everyone.
  const wanted = fold(query, i18n.language);
  const options = people
    .filter((p) => !taken.has(p.id) && (wanted === "" || fold(p.full_name, i18n.language).includes(wanted)))
    .sort((a, b) => a.full_name.localeCompare(b.full_name, i18n.language));
  const close = () => {
    setQuery("");
    setPick(null);
    onClose();
  };
  const choose = (person: Person) => {
    const on = busyBy.get(person.id);
    if (on) {
      setPick({ person, on });
      return;
    }
    setQuery("");
    onChoose(person.id, "assign", null);
  };
  const done = (how: "assign" | "move") => {
    const chosen = pick!;
    setPick(null);
    setQuery("");
    onChoose(chosen.person.id, how, chosen.on);
  };
  const title = replacing ? t("roster.replace_title", { person: replacing }) : t("roster.assign_title");
  return (
    <Sheet open={shift !== null} onClose={close} title={title} testID="assign-sheet">
      {pick ? (
        <View style={styles.warning} testID="assign-busy-choice">
          <Txt role="body">
            {pick.on.starts_at && pick.on.ends_at
              ? t("roster.busy_at", { person: pick.person.full_name, time: formatTimeRange(pick.on.starts_at, pick.on.ends_at, DEFAULT_TIME_ZONE) })
              : t("roster.busy_then", { person: pick.person.full_name })}
          </Txt>
          {pick.on.movable ? (
            <Button
              variant="primary"
              label={replacing ? t("roster.swap_with", { person: pick.person.full_name, other: replacing }) : t("roster.move_here", { person: pick.person.full_name })}
              onPress={() => done("move")}
              testID="action-busy-move"
            />
          ) : null}
          <Button variant="secondary" label={t("roster.keep_both", { person: pick.person.full_name })} onPress={() => done("assign")} testID="action-busy-keep" />
          <Button variant="quiet" label={t("common.cancel")} onPress={() => setPick(null)} testID="action-busy-cancel" />
        </View>
      ) : (
        <>
          <TextField label={t("roster.assign_search")} value={query} onChangeText={setQuery} testID="assign-search" autoFocus />
          {options.length === 0 ? (
            <Txt role="body" tone="secondary" testID="assign-none">
              {t("roster.assign_none")}
            </Txt>
          ) : null}
          {options.map((person) => (
            <View key={person.id} style={styles.option}>
              <Button variant="quiet" icon="user" label={person.full_name} testID="assign-option" web={{ dataSet: { userId: person.id, busy: String(busyBy.has(person.id)) } }} onPress={() => choose(person)} />
              {busyBy.has(person.id) ? (
                <Txt role="meta" tone="warning" testID="assign-busy">
                  {t("roster.busy_mark")}
                </Txt>
              ) : null}
            </View>
          ))}
        </>
      )}
      {/* A word to the person placed, shown on their own shift page, and who hears of it (T042g). */}
      <TextField label={t("roster.note_label")} value={note} onChangeText={onNote} maxLength={500} testID="edit-note" />
      <AlertChoice value={alert} onChange={onAlert} />
      {previewsEnabled ? (
        <EmptyState testID="preview-qualification-expiry" icon="star" badge={t("preview.badge")} title={t("preview.qualification_expiry_title")} body={t("preview.qualification_expiry_body")} feature={t("preview.comes_with", { feature: "014" })} />
      ) : null}
    </Sheet>
  );
}

/** ``text`` for matching: lower case, accents and Hebrew points removed, spaces squeezed. */
function fold(text: string, language: string): string {
  return text.normalize("NFD").replace(/[\u0300-\u036f\u0591-\u05c7]/g, "").toLocaleLowerCase(language).replace(/\s+/g, " ").trim();
}

/** The text that says a warning's numbers, when the rule sends them. */
function numbersOf(w: Warning): string | null {
  const values = w.values ?? {};
  if ("rest_hours" in values) return "roster.warning.rest";
  if ("maximum_hours" in values) return "roster.warning.weekly";
  return null;
}

function WarningSheet({ shift, warnings, nameOf, onClose, onOverridden }: { shift: Shift | null; warnings: Warning[]; nameOf: (id: string) => string; onClose: () => void; onOverridden: (code: string, employee: string | null) => void }) {
  const { t } = useTranslation();
  const session = useSession();
  const errorText = useErrorText();
  const [overriding, setOverriding] = useState<Warning | null>(null);
  const [reason, setReason] = useState("");
  const confirm = async () => {
    if (!shift || !overriding) return;
    const assignment = shift.assignments.find((a) => a.user_id === overriding.employee_id);
    if (!assignment) return;
    try {
      await call(session.api.POST("/api/v1/assignments/{id}/overrides", { params: { path: { id: assignment.id } }, body: { warning_code: overriding.code, reason } }));
      onOverridden(overriding.code, overriding.employee_id);
      setOverriding(null);
      setReason("");
    } catch (failure) {
      showToast(errorText(codeOf(failure)), { tone: "warning" });
    }
  };
  return (
    <Sheet
      open={shift !== null}
      onClose={onClose}
      title={t("roster.warnings_title")}
      testID="warning-detail"
      footer={<Button variant="primary" label={t("roster.warnings_ok")} onPress={onClose} testID="action-warnings-ok" />}
    >
      {warnings.map((w, i) => (
        <View key={`${w.code}-${i}`} style={styles.warning}>
          <Txt role="label">{w.employee_id ? nameOf(w.employee_id) : t("roster.the_shift")}</Txt>
          <Txt role="body">{t(w.code)}</Txt>
          {numbersOf(w) ? (
            // The rule and the numbers, in the UI's language (FR-041).
            <Txt role="meta" tone="secondary" numeric>
              {t(numbersOf(w)!, w.values ?? {})}
            </Txt>
          ) : null}
          {w.employee_id ? <Button variant="secondary" label={t("roster.override")} testID="action-override" onPress={() => setOverriding(w)} /> : null}
        </View>
      ))}
      {overriding ? (
        <View style={styles.warning}>
          <TextField label={t("roster.override_reason")} value={reason} onChangeText={setReason} testID="override-reason" />
          <Button variant="primary" label={t("roster.confirm_override")} disabled={reason.trim() === ""} onPress={() => void confirm()} testID="action-confirm-override" />
        </View>
      ) : null}
    </Sheet>
  );
}

function PublishSheet({ open, project, first, last, onClose, onPublished }: { open: boolean; project: string; first: string; last: string; onClose: () => void; onPublished: () => Promise<void> }) {
  const { t } = useTranslation();
  const session = useSession();
  const errorText = useErrorText();
  const [notify, setNotify] = useState(true);
  const [busy, setBusy] = useState(false);
  const preview = useLoad(
    async () => (open ? call(session.api.GET("/api/v1/projects/{id}/publish-preview", { params: { path: { id: project }, query: { from: first, to: last } } })) : null),
    [open, project, first],
  );
  const publish = async () => {
    setBusy(true);
    try {
      await call(session.api.POST("/api/v1/projects/{id}/publish", { params: { path: { id: project } }, body: { from: first, to: last, notify } }));
      await onPublished();
    } catch (failure) {
      showToast(errorText(codeOf(failure)), { tone: "warning" });
    } finally {
      setBusy(false);
    }
  };
  const counts = preview.data;
  return (
    <Sheet open={open} onClose={onClose} title={t("roster.publish_title")} testID="publish-review">
      {counts === null ? (
        <SkeletonRows rows={3} />
      ) : (
        <View style={styles.counts}>
          <Count label={t("roster.review_shifts")} value={counts.shifts} testID="review-shifts" />
          <Count label={t("roster.review_people")} value={counts.people} testID="review-people" />
          <Count label={t("roster.review_unfilled")} value={counts.unfilled} testID="review-unfilled" />
          <Count label={t("roster.review_warnings")} value={counts.open_warnings} testID="review-warnings" />
        </View>
      )}
      <View role="radiogroup" accessibilityLabel={t("roster.notify_label")} style={styles.radios}>
        <Radio checked={notify} label={t("roster.notify_everyone")} testID="notify-everyone" onPress={() => setNotify(true)} />
        <Radio checked={!notify} label={t("roster.publish_silently")} testID="publish-silently" onPress={() => setNotify(false)} />
      </View>
      <Button variant="primary" icon="check" label={t("roster.confirm_publish")} busy={busy} disabled={counts === null} onPress={() => void publish()} testID="action-confirm-publish" />
    </Sheet>
  );
}

function Count({ label, value, testID }: { label: string; value: number; testID: string }) {
  return (
    <View style={styles.count}>
      <Txt role="meta" tone="secondary">
        {label}
      </Txt>
      <Txt role="section" numeric testID={testID}>
        {String(value)}
      </Txt>
    </View>
  );
}

function Radio({ checked, label, testID, onPress }: { checked: boolean; label: string; testID: string; onPress: () => void }) {
  const { colors } = useTheme();
  return (
    <Pressable role="radio" aria-checked={checked} accessibilityLabel={label} testID={testID} onPress={onPress} style={[styles.radio, { borderColor: checked ? colors.accent.default : colors.border.subtle }]}>
      <Txt role="label">{label}</Txt>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  nameRow: { flexDirection: "row", alignItems: "center", gap: 4 },
  option: { gap: 2 },
  allProjects: { minHeight: 44, justifyContent: "center", paddingHorizontal: 8 },
  week: { flexDirection: "row", gap: 8 },
  day: { width: 168, borderWidth: 1, padding: 6, gap: 6 },
  dayHead: { paddingBottom: 4 },
  row: { gap: 6, minHeight: 40 },
  block: { borderWidth: 1, borderStartWidth: 3, padding: 6, gap: 4 },
  blockHead: { flexDirection: "row", alignItems: "center", gap: 6, justifyContent: "space-between" },
  badge: { minWidth: 24, minHeight: 24, alignItems: "center", justifyContent: "center", paddingHorizontal: 4 },
  person: { gap: 2, minHeight: 32, justifyContent: "center" },
  track: { height: 6, overflow: "visible" },
  bar: { height: 6 },
  seat: { minHeight: 32, borderWidth: 1, borderStyle: "dashed", alignItems: "center", justifyContent: "center" },
  continuation: { borderWidth: 1, borderStyle: "dashed", padding: 6 },
  agenda: { gap: 16 },
  agendaDay: { gap: 8 },
  warning: { gap: 4, paddingVertical: 6 },
  counts: { flexDirection: "row", flexWrap: "wrap", gap: 16 },
  count: { gap: 2, minWidth: 96 },
  radios: { flexDirection: "row", flexWrap: "wrap", gap: 8 },
  radio: { minHeight: 44, borderWidth: 1, paddingHorizontal: 12, justifyContent: "center" },
});
