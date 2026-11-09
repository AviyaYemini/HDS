/**
 * A proposal under review (specs/008-auto-scheduler-review US1, US2; FR-001 to FR-012). The week as
 * a change to the roster when the run was made: each person marked added, changed, removed or
 * unchanged, in words and an icon, never colour alone, with counts and each day's places filled
 * over needed. A proposal still open is accepted whole, by day, by project or by chosen pairs, and
 * the rest discarded; a person's menu assigns, unassigns, moves or swaps, and a rule break asks for
 * a reason before it is kept. Every edit and accept sends the version read, so two people never
 * overwrite each other; a stale one reads the proposal again.
 */
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { call, codeOf, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { Button, InlineBanner, Sheet, SkeletonRows, TextField, Txt, showToast } from "../../design/components";
import { Icon } from "../../design/icons/Icon";
import { useTheme } from "../../design/theme/ThemeProvider";
import { webProps } from "../../design/web";
import { DEFAULT_TIME_ZONE, formatHeaderDate, formatTimeRange } from "../../locale";
import { useLoad } from "../useLoad";
import { weeksOf, type Job, type Names } from "./model";
import {
  MARK_ICON,
  breaksOf,
  hasOpen,
  menuFor,
  hours,
  numbersKey,
  numbersOf,
  openDaysAndProjects,
  openMarks,
  pairsOf,
  partOfPairs,
  reasonKey,
  slotsByDay,
  type AcceptPart,
  type Cover,
  type Edit,
  type EditWarning,
  type Pair,
  type Review,
  type ReviewSlot,
} from "./review";

interface Props {
  job: Job;
  names: Names;
  /** The roster's people, to assign from. */
  people: { id: string; full_name: string }[];
  /** After an accept or a discard, so the page reads the job again. */
  onDecided: () => void;
}

interface PersonOn {
  slot: ReviewSlot;
  userId: string;
}

type Pending = { edit: Edit; breaks: EditWarning[] };

export function ReviewWeek({ job, names, people, onDecided }: Props) {
  const { t, i18n } = useTranslation();
  const { colors, isDesktop } = useTheme();
  const session = useSession();
  const errorText = useErrorText();
  const loaded = useLoad(
    async () => call(session.api.GET("/api/v1/schedule-jobs/{id}/proposal", { params: { path: { id: job.id } } })),
    [job.id, job.proposal_state],
  );
  const [review, setReview] = useState<Review | null>(null);
  const shown = review ?? loaded.data ?? null;
  const [busy, setBusy] = useState<string | null>(null);
  const [problem, setProblem] = useState<string | null>(null);
  const [chosen, setChosen] = useState<Pair[]>([]);
  const [personOn, setPersonOn] = useState<PersonOn | null>(null);
  const [assignTo, setAssignTo] = useState<ReviewSlot | null>(null);
  const [moving, setMoving] = useState<{ from: PersonOn; swap: boolean } | null>(null);
  const [pending, setPending] = useState<Pending | null>(null);
  const [reason, setReason] = useState("");
  // Who else could cover the slot being assigned to (FR-014): read each time the sheet opens on a
  // slot, since the working copy changes with every edit.
  const [fetched, setFetched] = useState<{ key: string; version: number; attempt: number; data: Cover | null; error: unknown } | null>(null);
  const [attempt, setAttempt] = useState(0);
  const shownVersion = review?.version ?? loaded.data?.version ?? -1;
  useEffect(() => {
    if (assignTo === null) return;
    let live = true;
    call(session.api.GET("/api/v1/schedule-jobs/{id}/slots/{key}/cover", { params: { path: { id: job.id, key: assignTo.key } } }))
      .then((data) => live && setFetched({ key: assignTo.key, version: shownVersion, attempt, data, error: null }))
      .catch((error: unknown) => live && setFetched({ key: assignTo.key, version: shownVersion, attempt, data: null, error }));
    return () => {
      live = false;
    };
  }, [assignTo, job.id, session.api, shownVersion, attempt]);
  // The answer for the slot open now, read on the version shown; anything else is still loading.
  const cover = assignTo && fetched?.key === assignTo.key && fetched.version === shownVersion && fetched.attempt === attempt ? fetched : null;

  if (loaded.error && !shown) {
    return <InlineBanner tone="warning" title={errorText(loaded.error)} action={{ label: t("common.retry"), onPress: loaded.reload }} />;
  }
  if (!shown) return <SkeletonRows rows={4} />;

  const open = shown.state === "open";
  const version = `"${shown.version}"`;
  const name = (id: string) => names.people.get(id) ?? people.find((p) => p.id === id)?.full_name ?? t("runs.unknown_person");
  const placeName = (slot: ReviewSlot) =>
    [names.projects.get(slot.project_id), names.types.get(slot.shift_type_id), formatHeaderDate(slot.date, i18n.language)].filter(Boolean).join(" · ");

  const stale = (code: string) => {
    if (code === "schedule.version_stale") {
      setReview(null);
      loaded.reload();
    }
  };

  const accept = async (part: AcceptPart | null, label: string) => {
    setBusy(label);
    setProblem(null);
    try {
      const answer = await call(
        session.api.POST("/api/v1/schedule-jobs/{id}/accept", {
          params: { path: { id: job.id }, header: { "If-Match": version } },
          ...(part ? { body: part } : {}),
        }),
      );
      showToast(t("runs.accepted", { count: answer.shifts_created + answer.shifts_updated }), { tone: "success" });
      setChosen([]);
      setReview(null);
      loaded.reload();
      onDecided();
    } catch (failure) {
      setProblem(codeOf(failure));
      stale(codeOf(failure));
    } finally {
      setBusy(null);
    }
  };

  const discardRest = async () => {
    setBusy("discard");
    setProblem(null);
    try {
      await call(session.api.POST("/api/v1/schedule-jobs/{id}/discard", { params: { path: { id: job.id }, header: { "If-Match": version } } }));
      setReview(null);
      loaded.reload();
      onDecided();
    } catch (failure) {
      setProblem(codeOf(failure));
      stale(codeOf(failure));
    } finally {
      setBusy(null);
    }
  };

  const closePanels = () => {
    setPersonOn(null);
    setAssignTo(null);
    setMoving(null);
    setPending(null);
    setReason("");
  };

  const send = async (edit: Edit) => {
    setBusy("edit");
    setProblem(null);
    let outcome;
    try {
      outcome = await session.api.POST("/api/v1/schedule-jobs/{id}/edits", {
        params: { path: { id: job.id }, header: { "If-Match": version } },
        body: edit,
      });
    } catch {
      // No answer at all (the network): say so, and leave every button usable (R7).
      closePanels();
      setProblem("errors.network");
      return;
    } finally {
      setBusy(null);
    }
    if (outcome.response.ok && outcome.data) {
      setReview(outcome.data.proposal);
      closePanels();
      const kept = outcome.data.warnings.filter((w) => !w.excused && w.code === "warnings.close_preference");
      if (kept.length > 0) showToast(t("warnings.close_preference"), { tone: "warning" });
      return;
    }
    const code = codeOf(outcome.error);
    if (code === "schedule.override_reason_required") {
      // The same sheet turns to the reason: one sheet open at a time, never one closing under another.
      setPending({ edit, breaks: breaksOf(outcome.error) });
      return;
    }
    closePanels();
    setProblem(code);
    stale(code);
  };

  const undo = async () => {
    setBusy("undo");
    setProblem(null);
    try {
      setReview(await call(session.api.POST("/api/v1/schedule-jobs/{id}/edits/undo", { params: { path: { id: job.id }, header: { "If-Match": version } } })));
    } catch (failure) {
      setProblem(codeOf(failure));
      stale(codeOf(failure));
    } finally {
      setBusy(null);
    }
  };

  const toggle = (slot: ReviewSlot, mark: ReviewSlot["marks"][number]) => {
    const pairs = pairsOf(slot, mark);
    const has = chosen.some((p) => p.key === slot.key && p.userId === mark.user_id);
    setChosen((now) => (has ? now.filter((p) => !(p.key === slot.key && pairs.some((q) => q.userId === p.userId))) : [...now, ...pairs]));
  };

  const byDay = slotsByDay(shown);
  const { days: openDays, projects: openProjects } = openDaysAndProjects(shown);
  const anyOpen = open && hasOpen(shown);
  const showBar = open;
  const marksText = t("review.counts", shown.counts);

  return (
    <View style={styles.weeks} testID="proposal-review" {...webProps({ dataSet: { version: String(shown.version) } })}>
      <Txt role="section" heading={2}>
        {t("runs.proposal_title")}
      </Txt>
      <Txt role="body" numeric testID="review-counts">
        {marksText}
      </Txt>
      {problem ? <InlineBanner tone="warning" testID="review-problem" title={errorText(problem)} /> : null}
      {showBar ? (
        <View style={styles.bar} testID="accept-bar">
          <Button variant="primary" icon="check" label={t("review.accept_all")} busy={busy === "all"} disabled={busy !== null} onPress={() => void accept(null, "all")} testID="action-accept" />
          {openProjects.length > 1
            ? openProjects.map((project) => (
                <Button
                  key={project}
                  label={t("review.accept_project", { project: names.projects.get(project) ?? "" })}
                  busy={busy === project}
                  disabled={busy !== null}
                  onPress={() => void accept({ project_ids: [project] }, project)}
                  testID="action-accept-project"
                  web={{ dataSet: { projectId: project } }}
                />
              ))
            : null}
          <Button
            label={t("review.accept_selected", { count: chosen.length })}
            disabled={busy !== null || chosen.length === 0}
            disabledReason={chosen.length === 0 ? t("review.choose_first") : undefined}
            busy={busy === "selected"}
            onPress={() => void accept(partOfPairs(chosen), "selected")}
            testID="action-accept-selected"
          />
          <Button variant="quiet" icon="undo" label={t("review.undo")} busy={busy === "undo"} disabled={busy !== null} onPress={() => void undo()} testID="action-undo" />
          <Button label={t("review.discard_rest")} busy={busy === "discard"} disabled={busy !== null} onPress={() => void discardRest()} testID="action-discard" />
        </View>
      ) : null}
      {weeksOf(job.from, job.to).map((days) => {
        const visible = isDesktop ? days : days.filter((day) => byDay.has(day));
        return (
          <View key={days[0]} testID="proposal-week" style={[styles.week, isDesktop ? styles.weekRow : null]}>
            {visible.map((day) => {
              const inRange = day >= job.from && day <= job.to;
              const cover = shown.coverage.filter((c) => c.date === day);
              const filled = cover.reduce((n, c) => n + c.filled, 0);
              const needed = cover.reduce((n, c) => n + c.needed, 0);
              return (
                <View
                  key={day}
                  testID="proposal-day"
                  {...webProps({ dataSet: { date: day } })}
                  style={[isDesktop ? styles.column : styles.dayStack, { borderColor: inRange ? colors.border.strong : colors.border.subtle }]}
                >
                  <Txt role="label" tone={inRange ? "primary" : "secondary"}>
                    {formatHeaderDate(day, i18n.language)}
                  </Txt>
                  {needed > 0 ? (
                    <Txt role="meta" tone={filled < needed ? "warning" : "secondary"} numeric testID="day-coverage">
                      {t("runs.filled", { filled, required: needed })}
                    </Txt>
                  ) : null}
                  {anyOpen && openDays.includes(day) ? (
                    <Button
                      variant="quiet"
                      icon="check"
                      label={t("review.accept_day")}
                      busy={busy === day}
                      disabled={busy !== null}
                      onPress={() => void accept({ days: [day] }, day)}
                      testID="action-accept-day"
                      web={{ dataSet: { date: day } }}
                    />
                  ) : null}
                  {(byDay.get(day) ?? []).map((slot) => (
                    <SlotBlock
                      key={slot.key}
                      slot={slot}
                      names={names}
                      name={name}
                      editable={open && menuFor(shown, slot).canEdit}
                      selectable={anyOpen}
                      chosen={chosen}
                      onToggle={(mark) => toggle(slot, mark)}
                      onPerson={(userId) => setPersonOn({ slot, userId })}
                      onAssign={() => setAssignTo(slot)}
                    />
                  ))}
                </View>
              );
            })}
          </View>
        );
      })}
      <Sheet
        open={personOn !== null || moving !== null || assignTo !== null || pending !== null}
        onClose={closePanels}
        title={
          pending
            ? t("review.override_title")
            : moving
              ? moving.swap
                ? t("review.swap_title")
                : t("review.move_title")
              : assignTo
                ? t("roster.assign_title")
                : personOn
                  ? name(personOn.userId)
                  : ""
        }
        footer={
          pending ? (
            <Button
              variant="primary"
              label={t("review.keep_anyway")}
              disabled={reason.trim() === "" || busy !== null}
              busy={busy === "edit"}
              onPress={() => void send({ ...pending.edit, override_reason: reason.trim() })}
              testID="action-keep-anyway"
            />
          ) : undefined
        }
      >
        {pending ? (
          <View testID="review-override-sheet" style={styles.panel}>
            {pending.breaks.map((w, i) => (
              <View key={`${w.code}-${i}`} style={styles.break} testID="review-break">
                <Txt role="label">{w.user_id ? name(w.user_id) : t("roster.the_shift")}</Txt>
                <Txt role="body">{t(w.code)}</Txt>
                {numbersKey(w.values) ? (
                  <Txt role="meta" tone="secondary" numeric>
                    {t(numbersKey(w.values)!, w.values)}
                  </Txt>
                ) : null}
              </View>
            ))}
            <TextField label={t("roster.override_reason")} value={reason} onChangeText={setReason} maxLength={500} testID="review-override-reason" />
          </View>
        ) : moving ? (
          <View testID="review-move-sheet" style={styles.panel}>
            {menuFor(shown, moving.from.slot).targets.flatMap((target) =>
              moving.swap
                ? target.people
                    .filter((p) => !moving.from.slot.people.some((q) => q.user_id === p.user_id))
                    .map((p) => (
                      <Button
                        key={`${target.key}-${p.user_id}`}
                        variant="quiet"
                        label={t("review.swap_with", { person: name(p.user_id), place: placeName(target) })}
                        testID="review-swap-option"
                        web={{ dataSet: { slotKey: target.key, userId: p.user_id } }}
                        onPress={() => void send({ op: "swap", key: moving.from.slot.key, user_id: moving.from.userId, to_key: target.key, other_user_id: p.user_id })}
                      />
                    ))
                : target.people.some((p) => p.user_id === moving.from.userId)
                  ? []
                  : [
                      <Button
                        key={target.key}
                        variant="quiet"
                        label={placeName(target)}
                        testID="review-move-option"
                        web={{ dataSet: { slotKey: target.key } }}
                        onPress={() => void send({ op: "move", key: moving.from.slot.key, user_id: moving.from.userId, to_key: target.key })}
                      />,
                    ],
            )}
          </View>
        ) : assignTo ? (
          <View testID="review-assign-sheet" style={styles.panel}>
            {assignTo.places > 0 ? <EmptyReasons review={shown} slot={assignTo} name={name} /> : null}
            <Txt role="label" heading={3}>
              {t("review.cover_title")}
            </Txt>
            {cover?.error ? (
              // The ranked list could not be read: say so, offer it again, and still let the
              // manager pick from the staff, the rules then checked by the edit itself (R24).
              <View style={styles.panel}>
                <InlineBanner tone="warning" title={errorText(codeOf(cover.error))} action={{ label: t("common.retry"), onPress: () => setAttempt((n) => n + 1) }} />
                {people
                  .filter((p) => !assignTo.people.some((q) => q.user_id === p.id))
                  .map((person) => (
                    <Button
                      key={person.id}
                      variant="quiet"
                      icon="user"
                      label={person.full_name}
                      testID="review-assign-option"
                      web={{ dataSet: { userId: person.id } }}
                      onPress={() => void send({ op: "assign", key: assignTo.key, user_id: person.id })}
                    />
                  ))}
              </View>
            ) : !cover?.data ? (
              <SkeletonRows rows={3} />
            ) : (
              cover.data.options.map((option) => {
                const numbers = numbersOf(option, i18n.language);
                const totals = cover.data!.totals;
                return (
                  <View key={option.user_id} style={styles.option} testID="review-cover-option" {...webProps({ dataSet: { userId: option.user_id, free: String(option.free) } })}>
                    <Button
                      variant="quiet"
                      icon="user"
                      label={option.free ? name(option.user_id) : t("review.assign_anyway_person", { person: name(option.user_id) })}
                      testID="review-assign-option"
                      web={{ dataSet: { userId: option.user_id, free: String(option.free) } }}
                      onPress={() => void send({ op: "assign", key: assignTo.key, user_id: option.user_id })}
                    />
                    <Txt role="meta" tone={option.free ? "secondary" : "warning"}>
                      {option.free
                        ? t(option.close ? "review.cover_would_rather_not" : "review.cover_free")
                        : [t(reasonKey(option.code ?? "", (key) => i18n.exists(key))), numbers ? t(numbers.key, numbers.values) : null].filter(Boolean).join(" · ")}
                    </Txt>
                    <Txt role="meta" tone="secondary" numeric>
                      {t("review.cover_load", {
                        hours: hours(option.minutes, i18n.language),
                        nights: option.nights,
                        weekends: option.weekends,
                        average_hours: hours(totals.people > 0 ? totals.minutes / totals.people : 0, i18n.language),
                      })}
                    </Txt>
                  </View>
                );
              })
            )}
          </View>
        ) : personOn ? (
          <View testID="review-person-sheet" style={styles.panel}>
            <Button variant="secondary" icon="close" label={t("review.unassign")} testID="action-review-unassign" onPress={() => void send({ op: "unassign", key: personOn.slot.key, user_id: personOn.userId })} />
            <Button variant="secondary" icon="arrow-next" label={t("review.move")} testID="action-review-move" onPress={() => { setMoving({ from: personOn, swap: false }); setPersonOn(null); }} />
            <Button variant="secondary" icon="people" label={t("review.swap")} testID="action-review-swap" onPress={() => { setMoving({ from: personOn, swap: true }); setPersonOn(null); }} />
          </View>
        ) : null}
      </Sheet>
    </View>
  );
}

/** Why a slot of the run stayed empty, from the run's own reasons, with the people they name. */
function EmptyReasons({ review, slot, name }: { review: Review; slot: ReviewSlot; name: (id: string) => string }) {
  const { t, i18n } = useTranslation();
  const gap = (review.unfilled as unknown as RunGap[]).find((u) => u.key === slot.key);
  if (!gap) return null;
  return (
    <View testID="review-why-empty" style={styles.panel}>
      <Txt role="label" heading={3}>
        {t("review.why_empty")}
      </Txt>
      {gap.reasons.map((reason) => {
        const words = t(reasonKey(reason.code, (key) => i18n.exists(key)));
        return (
          <View key={reason.code}>
            <Txt role="meta" tone="secondary">
              {reason.employees > 0 ? t("runs.reason", { reason: words, count: reason.employees }) : words}
            </Txt>
            {(reason.people ?? []).map((person) => {
              const numbers = numbersOf(person, i18n.language);
              return (
                <Txt key={person.user_id} role="meta" tone="secondary" numeric>
                  {numbers ? t("runs.person_numbers", { person: name(person.user_id), numbers: t(numbers.key, numbers.values) }) : name(person.user_id)}
                </Txt>
              );
            })}
          </View>
        );
      })}
    </View>
  );
}

/** One empty slot of the run as the job stores it (the review sends it as it is). */
interface RunGap {
  key: string;
  places: number;
  reasons: { code: string; employees: number; people?: ({ user_id: string } & Parameters<typeof numbersOf>[0])[] }[];
}

interface BlockProps {
  slot: ReviewSlot;
  names: Names;
  name: (id: string) => string;
  editable: boolean;
  selectable: boolean;
  chosen: Pair[];
  onToggle: (mark: ReviewSlot["marks"][number]) => void;
  onPerson: (userId: string) => void;
  onAssign: () => void;
}

function SlotBlock({ slot, names, name, editable, selectable, chosen, onToggle, onPerson, onAssign }: BlockProps) {
  const { t } = useTranslation();
  const { colors } = useTheme();
  const open = new Set(openMarks(slot).map((m) => m.user_id));
  const person = new Map(slot.people.map((p) => [p.user_id, p]));
  return (
    <View
      testID="proposal-block"
      {...webProps({ dataSet: { slotKey: slot.key, projectId: slot.project_id } })}
      style={[styles.block, { borderColor: colors.border.subtle, backgroundColor: colors.bg.surface }]}
    >
      <Txt role="label" numberOfLines={2}>
        {[names.projects.get(slot.project_id), names.types.get(slot.shift_type_id)].filter(Boolean).join(" · ")}
      </Txt>
      <Txt role="meta" tone="secondary" numeric>
        {formatTimeRange(slot.starts_at, slot.ends_at, DEFAULT_TIME_ZONE)}
      </Txt>
      <Txt role="meta" tone={slot.places > 0 ? "warning" : "secondary"} numeric>
        {t("runs.filled", { filled: slot.headcount - slot.places, required: slot.headcount })}
      </Txt>
      {slot.marks.map((mark) => {
        const who = person.get(mark.user_id);
        const picked = chosen.some((p) => p.key === slot.key && p.userId === mark.user_id);
        const label = mark.mark === "changed" && mark.replaces ? t("review.mark.changed_from", { person: name(mark.replaces) }) : t(`review.mark.${mark.mark}`);
        return (
          <View key={`${mark.user_id}-${mark.mark}`} style={styles.person} testID="proposal-person" {...webProps({ dataSet: { userId: mark.user_id, change: mark.mark, accepted: String(mark.accepted) } })}>
            {selectable && open.has(mark.user_id) ? (
              <Button
                variant="quiet"
                icon={picked ? "check" : "plus"}
                label={picked ? t("review.chosen") : t("review.choose")}
                accessibilityLabel={t(picked ? "review.chosen_pair" : "review.choose_pair", { person: name(mark.user_id) })}
                onPress={() => onToggle(mark)}
                testID="action-choose-pair"
              />
            ) : null}
            <Icon name={MARK_ICON[mark.mark]} size={14} color={mark.mark === "removed" ? colors.text.secondary : colors.text.primary} />
            {editable && mark.mark !== "removed" ? (
              <Button variant="quiet" label={name(mark.user_id)} onPress={() => onPerson(mark.user_id)} testID="review-person" accessibilityHint={t("review.person_hint")} />
            ) : (
              <Txt role="meta" tone={mark.mark === "removed" ? "secondary" : "primary"} style={mark.mark === "removed" ? styles.removed : null}>
                {name(mark.user_id)}
              </Txt>
            )}
            {mark.mark === "unchanged" ? null : (
              <Txt role="meta" tone="secondary">
                {label}
              </Txt>
            )}
            {who?.by === "person" ? (
              <Txt role="meta" tone="secondary" testID="mark-by-person">
                {t("review.by_person")}
              </Txt>
            ) : null}
            {who?.override ? (
              <Txt role="meta" tone="warning" testID="mark-override">
                {t("review.overridden")}
              </Txt>
            ) : null}
            {mark.accepted && mark.mark !== "unchanged" ? (
              <Txt role="meta" tone="success" testID="mark-accepted">
                {t("review.accepted")}
              </Txt>
            ) : null}
          </View>
        );
      })}
      {/* Staff the run could not place but who stay on the draft (a Manager placed by hand, say):
          they fill places, so they are shown, unchanged and outside the proposal's edits. */}
      {slot.kept.map((userId) => (
        <View key={`kept-${userId}`} style={styles.person} testID="proposal-kept" {...webProps({ dataSet: { userId } })}>
          <Icon name={MARK_ICON.unchanged} size={14} color={colors.text.primary} />
          <Txt role="meta">{name(userId)}</Txt>
        </View>
      ))}
      {editable ? <Button variant="quiet" icon="plus" label={t("review.assign")} onPress={onAssign} testID="action-review-assign" /> : null}
    </View>
  );
}

const styles = StyleSheet.create({
  option: { gap: 2 },
  weeks: { gap: 16 },
  bar: { flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 8 },
  week: { gap: 12 },
  weekRow: { flexDirection: "row", alignItems: "flex-start", gap: 8 },
  column: { flexGrow: 1, flexShrink: 1, flexBasis: 0, minWidth: 0, gap: 8, paddingTop: 8, borderTopWidth: 1 },
  dayStack: { gap: 8, paddingTop: 8, borderTopWidth: 1 },
  block: { gap: 4, padding: 8, borderWidth: 1 },
  person: { flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 4 },
  removed: { textDecorationLine: "line-through" },
  break: { gap: 4 },
  panel: { gap: 8 },
});
