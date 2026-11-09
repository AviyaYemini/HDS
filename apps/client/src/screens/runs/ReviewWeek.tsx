/**
 * A proposal under review (specs/008-auto-scheduler-review US1, US2; FR-001 to FR-012). The week as
 * a change to the roster when the run was made: each person marked added, changed, removed or
 * unchanged, in words and an icon, never colour alone, with counts and each day's places filled
 * over needed. A proposal still open is accepted whole, by day, by project or by chosen pairs, and
 * the rest discarded; a person's menu assigns, unassigns, moves or swaps, and a rule break asks for
 * a reason before it is kept. Every edit and accept sends the version read, so two people never
 * overwrite each other; a stale one reads the proposal again.
 */
import { useTranslation } from "react-i18next";
import { View } from "react-native";

import { codeOf, useErrorText } from "../../api/errors";
import { Button, InlineBanner, Sheet, SkeletonRows, TextField, Txt } from "../../design/components";
import { useTheme } from "../../design/theme/ThemeProvider";
import { webProps } from "../../design/web";
import { formatHeaderDate } from "../../locale";
import { FairnessPanel } from "./FairnessPanel";
import { groupWarnings } from "../roster/model";
import { weeksOf, type Job, type Names } from "./model";
import { hasOpen, menuFor, hours, numbersKey, numbersOf, openDaysAndProjects, partOfPairs, reasonKey, slotsByDay, type ReviewSlot } from "./review";

import { styles } from "./reviewStyles";
import { EmptyReasons } from "./EmptyReasons";
import { SlotBlock } from "./SlotBlock";
import { useProposalReview } from "./useProposalReview";

interface Props {
  job: Job;
  names: Names;
  /** The roster's people, to assign from. */
  people: { id: string; full_name: string }[];
  /** After an accept or a discard, so the page reads the job again. */
  onDecided: () => void;
  /** Hears the proposal's version as it moves with each edit, so the record can follow (FR-030). */
  onVersion?: (version: number) => void;
}


export function ReviewWeek({ job, names, people, onDecided, onVersion }: Props) {
  const { t, i18n } = useTranslation();
  const { colors, isDesktop } = useTheme();
  const errorText = useErrorText();
  const {
    loaded,
    shown,
    busy,
    problem,
    chosen,
    personOn,
    setPersonOn,
    assignTo,
    setAssignTo,
    moving,
    setMoving,
    pending,
    reason,
    setReason,
    setAttempt,
    cover,
    accept,
    discardRest,
    closePanels,
    send,
    undo,
    toggle,
  } = useProposalReview(job, onDecided, onVersion);

  if (loaded.error && !shown) {
    return <InlineBanner tone="warning" title={errorText(loaded.error)} action={{ label: t("common.retry"), onPress: loaded.reload }} />;
  }
  if (!shown) return <SkeletonRows rows={4} />;

  const open = shown.state === "open";
  const name = (id: string) => names.people.get(id) ?? people.find((p) => p.id === id)?.full_name ?? t("runs.unknown_person");
  const placeName = (slot: ReviewSlot) =>
    [names.projects.get(slot.project_id), names.types.get(slot.shift_type_id), formatHeaderDate(slot.date, i18n.language)].filter(Boolean).join(" · ");

  const byDay = slotsByDay(shown);
  const empty = shown.slots.filter((slot) => slot.places > 0);
  // Bring the slot into view behind the sheet, then open its "Who else could cover" (T042b).
  const openSlot = (slot: ReviewSlot) => {
    if (typeof document !== "undefined") {
      // The slot's block, not the strip's own button, which carries the same key (R50).
      document.querySelector(`[data-testid="proposal-block"][data-slot-key="${CSS.escape(slot.key)}"]`)?.scrollIntoView({ block: "center" });
    }
    setAssignTo(slot);
  };
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
      <FairnessPanel
        jobId={job.id}
        version={shown.version}
        name={name}
        place={(id) => names.projects.get(id) ?? ""}
        type={(id) => names.types.get(id) ?? ""}
      />
      {empty.length > 0 ? (
        // The slots left with empty places, first and in red with an icon and words; each opens
        // its slot with "Who else could cover" (T042b, FR-014).
        <InlineBanner tone="danger" title={t("review.empty_title", { count: empty.length })} testID="review-empty">
          {empty.map((slot) => (
            // Its words wrap at phone width, where a long place name would not fit on one line.
            <Button
              key={slot.key}
              variant="quiet"
              wrap
              label={t("review.empty_slot", { place: placeName(slot), count: slot.places })}
              disabled={!open || busy !== null}
              onPress={() => openSlot(slot)}
              testID="review-empty-slot"
              web={{ dataSet: { slotKey: slot.key } }}
            />
          ))}
        </InlineBanner>
      ) : null}
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
      {/* Only the weeks that hold a slot of the run: calendar weeks with none add nothing (T042a). */}
      {weeksOf(job.from, job.to)
        .filter((days) => days.some((day) => byDay.has(day)))
        .map((days) => {
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
            {/* One row per person and rule, as on the roster: one reason covers them all. */}
            {groupWarnings(pending.breaks, (w) => w.user_id ?? null).map(({ first: w, values }) => (
              <View key={`${w.user_id ?? "shift"}-${w.code}`} style={styles.break} testID="review-break">
                <Txt role="label">{w.user_id ? name(w.user_id) : t("roster.the_shift")}</Txt>
                <Txt role="body">{t(w.code)}</Txt>
                {numbersKey(w.values)
                  ? values.map((v) => (
                      <Txt key={JSON.stringify(v)} role="meta" tone="secondary" numeric>
                        {t(numbersKey(v)!, v)}
                      </Txt>
                    ))
                  : null}
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
            <Button
              variant="secondary"
              icon="lock"
              label={personOn.slot.people.find((p) => p.user_id === personOn.userId)?.locked ? t("review.unlock") : t("review.lock")}
              testID="action-review-lock"
              onPress={() => {
                const locked = personOn.slot.people.find((p) => p.user_id === personOn.userId)?.locked;
                void send({ op: locked ? "unlock" : "lock", key: personOn.slot.key, user_id: personOn.userId });
              }}
            />
          </View>
        ) : null}
      </Sheet>
    </View>
  );
}
