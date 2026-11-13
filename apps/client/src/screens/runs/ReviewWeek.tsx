/**
 * A proposal under review (specs/008-auto-scheduler-review US1, US2; FR-001 to FR-012). The week as
 * a change to the roster when the run was made: each person marked added, changed, removed or
 * unchanged, in words and an icon, never colour alone, with counts and each day's places filled
 * over needed. A proposal still open is accepted whole, by day, by project or by chosen pairs, and
 * the rest discarded; a person's menu assigns, unassigns, moves or swaps, each stored as made (no
 * rule warnings: out of scope, the owner, 2026-11-13). Every edit and accept sends the version read, so two people never
 * overwrite each other; a stale one reads the proposal again.
 */
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { ScrollView, StyleSheet, View } from "react-native";

import { call, codeOf, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { Button, InlineBanner, Sheet, SkeletonRows, Txt } from "../../design/components";
import { useTheme } from "../../design/theme/ThemeProvider";
import { webProps } from "../../design/web";
import { nameText, formatHeaderDate, formatNumericRange } from "../../locale";
import { Disclosure, KeyFigures, SectionHead, type KeyFigure } from "../deck/parts";
import { useEntrance } from "../deck/useEntrance";
import { useLoad } from "../useLoad";
import { HonestyLabel } from "./HonestyLabel";
import { peoplePlaced, proposalSheet, scopeOf, solveSeconds } from "./runSummary";
import { FairnessPanel } from "./FairnessPanel";
import { coveredText, weeksOf, type Job, type Names } from "./model";
import { hasOpen, menuFor, hours, openDaysAndProjects, partOfPairs, slotsByDay, type ReviewSlot } from "./review";

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
  const session = useSession();
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
    setAttempt,
    cover,
    accept,
    discardRest,
    closePanels,
    send,
    undo,
    toggle,
  } = useProposalReview(job, onDecided, onVersion);
  // The projects' clients and types: the scope line and the week as the roster sheet.
  const projects = useLoad(async () => (await call(session.api.GET("/api/v1/projects"))).items, []);
  const clients = useLoad(async () => (await call(session.api.GET("/api/v1/clients"))).items, []);
  const [byProject, setByProject] = useState(false);
  const [sheetWidth, setSheetWidth] = useState(0);
  const progress = useEntrance(shown ? `${job.id}|${shown.version}` : null);

  if (loaded.error && !shown) {
    return <InlineBanner tone="warning" title={errorText(loaded.error)} action={{ label: t("common.retry"), onPress: loaded.reload }} />;
  }
  if (!shown) return <SkeletonRows rows={4} />;

  const open = shown.state === "open";
  const name = (id: string) => names.people.get(id) ?? people.find((p) => p.id === id)?.full_name ?? t("runs.unknown_person");
  const placeName = (slot: ReviewSlot) =>
    [names.projects.get(slot.project_id), names.types.get(slot.shift_type_id)].filter((x): x is string => Boolean(x)).map(nameText).concat(formatHeaderDate(slot.date, i18n.language)).join(" · ");

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
  // Filled over needed from the review's slots: each slot's places are the ones still empty.
  const count = { filled: shown.slots.reduce((n, sl) => n + (sl.headcount - sl.places), 0), required: shown.slots.reduce((n, sl) => n + sl.headcount, 0) };
  const seconds = solveSeconds(job);
  const scope = scopeOf(job.project_ids ?? [], (projects.data ?? []).map((p) => ({ id: p.id, name: p.name, type: p.type })));
  const range = formatNumericRange(job.from, job.to);
  const scopeText = scope.all
    ? t("runsum.scope.all", { range })
    : scope.names
      ? t("runsum.scope.named", { names: scope.names.join(", "), range })
      : t(`runsum.scope.${scope.type}`, { count: scope.count, range });
  const figures: KeyFigure[] = [
    {
      key: "filled",
      label: t("runsum.figure.filled"),
      value: `${count.filled}/${count.required}`,
      valueTestID: "run-filled",
      chip: <HonestyLabel job={job} />,
      detail: count.required - count.filled > 0 ? t("runsum.figure.filled_detail", { count: count.required - count.filled }) : t("runsum.figure.filled_full"),
    },
    {
      key: "changes",
      testID: "review-counts",
      label: t("runsum.figure.changes"),
      value: String(shown.counts.added),
      unit: t("runsum.added"),
      count: { to: shown.counts.added, format: (n: number) => String(Math.round(n)) },
      detail: t("runsum.figure.changes_detail", { changed: shown.counts.changed, removed: shown.counts.removed, unchanged: shown.counts.unchanged }),
    },
    {
      key: "people",
      label: t("runsum.figure.people"),
      value: String(peoplePlaced(shown.slots)),
      count: { to: peoplePlaced(shown.slots), format: (n: number) => String(Math.round(n)) },
      detail: t("runsum.figure.people_detail", { count: people.length }),
    },
    { key: "time", label: t("runsum.figure.time"), value: seconds === null ? "—" : t("runsum.figure.seconds", { count: seconds }) },
  ];

  const projectButtons =
    showBar && byProject && openProjects.length > 1 ? <View style={styles.bar}>{openProjects.map((project) => (
                <Button
                  key={project}
                  label={t("review.accept_project", { project: names.projects.get(project) ?? "" })}
                  busy={busy === project}
                  disabled={busy !== null}
                  onPress={() => void accept({ project_ids: [project] }, project)}
                  testID="action-accept-project"
                  web={{ dataSet: { projectId: project } }}
                />
              ))}</View> : null;
  const actionBar = showBar ? (
        <View testID="accept-bar" style={styles.bar}>
          <Button variant="primary" icon="check" label={t("review.accept_all")} busy={busy === "all"} disabled={busy !== null} onPress={() => void accept(null, "all")} testID="action-accept" />
          {openProjects.length > 1 ? (
            // One project at a time behind a toggle, not a wall of buttons (the owner, 2026-11-13).
            <Button
              variant="quiet"
              icon={byProject ? "chevron-up" : "chevron-down"}
              label={t("runsum.accept_by_project", { count: openProjects.length })}
              onPress={() => setByProject(!byProject)}
              testID="action-accept-by-project"
              web={{ "aria-expanded": String(byProject) }}
            />
          ) : null}
          {!isDesktop ? (
          <Button
            label={t("review.accept_selected", { count: chosen.length })}
            disabled={busy !== null || chosen.length === 0}
            disabledReason={chosen.length === 0 ? t("review.choose_first") : undefined}
            busy={busy === "selected"}
            onPress={() => void accept(partOfPairs(chosen), "selected")}
            testID="action-accept-selected"
          />
          ) : null}
          <Button variant="quiet" icon="undo" label={t("review.undo")} busy={busy === "undo"} disabled={busy !== null} onPress={() => void undo()} testID="action-undo" />
          <Button label={t("review.discard_rest")} busy={busy === "discard"} disabled={busy !== null} onPress={() => void discardRest()} testID="action-discard" />
        </View>
  ) : null;

  return (
    <View style={styles.weeks} testID="proposal-review" {...webProps({ dataSet: { version: String(shown.version) } })}>
      {/* The run at a glance, as the deck draws figures (the owner, 2026-11-13). */}
      <KeyFigures figures={figures} progress={progress} />
      <Disclosure testID="run-projects-covered" summary={scopeText} showLabel={t("runsum.show_names")} hideLabel={t("runsum.hide")}>
        <Txt role="meta" tone="secondary">
          {coveredText(job.project_ids ?? [], names.projects, t("runs.every_project"))}
        </Txt>
      </Disclosure>
      {shown.no_availability.length > 0 ? (
        <Disclosure testID="no-availability" summary={t("runsum.no_availability", { count: shown.no_availability.length })} showLabel={t("runsum.show_names")} hideLabel={t("runsum.hide")}>
          <Txt role="meta" tone="secondary">
            {shown.no_availability.map((id) => name(id)).join(", ")}
          </Txt>
        </Disclosure>
      ) : null}
      {problem ? <InlineBanner tone="warning" testID="review-problem" title={errorText(problem)} /> : null}
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
      {/* The actions right after the figures, before the long week, on every width. Pinning them to
          a phone's bottom waits on the shell: its page wrapper keeps a transform, which holds any
          "fixed" element inside the page instead of the window. */}
      {actionBar}
      {projectButtons}
      {/* Only the weeks that hold a slot of the run: calendar weeks with none add nothing (T042a). */}
      {weeksOf(job.from, job.to)
        .filter((days) => days.some((day) => byDay.has(day)))
        .map((days) => {
        const visible = isDesktop ? days : days.filter((day) => byDay.has(day));
        if (isDesktop) {
          // The week as the roster sheet: projects as rows by client, days as columns (the owner, 2026-11-13).
          const groups = proposalSheet(shown.slots, days, projects.data ?? [], clients.data ?? [], i18n.language);
          return (
            <ScrollView key={days[0]} horizontal testID="proposal-week" contentContainerStyle={sheetStyles.sheetScroll} onLayout={(e) => setSheetWidth(e.nativeEvent.layout.width)}>
              {/* A definite width: in a sideways scroll the columns would otherwise grow to fit every
                  name on one line instead of sharing the space and wrapping. */}
              <View testID="proposal-sheet" style={[sheetStyles.sheet, { borderColor: colors.border.strong, width: Math.max(sheetWidth, SHEET_LABEL + SHEET_DAY * 7) }]}>
                <View style={sheetStyles.sheetLine}>
                  <View style={[sheetStyles.sheetHead, sheetStyles.sheetLabel, { borderColor: colors.border.subtle, backgroundColor: colors.bg.canvas }]} />
                  {days.map((day) => {
                    const cover = shown.coverage.filter((c) => c.date === day);
                    const filledDay = cover.reduce((n, c) => n + c.filled, 0);
                    const needed = cover.reduce((n, c) => n + c.needed, 0);
                    return (
                      <View key={day} testID="proposal-day-head" {...webProps({ dataSet: { date: day } })} style={[sheetStyles.sheetHead, sheetStyles.sheetDay, { borderColor: colors.border.subtle, backgroundColor: colors.bg.canvas }]}>
                        {/* Short, as on the roster sheet: the weekday and the date, so seven columns fit. */}
                        <Txt role="eyebrow" tone="secondary">
                          {t(`sheet.weekday.${new Date(`${day}T12:00:00Z`).getUTCDay()}`)}
                        </Txt>
                        <Txt role="section" numeric tone={day >= job.from && day <= job.to ? "primary" : "secondary"}>
                          {`${Number(day.slice(8))}.${Number(day.slice(5, 7))}`}
                        </Txt>
                        {needed > 0 ? (
                          <Txt role="meta" tone="secondary" numeric testID="day-coverage">
                            {t("runs.filled", { filled: filledDay, required: needed })}
                          </Txt>
                        ) : null}
                        {anyOpen && openDays.includes(day) ? (
                          <Button
                            variant="quiet"
                            icon="check"
                            // Short in the narrow column; the spoken label names the day.
                            label={t("runsum.accept_short")}
                            accessibilityLabel={`${t("review.accept_day")} · ${formatHeaderDate(day, i18n.language)}`}
                            busy={busy === day}
                            disabled={busy !== null}
                            onPress={() => void accept({ days: [day] }, day)}
                            testID="action-accept-day"
                            web={{ dataSet: { date: day } }}
                          />
                        ) : null}
                      </View>
                    );
                  })}
                </View>
                {groups.map((g) => (
                  <View key={g.clientId} testID="proposal-client">
                    <View style={[sheetStyles.sheetClient, { borderColor: colors.border.subtle, backgroundColor: colors.bg.canvas }]}>
                      <Txt role="eyebrow" tone="accent">
                        {g.clientName || t("sheet.no_client")}
                      </Txt>
                    </View>
                    {g.rows.map((row) => (
                      <View key={row.projectId} style={sheetStyles.sheetLine} testID="proposal-row">
                        <View style={[sheetStyles.sheetCell, sheetStyles.sheetLabel, { borderColor: colors.border.subtle, backgroundColor: colors.bg.canvas }]}>
                          <Txt role="label" numberOfLines={3}>
                            {row.name}
                          </Txt>
                        </View>
                        {row.cells.map((cell, i) => (
                          <View key={days[i]} testID="proposal-day" {...webProps({ dataSet: { date: days[i]! } })} style={[sheetStyles.sheetCell, sheetStyles.sheetDay, { borderColor: colors.border.subtle, backgroundColor: colors.bg.surface }]}>
                            {cell.map((slot) => (
                              <SlotBlock
                                key={slot.key}
                                compact
                                slot={slot}
                                names={names}
                                name={name}
                                editable={open && menuFor(shown, slot).canEdit}
                                // Choosing single pairs is the phone list's; the sheet accepts by day or project.
                                selectable={false}
                                chosen={chosen}
                                onToggle={(mark) => toggle(slot, mark)}
                                onPerson={(userId) => setPersonOn({ slot, userId })}
                                onAssign={() => setAssignTo(slot)}
                              />
                            ))}
                          </View>
                        ))}
                      </View>
                    ))}
                  </View>
                ))}
              </View>
            </ScrollView>
          );
        }
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
      {/* The details below the week: fairness, the trade-offs and the chips. */}
      <SectionHead title={t("runsum.details")} />
      <FairnessPanel
        jobId={job.id}
        version={shown.version}
        name={name}
        place={(id) => names.projects.get(id) ?? ""}
        type={(id) => names.types.get(id) ?? ""}
      />
      <Sheet
        open={personOn !== null || moving !== null || assignTo !== null}
        onClose={closePanels}
        title={
          moving
              ? moving.swap
                ? t("review.swap_title")
                : t("review.move_title")
              : assignTo
                ? t("roster.assign_title")
                : personOn
                  ? name(personOn.userId)
                  : ""
        }
      >
        {moving ? (
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
                const totals = cover.data!.totals;
                return (
                  <View key={option.user_id} style={styles.option} testID="review-cover-option" {...webProps({ dataSet: { userId: option.user_id, free: String(option.free) } })}>
                    <Button
                      variant="quiet"
                      icon="user"
                      label={name(option.user_id)}
                      testID="review-assign-option"
                      web={{ dataSet: { userId: option.user_id, free: String(option.free) } }}
                      onPress={() => void send({ op: "assign", key: assignTo.key, user_id: option.user_id })}
                    />
                    {/* Who is free (or would rather not); never the rule someone would break (the owner, 2026-11-13). */}
                    {option.free ? (
                      <Txt role="meta" tone="secondary">
                        {t(option.close ? "review.cover_would_rather_not" : "review.cover_free")}
                      </Txt>
                    ) : null}
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

const SHEET_LABEL = 170;
const SHEET_DAY = 120;

const sheetStyles = StyleSheet.create({
  sheetScroll: { flexGrow: 1 },
  sheet: { borderWidth: 1, flexGrow: 1 },
  sheetLine: { flexDirection: "row" },
  sheetHead: { padding: 6, borderStartWidth: 1, borderTopWidth: 1, gap: 2 },
  sheetLabel: { width: SHEET_LABEL, flexShrink: 0 },
  sheetDay: { flex: 1, minWidth: SHEET_DAY, flexBasis: 0 },
  sheetClient: { paddingVertical: 4, paddingHorizontal: 8, borderTopWidth: 1 },
  sheetCell: { padding: 6, borderStartWidth: 1, borderTopWidth: 1, gap: 6, minHeight: 56 },
});
