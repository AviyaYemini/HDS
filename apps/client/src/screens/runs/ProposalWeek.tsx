/**
 * A run's proposal as a roster week, read-only (specs/007-ux-redesign FR-080, T107; the owner:
 * "clicking on the run allow the user to view it's output"). Each day of each week the run covers,
 * its slots by start time: project and shift type, the times, filled over needed, and who goes,
 * by name, each marked as the run adds them, keeps them from the drafts or takes them off; marked in
 * words and an icon, never colour alone. Desktop shows the seven days side by side, as the roster
 * does; a phone lists the days that have slots.
 */
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { Txt } from "../../design/components";
import { Icon } from "../../design/icons/Icon";
import { useTheme } from "../../design/theme/ThemeProvider";
import { webProps } from "../../design/web";
import { DEFAULT_TIME_ZONE, formatHeaderDate, formatTimeRange } from "../../locale";
import { peopleOf, slotsByDay, weeksOf, type Job, type Proposal, type Slot } from "./model";

export interface Names {
  projects: Map<string, string>;
  types: Map<string, string>;
  people: Map<string, string>;
}

const MARK = { added: "plus", kept: "check", removed: "minus" } as const;

function SlotBlock({ slot, changes, names }: { slot: Slot; changes: Proposal["changes"]; names: Names }) {
  const { t } = useTranslation();
  const { colors } = useTheme();
  const people = peopleOf(slot, changes);
  const placed = people.filter((p) => p.change !== "removed").length;
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
      <Txt role="meta" tone={placed < slot.headcount ? "warning" : "secondary"} numeric>
        {t("runs.filled", { filled: Math.min(placed, slot.headcount), required: slot.headcount })}
      </Txt>
      {people.map((person) => (
        <View
          key={`${person.userId}-${person.change}`}
          testID="proposal-person"
          {...webProps({ dataSet: { userId: person.userId, change: person.change } })}
          style={styles.person}
        >
          <Icon name={MARK[person.change]} size={14} color={person.change === "removed" ? colors.text.secondary : colors.text.primary} />
          <Txt role="meta" tone={person.change === "removed" ? "secondary" : "primary"} style={person.change === "removed" ? styles.removed : null}>
            {names.people.get(person.userId) ?? t("runs.unknown_person")}
          </Txt>
          {person.change === "kept" ? null : (
            <Txt role="meta" tone="secondary">
              {t(`runs.change.${person.change}`)}
            </Txt>
          )}
        </View>
      ))}
    </View>
  );
}

export function ProposalWeek({ job, names }: { job: Job; names: Names }) {
  const { t, i18n } = useTranslation();
  const { colors, isDesktop } = useTheme();
  const proposal = job.proposal;
  if (proposal === null) return null;
  const byDay = slotsByDay(proposal);
  return (
    <View style={styles.weeks}>
      <Txt role="section" heading={2}>
        {t("runs.proposal_title")}
      </Txt>
      {weeksOf(job.from, job.to).map((days) => {
        const shown = isDesktop ? days : days.filter((day) => byDay.has(day));
        return (
          <View key={days[0]} testID="proposal-week" style={[styles.week, isDesktop ? styles.weekRow : null]}>
            {shown.map((day) => {
              const inRange = day >= job.from && day <= job.to;
              return (
                <View
                  key={day}
                  testID="proposal-day"
                  {...webProps({ dataSet: { date: day } })}
                  style={[isDesktop ? styles.column : styles.dayStack, { borderColor: colors.border.subtle, opacity: inRange ? 1 : 0.5 }]}
                >
                  <Txt role="label" tone="secondary">
                    {formatHeaderDate(day, i18n.language)}
                  </Txt>
                  {(byDay.get(day) ?? []).map((slot) => (
                    <SlotBlock key={slot.key} slot={slot} changes={proposal.changes} names={names} />
                  ))}
                </View>
              );
            })}
          </View>
        );
      })}
    </View>
  );
}

const styles = StyleSheet.create({
  weeks: { gap: 16 },
  week: { gap: 12 },
  weekRow: { flexDirection: "row", alignItems: "flex-start", gap: 8 },
  column: { flexGrow: 1, flexShrink: 1, flexBasis: 0, minWidth: 0, gap: 8, paddingTop: 8, borderTopWidth: 1 },
  dayStack: { gap: 8, paddingTop: 8, borderTopWidth: 1 },
  block: { gap: 4, padding: 8, borderWidth: 1 },
  person: { flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 4 },
  removed: { textDecorationLine: "line-through" },
});
