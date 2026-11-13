/**
 * A finished run's proposal (specs/007-ux-redesign FR-042; 004 US2 to US4): "Optimal" or "Best found
 * in N s", what it filled, and the places left empty with their reasons.
 */
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { Txt } from "../../design/components";
import { webProps } from "../../design/web";
import { useTheme } from "../../design/theme/ThemeProvider";
import { nameText, DEFAULT_TIME_ZONE, formatNumericDate, formatTimeRange } from "../../locale";
import { numbersOf, reasonKey } from "./review";
import type { Job, Names } from "./model";


export function Proposal({ job, names }: { job: Job; names: Names }) {
  const { t, i18n } = useTranslation();
  // A code this build does not know yet still reads as a reason.
  const reasonText = (code: string) => t(reasonKey(code, (key) => i18n.exists(key)));
  const { colors } = useTheme();
  const proposal = job.proposal;
  if (proposal === null) return null;
  const slots = new Map(proposal.slots.map((slot) => [slot.key, slot]));
  return (
    <View style={styles.section}>
      {proposal.unfilled.length === 0 ? null : (
        <View style={styles.list}>
          <Txt role="label" heading={2}>
            {t("runs.unfilled_title")}
          </Txt>
          {proposal.unfilled.map((gap) => {
            const slot = slots.get(gap.key);
            return (
              <View key={gap.key} testID="unfilled" style={[styles.row, { borderColor: colors.border.subtle }]}>
                {slot ? (
                  <View style={styles.when}>
                    <Txt role="label">{[names.projects.get(slot.project_id), names.types.get(slot.shift_type_id)].filter((x): x is string => Boolean(x)).map(nameText).join(" · ")}</Txt>
                    <Txt role="meta" tone="secondary" numeric>
                      {`${formatNumericDate(slot.date)} ${formatTimeRange(slot.starts_at, slot.ends_at, DEFAULT_TIME_ZONE)}`}
                    </Txt>
                  </View>
                ) : null}
                <Txt role="meta" tone="warning">
                  {t("runs.places_empty", { count: gap.places })}
                </Txt>
                {gap.reasons.map((reason) => (
                  <View key={reason.code} testID="unfilled-reason" {...webProps({ dataSet: { code: reason.code } })}>
                    <Txt role="meta" tone="secondary">
                      {reason.employees > 0 ? t("runs.reason", { reason: reasonText(reason.code), count: reason.employees }) : reasonText(reason.code)}
                    </Txt>
                    {/* Rest and the weekly maximum name each person, with the numbers (008 FR-013). */}
                    {(reason.people ?? []).map((person) => {
                      const numbers = numbersOf(person, i18n.language);
                      const who = names.people.get(person.user_id) ?? t("runs.unknown_person");
                      return (
                        <Txt key={person.user_id} role="meta" tone="secondary" numeric testID="unfilled-person" {...webProps({ dataSet: { userId: person.user_id } })}>
                          {numbers ? t("runs.person_numbers", { person: who, numbers: t(numbers.key, numbers.values) }) : who}
                        </Txt>
                      );
                    })}
                  </View>
                ))}
              </View>
            );
          })}
        </View>
      )}
      {proposal.short_published.length > 0 ? (
        <Txt role="meta" tone="secondary">
          {t("runs.short_published", { count: proposal.short_published.length })}
        </Txt>
      ) : null}
      <Txt role="meta" tone="secondary" numeric>
        {t("runs.changes", { added: proposal.changes.added.length, removed: proposal.changes.removed.length })}
      </Txt>
    </View>
  );
}

/**
 * Best so far and Stop while a run works (008 FR-027, FR-028): the places filled so far out of those
 * needed, and "Stop and use best so far" once there is a result, Cancel before. A stop takes a few
 * seconds to land; until then the button says so.
 */

const styles = StyleSheet.create({
  section: { gap: 12 },
  list: { gap: 8 },
  row: { flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 12, padding: 12, borderWidth: 1 },
  when: { flexGrow: 1, minWidth: 160 },
});
