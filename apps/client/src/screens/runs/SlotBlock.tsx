/**
 * One slot of a proposal (specs/008-auto-scheduler-review FR-003 to FR-012): its time and place, each
 * person with their mark and lock, the pairs a part accept chooses, and its empty places.
 */
import { useTranslation } from "react-i18next";
import { View } from "react-native";

import { Button, Txt } from "../../design/components";
import { Icon } from "../../design/icons/Icon";
import { useTheme } from "../../design/theme/ThemeProvider";
import { webProps } from "../../design/web";
import { DEFAULT_TIME_ZONE, formatTimeRange } from "../../locale";
import type { Names } from "./model";
import { MARK_ICON, openMarks, type Pair, type ReviewSlot } from "./review";
import { styles } from "./reviewStyles";


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

export function SlotBlock({ slot, names, name, editable, selectable, chosen, onToggle, onPerson, onAssign }: BlockProps) {
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
            {who?.locked ? (
              // Locked: later runs keep this person here (FR-018), in words beside the icon.
              <View style={styles.lockMark} testID="mark-locked">
                <Icon name="lock" size={12} color={colors.text.secondary} />
                <Txt role="meta" tone="secondary">
                  {t("review.locked")}
                </Txt>
              </View>
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

