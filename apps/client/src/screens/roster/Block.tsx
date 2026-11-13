/**
 * One shift on the roster week (specs/007-ux-redesign FR-025 to FR-041): its times and coverage, each
 * person's bar against it (a half shift narrower, extra hours past its edge), a lock, its warnings
 * badge and its empty seats.
 */
import { useTranslation } from "react-i18next";
import { Pressable, StyleSheet, View } from "react-native";

import { Txt } from "../../design/components";
import { Icon } from "../../design/icons/Icon";
import { useTheme } from "../../design/theme/ThemeProvider";
import { webProps } from "../../design/web";
import { formatTimeRange } from "../../locale";
import { barGeometry, coverage, type Shift } from "./model";

/** Extra hours draw past the block's edge by design (US3); the overflow check (FR-079) allows it. */
const OVERHANG = webProps({ dataSet: { overflow: "intended" } as const });

export function Block({
  shift,
  editable,
  nameOf,
  onOpenSeat,
  onPerson,
}: {
  shift: Shift;
  editable: boolean;
  nameOf: (id: string) => string;
  onOpenSeat: () => void;
  onPerson: (assignmentId: string, userId: string) => void;
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

const styles = StyleSheet.create({
  nameRow: { flexDirection: "row", alignItems: "center", gap: 4 },
  block: { borderWidth: 1, borderStartWidth: 3, padding: 6, gap: 4 },
  blockHead: { flexDirection: "row", alignItems: "center", gap: 6, justifyContent: "space-between" },
  person: { gap: 2, minHeight: 32, justifyContent: "center" },
  track: { height: 6, overflow: "visible" },
  bar: { height: 6 },
  seat: { minHeight: 32, borderWidth: 1, borderStyle: "dashed", alignItems: "center", justifyContent: "center" },
});
