/**
 * A date-range field (specs/007-ux-redesign US5, FR-051, FR-052; D1 to D4).
 *
 * Presets (Today … Last month, Custom; the pay-period pair for reports) beside the calendar as a
 * list on a desktop window and as chips above it on a phone; the dates a preset stands for appear
 * under it and match the highlighted range. Two months sit side by side on desktop and the view
 * does not jump while the range stays in sight. A hand change turns the preset to Custom; an end
 * before the start is refused with a reason. Typed From and To fields accept any separator.
 */
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Pressable, ScrollView, StyleSheet, View, type StyleProp, type ViewStyle } from "react-native";

import {
  DEFAULT_TIME_ZONE,
  PAY_PERIOD_PRESETS,
  RANGE_PRESETS,
  addMonths,
  compareDates,
  endOfMonth,
  formatNumericRange,
  holidaysBetween,
  presetFor,
  presetRange,
  startOfMonth,
  todayIn,
  type DateRange,
  type PresetKey,
  type RangePreset,
} from "../../../locale";
import { useKeyboardMode } from "../../focus";
import { useTheme } from "../../theme/ThemeProvider";
import { component } from "../../theme/theme.generated";
import { webProps } from "../../web";
import { Button, focusRing, type PressState } from "../Button";
import { Txt } from "../Text";
import { CalendarMonth } from "./CalendarMonth";
import { DateField } from "./DateField";
import { useCalendarKeys } from "./useCalendarKeys";

export interface DateRangeFieldProps {
  label: string;
  value: DateRange;
  onChange: (range: DateRange) => void;
  /** Add "This pay period" and "Last pay period" (reports). */
  payPeriods?: boolean;
  minDate?: string | null;
  maxDate?: string | null;
  /** Today in the site's zone; read from the clock by default. */
  today?: string;
  /** Hide the typed From and To fields. */
  hideFields?: boolean;
  testID?: string;
  style?: StyleProp<ViewStyle>;
}

/** A date-range field. */
export function DateRangeField({
  label,
  value,
  onChange,
  payPeriods = false,
  minDate = null,
  maxDate = null,
  today: givenToday,
  hideFields = false,
  testID = "date-range",
  style,
}: DateRangeFieldProps) {
  const { colors, isDesktop } = useTheme();
  const { t, i18n } = useTranslation();
  const keyboard = useKeyboardMode();
  const today = givenToday ?? todayIn(DEFAULT_TIME_ZONE);
  const presets: readonly RangePreset[] = payPeriods ? [...RANGE_PRESETS, ...PAY_PERIOD_PRESETS] : RANGE_PRESETS;
  const current: PresetKey = presetFor(value, today, presets);
  const months = isDesktop ? 2 : 1;
  const [anchor, setAnchor] = useState(startOfMonth(value.start));
  const [pendingStart, setPendingStart] = useState<string | null>(null);
  const [focusedDate, setFocusedDate] = useState(value.start);
  const [focusOn, setFocusOn] = useState(false);
  const [endError, setEndError] = useState<string | null>(null);

  // Keep the range in sight: move only when its start left the visible months (derived while
  // rendering, React's pattern for state that follows a prop, so the view never jumps otherwise).
  const [seenStart, setSeenStart] = useState(value.start);
  if (seenStart !== value.start) {
    setSeenStart(value.start);
    const startMonth = startOfMonth(value.start);
    const lastVisible = addMonths(anchor, months - 1);
    if (compareDates(startMonth, anchor) < 0 || compareDates(startMonth, lastVisible) > 0) setAnchor(startMonth);
  }
  const presetKeys: PresetKey[] = [...presets, "custom"];

  const visible = Array.from({ length: months }, (_, i) => addMonths(anchor, i));
  const holidays = useMemo(
    () => holidaysBetween(visible[0]!, endOfMonth(visible[visible.length - 1]!), i18n.language),
    [visible, i18n.language],
  );

  const apply = (range: DateRange): void => {
    setEndError(null);
    setPendingStart(null);
    onChange(range);
  };

  const choosePreset = (preset: RangePreset): void => {
    const range = presetRange(preset, today);
    setFocusedDate(range.start);
    apply(range);
  };

  const pick = (date: string): void => {
    setFocusedDate(date);
    if (pendingStart === null) {
      setPendingStart(date);
      onChange({ start: date, end: date });
      return;
    }
    const [start, end] = compareDates(date, pendingStart) < 0 ? [date, pendingStart] : [pendingStart, date];
    apply({ start, end });
  };

  const onKeyDown = useCalendarKeys({
    onMove: (date) => {
      setFocusOn(true);
      setFocusedDate(date);
    },
    onChoose: pick,
    minDate,
    maxDate,
  });

  const summary = formatNumericRange(value.start, value.end);

  const presetButton = (preset: PresetKey) => {
    const checked = current === preset;
    const dates = preset === "custom" ? null : presetRange(preset, today);
    const radioWebProps = webProps({ tabIndex: checked ? 0 : -1 });
    return (
      <Pressable
        key={preset}
        testID={`preset-${preset}`}
        role="radio"
        accessibilityState={{ checked }}
        aria-checked={checked}
        disabled={preset === "custom"}
        onPress={() => preset !== "custom" && choosePreset(preset)}
        {...radioWebProps}
        style={(state) => {
          const s = state as PressState;
          return [
            isDesktop ? styles.presetRow : styles.presetChip,
            { minHeight: component.target.phone, borderColor: checked ? colors.accent.default : colors.border.subtle },
            checked ? { backgroundColor: colors.bg.selected, borderStartColor: colors.accent.default } : null,
            !checked && (s.hovered || s.pressed) && preset !== "custom" ? { backgroundColor: colors.bg.surface } : null,
            keyboard && s.focused ? focusRing(colors.focus.ring) : null,
          ];
        }}
      >
        <Txt role="label" tone="inherit" style={{ color: checked ? colors.accent.onSelected : colors.text.primary }} numberOfLines={1}>
          {t(`date.preset.${preset}`)}
        </Txt>
        {isDesktop && dates ? (
          <Txt role="meta" tone="secondary" numeric numberOfLines={1}>
            {formatNumericRange(dates.start, dates.end)}
          </Txt>
        ) : null}
      </Pressable>
    );
  };

  const presetList = (
    <View
      role="radiogroup"
      accessibilityLabel={t("date.presets")}
      style={isDesktop ? styles.presetList : styles.presetChips}
      testID={`${testID}-presets`}
    >
      {presetKeys.map(presetButton)}
    </View>
  );

  return (
    <View testID={testID} style={[styles.root, style]} role="group" accessibilityLabel={label}>
      <Txt role="meta" tone="secondary">
        {label}
      </Txt>
      <View style={isDesktop ? styles.bodyDesktop : styles.bodyPhone}>
        {isDesktop ? presetList : <ScrollView horizontal showsHorizontalScrollIndicator={false}>{presetList}</ScrollView>}
        <View style={styles.calendar}>
          {!hideFields ? (
            <View style={styles.fields}>
              <DateField
                label={t("date.from")}
                value={value.start}
                onChange={(date) => {
                  if (!date) return;
                  if (compareDates(date, value.end) > 0) apply({ start: date, end: date });
                  else apply({ start: date, end: value.end });
                }}
                minDate={minDate}
                maxDate={maxDate}
                today={today}
                testID={`${testID}-from`}
                style={styles.field}
              />
              <DateField
                label={t("date.to")}
                value={value.end}
                onChange={(date) => {
                  if (!date) return;
                  if (compareDates(date, value.start) < 0) {
                    setEndError(t("date.end_before_start"));
                    return;
                  }
                  apply({ start: value.start, end: date });
                }}
                error={endError}
                minDate={minDate}
                maxDate={maxDate}
                today={today}
                testID={`${testID}-to`}
                style={styles.field}
              />
            </View>
          ) : null}
          <View style={styles.monthHeader}>
            <Button variant="quiet" icon="chevron-back" label="" accessibilityLabel={t("month.previous")} onPress={() => setAnchor(addMonths(anchor, -1))} />
            <Txt role="meta" tone="secondary" numeric testID={`${testID}-summary`}>
              {summary}
            </Txt>
            <Button variant="quiet" icon="chevron-next" label="" accessibilityLabel={t("month.next")} onPress={() => setAnchor(addMonths(anchor, 1))} />
          </View>
          <View style={styles.months}>
            {visible.map((month) => (
              <CalendarMonth
                key={month}
                month={month}
                today={today}
                start={value.start}
                end={value.end}
                focusedDate={startOfMonth(focusedDate) === month ? focusedDate : null}
                focusOn={focusOn}
                minDate={minDate}
                maxDate={maxDate}
                holidays={holidays}
                onSelect={pick}
                onKeyDown={onKeyDown}
                testID={`${testID}-month-${month.slice(0, 7)}`}
              />
            ))}
          </View>
          {pendingStart ? (
            <Txt role="meta" tone="secondary" accessibilityLiveRegion="polite">
              {t("date.choose_end")}
            </Txt>
          ) : null}
        </View>
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  root: { gap: 8, alignSelf: "stretch" },
  bodyDesktop: { flexDirection: "row", gap: 24, alignItems: "flex-start" },
  bodyPhone: { gap: 12 },
  presetList: { width: 220, gap: 2 },
  presetChips: { flexDirection: "row", gap: 8, paddingBottom: 4 },
  presetRow: { paddingHorizontal: 12, paddingVertical: 6, borderStartWidth: 2, borderColor: "transparent", justifyContent: "center" },
  presetChip: { paddingHorizontal: 14, borderWidth: 1, justifyContent: "center" },
  calendar: { gap: 8, flexShrink: 1 },
  fields: { flexDirection: "row", gap: 12, flexWrap: "wrap" },
  field: { flex: 1, minWidth: 160 },
  monthHeader: { flexDirection: "row", alignItems: "center", justifyContent: "space-between" },
  months: { flexDirection: "row", gap: 24, flexWrap: "wrap" },
});
