/**
 * A date-range field (specs/007-ux-redesign US5, FR-051, FR-052; D1 to D4).
 *
 * Presets (Today … Last month, Custom; the pay-period pair for reports) beside the calendar as a
 * list on a desktop window and as chips above it on a phone; the dates a preset stands for appear
 * under it and match the highlighted range. Two months sit side by side on desktop and the view
 * does not jump while the range stays in sight. A hand change turns the preset to Custom; an end
 * before the start is refused with a reason. Typed From and To fields accept any separator.
 */
import { useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { Pressable, ScrollView, StyleSheet, View, type StyleProp, type ViewStyle } from "react-native";

import {
  DEFAULT_TIME_ZONE,
  FORWARD_PRESETS,
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
import { isRtlLanguage } from "../../../i18n";
import { webProps, type WebKeyEvent } from "../../web";
import { Button, focusRing, type PressState } from "../Button";
import { Txt } from "../Text";
import { CalendarMonth } from "./CalendarMonth";
import { DateField } from "./DateField";
import { useCalendarKeys } from "./useCalendarKeys";

/** Anything the web can move focus to. */
type Focusable = { focus?: () => void };

export interface DateRangeFieldProps {
  label: string;
  value: DateRange;
  onChange: (range: DateRange) => void;
  /** Add "This pay period" and "Last pay period" (reports). */
  payPeriods?: boolean;
  /** Offer ranges ahead (Next week, Next 7 days, Next 30 days) instead, for planning. */
  forward?: boolean;
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
  forward = false,
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
  const presets: readonly RangePreset[] = forward
    ? FORWARD_PRESETS
    : payPeriods
      ? [...RANGE_PRESETS, ...PAY_PERIOD_PRESETS]
      : RANGE_PRESETS;
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

  const visible = useMemo(() => Array.from({ length: months }, (_, i) => addMonths(anchor, i)), [anchor, months]);
  const holidays = useMemo(
    () => holidaysBetween(anchor, endOfMonth(addMonths(anchor, months - 1)), i18n.language),
    [anchor, months, i18n.language],
  );

  /** Bring the month holding `date` into view, moving as little as possible. */
  const reveal = (date: string): void => {
    const month = startOfMonth(date);
    const last = addMonths(anchor, months - 1);
    if (compareDates(month, anchor) < 0) setAnchor(month);
    else if (compareDates(month, last) > 0) setAnchor(addMonths(month, -(months - 1)));
  };

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
      reveal(date);
    },
    onChoose: pick,
    minDate,
    maxDate,
  });

  const summary = formatNumericRange(value.start, value.end);

  // The presets are one radio group: arrow keys move the choice (the forward arrow is the one that
  // points forward in the language) and focus follows; Custom is a state, chosen by the calendar.
  const rtl = isRtlLanguage(i18n.language);
  const presetRefs = useRef<(Focusable | null)[]>([]);
  const [presetFocus, setPresetFocus] = useState<number | null>(null);
  useEffect(() => {
    if (presetFocus !== null) presetRefs.current[presetFocus]?.focus?.();
  }, [presetFocus]);
  const onPresetKey = (index: number) => (event: WebKeyEvent) => {
    const nextKey = rtl ? "ArrowLeft" : "ArrowRight";
    const backKey = rtl ? "ArrowRight" : "ArrowLeft";
    let next: number | null = null;
    if (event.key === "ArrowDown" || event.key === nextKey) next = (index + 1) % presets.length;
    else if (event.key === "ArrowUp" || event.key === backKey) next = (index - 1 + presets.length) % presets.length;
    else if (event.key === "Home") next = 0;
    else if (event.key === "End") next = presets.length - 1;
    if (next === null) return;
    event.preventDefault();
    choosePreset(presets[next]!);
    setPresetFocus(next);
  };

  const presetButton = (preset: PresetKey) => {
    const checked = current === preset;
    const dates = preset === "custom" ? null : presetRange(preset, today);
    const index = preset === "custom" ? presets.length - 1 : presets.indexOf(preset);
    const radioWebProps = webProps({ tabIndex: checked ? 0 : -1, onKeyDown: onPresetKey(index) });
    return (
      <Pressable
        key={preset}
        ref={(node) => {
          if (preset !== "custom") presetRefs.current[index] = node as unknown as Focusable | null;
        }}
        testID={`preset-${preset}`}
        role="radio"
        accessibilityState={{ checked }}
        aria-checked={checked}
        accessibilityHint={preset === "custom" ? t("date.custom_hint") : undefined}
        onPress={() => preset !== "custom" && choosePreset(preset)}
        {...radioWebProps}
        style={(state) => {
          const s = state as PressState;
          return [
            isDesktop ? styles.presetRow : styles.presetChip,
            { minHeight: component.target.phone, borderColor: checked ? colors.accent.default : colors.border.subtle },
            checked ? { backgroundColor: colors.bg.selected, borderStartColor: colors.accent.default } : null,
            !checked && (s.hovered || s.pressed) && preset !== "custom" ? { backgroundColor: colors.bg.surface } : null,
            preset === "custom" && !checked ? { opacity: 0.7 } : null,
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
  // The width is the basis, so the row wraps on a narrow screen (flex: 1 would set it to 0, FR-079).
  field: { flexGrow: 1, flexShrink: 1, flexBasis: 160 },
  monthHeader: { flexDirection: "row", alignItems: "center", justifyContent: "space-between" },
  months: { flexDirection: "row", gap: 24, flexWrap: "wrap" },
});
