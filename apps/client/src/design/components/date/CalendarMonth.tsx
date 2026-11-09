/**
 * One month of the calendar (specs/007-ux-redesign FR-051, FR-052, FR-054; D1 to D4, D6).
 *
 * A `grid` with weekday column headers, Sunday first, and one `gridcell` per day of the month;
 * days of other months are blank, so a range never shows a day twice. Each cell carries
 * `data-date` and, inside the chosen range, `data-in-range`. Today is outlined, the range ends are
 * filled, Saturday is the day of rest, and Israeli holidays, their eves and the intermediate days
 * are marked and named. One cell at a time is in the tab order (the focused date); the owner
 * handles the keys.
 */
import { useLayoutEffect, useRef } from "react";
import { useTranslation } from "react-i18next";
import { Pressable, StyleSheet, View } from "react-native";

import {
  addDays,
  compareDates,
  daysInMonth,
  formatLongDate,
  formatMonthYear,
  formatNumericDate,
  iso,
  isWithin,
  parts,
  weekday,
  weekdayNames,
  type Holiday,
} from "../../../locale";
import { useKeyboardMode } from "../../focus";
import { useTheme } from "../../theme/ThemeProvider";
import { component } from "../../theme/theme.generated";
import { webProps, type WebKeyEvent } from "../../web";
import { focusRing, type PressState } from "../Button";
import { Txt } from "../Text";

/** Anything the web can move focus to. */
type Focusable = { focus?: () => void };

// WAI-ARIA names and the `data-in-range` value the e2e specs read (not text anyone sees).
const GRIDCELL = "gridcell";
const IN_RANGE = "true";

export interface CalendarMonthProps {
  /** Any date in the month to draw. */
  month: string;
  today: string;
  /** The chosen range, or a single date as both ends. */
  start?: string | null;
  end?: string | null;
  /** The date in the tab order; focused when `focusOn` is true. */
  focusedDate: string | null;
  focusOn: boolean;
  minDate?: string | null;
  maxDate?: string | null;
  holidays: ReadonlyMap<string, Holiday>;
  onSelect: (date: string) => void;
  onKeyDown?: (event: WebKeyEvent, date: string) => void;
  /** Hide the month's name (the owner draws one header for two months). */
  hideTitle?: boolean;
  testID?: string;
}

/** The six weeks that show a month, as ISO dates or null for a blank cell. */
export function monthGrid(month: string): (string | null)[][] {
  const { year, month: m } = parts(month);
  const first = iso(year, m, 1);
  const lead = weekday(first);
  const count = daysInMonth(year, m);
  const cells: (string | null)[] = [];
  for (let i = 0; i < lead; i += 1) cells.push(null);
  for (let d = 1; d <= count; d += 1) cells.push(iso(year, m, d));
  while (cells.length % 7 !== 0) cells.push(null);
  while (cells.length < 42) cells.push(null);
  const rows: (string | null)[][] = [];
  for (let i = 0; i < cells.length; i += 7) rows.push(cells.slice(i, i + 7));
  return rows;
}

/** A month. */
export function CalendarMonth({
  month,
  today,
  start = null,
  end = null,
  focusedDate,
  focusOn,
  minDate = null,
  maxDate = null,
  holidays,
  onSelect,
  onKeyDown,
  hideTitle = false,
  testID,
}: CalendarMonthProps) {
  const { colors, isDesktop } = useTheme();
  const { t, i18n } = useTranslation();
  const keyboard = useKeyboardMode();
  const cells = useRef(new Map<string, Focusable>());
  const size = isDesktop ? 36 : component.target.phone;
  const names = weekdayNames(i18n.language, isDesktop ? "short" : "narrow");
  const longNames = weekdayNames(i18n.language, "long");
  const rows = monthGrid(month);
  const blankProps = webProps({ role: GRIDCELL });

  // Focus moves with the keyboard before the browser paints, so the next key lands on the new cell;
  // a cell that is not mounted yet (the dialog's first frame) is focused a tick later.
  useLayoutEffect(() => {
    if (!focusOn || !focusedDate) return undefined;
    const cell = cells.current.get(focusedDate);
    if (cell) {
      cell.focus?.();
      return undefined;
    }
    const handle = setTimeout(() => cells.current.get(focusedDate)?.focus?.(), 0);
    return () => clearTimeout(handle);
  }, [focusOn, focusedDate]);

  const lo = start && end ? (compareDates(start, end) <= 0 ? start : end) : (start ?? end);
  const hi = start && end ? (compareDates(start, end) <= 0 ? end : start) : (start ?? end);

  return (
    <View testID={testID} style={styles.month}>
      {hideTitle ? null : (
        <Txt role="label" style={styles.title} testID={testID ? `${testID}-title` : undefined}>
          {formatMonthYear(month, i18n.language)}
        </Txt>
      )}
      <View role="grid" accessibilityLabel={formatMonthYear(month, i18n.language)} style={styles.grid}>
        <View role="row" style={[styles.row, isDesktop ? null : styles.rowPhone]}>
          {names.map((name, index) => (
            <View key={index} role="columnheader" accessibilityLabel={longNames[index]} style={[styles.cell, { width: size, height: 28 }]}>
              <Txt role="eyebrow" tone="secondary" style={styles.center}>
                {name}
              </Txt>
            </View>
          ))}
        </View>
        {rows.map((row, r) => (
          <View key={r} role="row" style={[styles.row, isDesktop ? null : styles.rowPhone]}>
            {row.map((date, c) => {
              if (!date) {
                return <View key={`blank-${c}`} {...blankProps} aria-disabled style={[styles.cell, { width: size, height: size }]} />;
              }
              const inRange = !!lo && !!hi && isWithin(date, lo, hi);
              const isEnd = date === lo || date === hi;
              const isToday = date === today;
              const disabled = (!!minDate && compareDates(date, minDate) < 0) || (!!maxDate && compareDates(date, maxDate) > 0);
              const holiday = holidays.get(date);
              const saturday = weekday(date) === 6;
              const label = [
                formatLongDate(date, i18n.language),
                isToday ? t("date.today") : null,
                holiday ? `${t(`date.${holiday.kind}`)}: ${holiday.name}` : null,
              ]
                .filter(Boolean)
                .join(", ");
              const cellProps = webProps({
                role: GRIDCELL,
                dataSet: { date, ...(inRange ? { inRange: IN_RANGE } : {}) },
                tabIndex: focusedDate === date ? 0 : -1,
                onKeyDown: onKeyDown ? (event) => onKeyDown(event, date) : undefined,
              });
              const textColor = isEnd
                ? colors.bg.canvas
                : disabled
                  ? colors.border.strong
                  : saturday || holiday?.kind === "holiday"
                    ? colors.text.secondary
                    : colors.text.primary;
              return (
                <Pressable
                  key={date}
                  ref={(node) => {
                    if (node) cells.current.set(date, node as unknown as Focusable);
                    else cells.current.delete(date);
                  }}
                  accessibilityRole="button"
                  accessibilityLabel={label}
                  accessibilityState={{ selected: inRange, disabled }}
                  aria-selected={inRange}
                  aria-disabled={disabled}
                  disabled={disabled}
                  onPress={() => onSelect(date)}
                  {...cellProps}
                  style={(state) => {
                    const s = state as PressState;
                    return [
                      styles.cell,
                      { width: size, height: size },
                      inRange ? { backgroundColor: colors.bg.selected } : null,
                      isEnd ? { backgroundColor: colors.text.primary } : null,
                      isToday && !isEnd ? { borderWidth: 1, borderColor: colors.accent.default } : null,
                      !isEnd && !disabled && (s.hovered || s.pressed) ? { backgroundColor: colors.border.subtle } : null,
                      keyboard && s.focused ? focusRing(colors.focus.ring) : null,
                    ];
                  }}
                >
                  <Txt role="numeric" tone="inherit" style={[styles.center, { color: textColor }, isEnd ? styles.strong : null]}>
                    {String(parts(date).day)}
                  </Txt>
                  {holiday ? (
                    <View
                      style={[
                        styles.mark,
                        { borderColor: isEnd ? colors.bg.canvas : colors.status.info },
                        holiday.kind === "holiday" ? { backgroundColor: isEnd ? colors.bg.canvas : colors.status.info } : null,
                      ]}
                    />
                  ) : null}
                </Pressable>
              );
            })}
          </View>
        ))}
      </View>
      <Txt role="meta" tone="secondary" style={styles.hidden} accessibilityElementsHidden>
        {lo && hi ? `${formatNumericDate(lo)}–${formatNumericDate(hi)}` : ""}
      </Txt>
    </View>
  );
}

/** The date `days` after `date`, kept inside `[min, max]` when given. */
export function stepDate(date: string, days: number, min: string | null, max: string | null): string {
  let next = addDays(date, days);
  if (min && compareDates(next, min) < 0) next = min;
  if (max && compareDates(next, max) > 0) next = max;
  return next;
}

const styles = StyleSheet.create({
  month: { gap: 8 },
  title: { textAlign: "center" },
  grid: { gap: 2 },
  row: { flexDirection: "row", gap: 2 },
  // On a phone the 44-pixel cells touch, so a month is 7 x 44 = 308 wide and fits a 310 column.
  rowPhone: { gap: 0 },
  cell: { alignItems: "center", justifyContent: "center" },
  center: { textAlign: "center" },
  strong: { fontWeight: "600" },
  mark: { position: "absolute", bottom: 3, width: 5, height: 5, borderWidth: 1 },
  hidden: { position: "absolute", width: 1, height: 1, overflow: "hidden", opacity: 0 },
});
