/**
 * A date field (specs/007-ux-redesign FR-051, FR-052, FR-055; D1 to D3).
 *
 * Type the date with any separator, day first, and it reads back as dd/MM/yyyy; or open the
 * calendar, a dialog with today marked and impossible dates disabled, drive it by keyboard, and
 * press Enter. The chosen date is announced, Escape returns to the field. On a desktop window the
 * calendar opens under the field; on a phone it rises as a sheet.
 */
import { useMemo, useRef, useState, type ComponentRef } from "react";
import { useTranslation } from "react-i18next";
import { Modal, Pressable, StyleSheet, useWindowDimensions, View, type StyleProp, type ViewStyle } from "react-native";

import {
  DEFAULT_TIME_ZONE,
  addMonths,
  compareDates,
  endOfMonth,
  formatMonthYear,
  formatNumericDate,
  holidaysBetween,
  parseTypedDate,
  startOfMonth,
  todayIn,
} from "../../../locale";
import { useTheme } from "../../theme/ThemeProvider";
import { component } from "../../theme/theme.generated";
import { webProps } from "../../web";
import { Button } from "../Button";
import { Txt } from "../Text";
import { TextField, type TextFieldHandle } from "../TextField";
import { CalendarMonth } from "./CalendarMonth";
import { useCalendarKeys } from "./useCalendarKeys";

export interface DateFieldProps {
  label: string;
  /** The chosen date, ISO, or null. */
  value: string | null;
  onChange: (date: string | null) => void;
  minDate?: string | null;
  maxDate?: string | null;
  helper?: string;
  /** An error from the owner (a range's end before its start, say); typing errors are the field's own. */
  error?: string | null;
  /** The error text's test id (see TextField). */
  errorTestID?: string;
  disabled?: boolean;
  /** Today in the site's zone; read from the clock by default. */
  today?: string;
  /** The ids: `<testID>` is the input, `<testID>-open` the calendar button. */
  testID?: string;
  style?: StyleProp<ViewStyle>;
}

/** A date field. */
export function DateField({
  label,
  value,
  onChange,
  minDate = null,
  maxDate = null,
  helper,
  error = null,
  errorTestID,
  disabled = false,
  today: givenToday,
  testID = "date-field",
  style,
}: DateFieldProps) {
  const { colors, isDesktop } = useTheme();
  const { t, i18n } = useTranslation();
  const window = useWindowDimensions();
  const today = givenToday ?? todayIn(DEFAULT_TIME_ZONE);
  const [text, setText] = useState(value ? formatNumericDate(value) : "");
  const [typingError, setTypingError] = useState<string | null>(null);
  const [open, setOpen] = useState(false);
  const [focusedDate, setFocusedDate] = useState(value ?? today);
  const [announced, setAnnounced] = useState("");
  const [anchor, setAnchor] = useState<{ x: number; y: number; width: number; height: number } | null>(null);
  const inputRef = useRef<TextFieldHandle>(null);
  const statusWebProps = webProps({ "aria-live": "polite" });
  const backdropWebProps = webProps({ tabIndex: -1 });
  const dialogWebProps = webProps({ "aria-label": t("date.calendar") });
  const fieldRef = useRef<ComponentRef<typeof View>>(null);

  // Follow the value from outside (derived while rendering, React's pattern for prop-driven state).
  const [seenValue, setSeenValue] = useState(value);
  if (seenValue !== value) {
    setSeenValue(value);
    setText(value ? formatNumericDate(value) : "");
    if (value) setTypingError(null);
  }

  const month = startOfMonth(focusedDate);
  const holidays = useMemo(() => holidaysBetween(month, endOfMonth(month), i18n.language), [month, i18n.language]);

  const allowed = (date: string): boolean =>
    !(minDate && compareDates(date, minDate) < 0) && !(maxDate && compareDates(date, maxDate) > 0);

  const commitTyped = (): void => {
    const trimmed = text.trim();
    if (trimmed === "") {
      setTypingError(null);
      if (value !== null) onChange(null);
      return;
    }
    const parsed = parseTypedDate(trimmed);
    if (!parsed) {
      setTypingError(t("date.invalid"));
      return;
    }
    if (!allowed(parsed)) {
      setTypingError(t("date.out_of_range"));
      return;
    }
    setTypingError(null);
    setText(formatNumericDate(parsed));
    setFocusedDate(parsed);
    if (parsed !== value) onChange(parsed);
  };

  const close = (): void => {
    setOpen(false);
    setTimeout(() => (inputRef.current as unknown as { focus?: () => void } | null)?.focus?.(), 0);
  };

  const choose = (date: string): void => {
    if (!allowed(date)) return;
    onChange(date);
    setText(formatNumericDate(date));
    setTypingError(null);
    setAnnounced(t("date.chosen", { date: formatNumericDate(date) }));
    close();
  };

  const openCalendar = (): void => {
    setFocusedDate(value ?? today);
    (fieldRef.current as unknown as { measureInWindow?: (cb: (x: number, y: number, w: number, h: number) => void) => void } | null)?.measureInWindow?.(
      (x, y, width, height) => setAnchor({ x, y, width, height }),
    );
    setOpen(true);
  };

  const onKeyDown = useCalendarKeys({ onMove: setFocusedDate, onChoose: choose, onEscape: close, minDate, maxDate });

  const calendarWidth = isDesktop ? 7 * 38 + 32 : window.width;
  const popover: ViewStyle | null =
    isDesktop && anchor
      ? {
          position: "absolute",
          top: Math.min(anchor.y + anchor.height + 4, window.height - 380),
          // Aligned with the field's start edge in both directions.
          ...(i18n.dir() === "rtl"
            ? { right: Math.max(8, window.width - (anchor.x + anchor.width)) }
            : { left: Math.max(8, Math.min(anchor.x, window.width - calendarWidth - 8)) }),
          width: calendarWidth,
        }
      : null;

  return (
    <View ref={fieldRef} style={[styles.root, style]}>
      <TextField
        ref={inputRef}
        testID={testID}
        label={label}
        value={text}
        onChangeText={(next) => {
          setText(next);
          if (typingError) setTypingError(null);
        }}
        onBlur={commitTyped}
        onSubmitEditing={commitTyped}
        placeholder={t("date.placeholder")}
        helper={helper}
        error={error ?? typingError}
        errorTestID={errorTestID}
        disabled={disabled}
        direction="ltr"
        inputMode="numeric"
        autoComplete="off"
        autoCorrect={false}
        trailing={
          <Button
            variant="quiet"
            icon="calendar"
            label=""
            accessibilityLabel={t("date.open_calendar")}
            onPress={openCalendar}
            disabled={disabled}
            testID={`${testID}-open`}
            web={{ "aria-haspopup": "dialog", "aria-expanded": open }}
          />
        }
      />
      <Txt role="meta" tone="secondary" a11yRole="status" accessibilityLiveRegion="polite" style={styles.hidden} {...statusWebProps} testID={`${testID}-status`}>
        {announced}
      </Txt>
      {open ? (
        <Modal visible transparent animationType="none" onRequestClose={close} statusBarTranslucent {...dialogWebProps}>
          <View style={styles.modalRoot}>
            <Pressable accessibilityLabel={t("common.close")} role="button" onPress={close} style={styles.backdropPress} {...backdropWebProps}>
              <View style={[styles.backdrop, isDesktop ? styles.backdropClear : null]} />
            </Pressable>
            <View
              accessibilityViewIsModal
              accessibilityLabel={t("date.calendar")}
              testID={`${testID}-dialog`}
              style={[
                styles.dialog,
                { backgroundColor: colors.bg.canvas, borderColor: colors.border.strong },
                popover ?? styles.sheet,
                !isDesktop ? { paddingBottom: 24 } : null,
              ]}
            >
              <View style={styles.monthHeader}>
                <Button
                  variant="quiet"
                  icon="chevron-back"
                  label=""
                  accessibilityLabel={t("month.previous")}
                  onPress={() => setFocusedDate(addMonths(focusedDate, -1))}
                />
                <Txt role="label" testID={`${testID}-month`}>
                  {formatMonthYear(month, i18n.language)}
                </Txt>
                <Button
                  variant="quiet"
                  icon="chevron-next"
                  label=""
                  accessibilityLabel={t("month.next")}
                  onPress={() => setFocusedDate(addMonths(focusedDate, 1))}
                />
              </View>
              <CalendarMonth
                month={month}
                today={today}
                start={value}
                end={value}
                focusedDate={focusedDate}
                focusOn={open}
                minDate={minDate}
                maxDate={maxDate}
                holidays={holidays}
                onSelect={choose}
                onKeyDown={onKeyDown}
                hideTitle
                testID={`${testID}-calendar`}
              />
              <View style={styles.dialogFooter}>
                <Button variant="link" label={t("date.today")} onPress={() => choose(today)} disabled={!allowed(today)} />
                <Button variant="quiet" label={t("common.close")} onPress={close} />
              </View>
            </View>
          </View>
        </Modal>
      ) : null}
    </View>
  );
}

const styles = StyleSheet.create({
  root: { alignSelf: "stretch" },
  hidden: { position: "absolute", width: 1, height: 1, overflow: "hidden", opacity: 0 },
  modalRoot: { flex: 1 },
  backdropPress: { position: "absolute", top: 0, bottom: 0, start: 0, end: 0 },
  backdrop: { flex: 1, backgroundColor: "rgba(16, 20, 19, 0.45)" },
  backdropClear: { backgroundColor: "transparent" },
  dialog: { borderWidth: 1, padding: 16, gap: 8 },
  sheet: { position: "absolute", bottom: 0, start: 0, end: 0, alignItems: "center", borderBottomWidth: 0 },
  monthHeader: { flexDirection: "row", alignItems: "center", justifyContent: "space-between", minHeight: component.target.phone },
  dialogFooter: { flexDirection: "row", justifyContent: "space-between", alignItems: "center", marginTop: 4 },
});
