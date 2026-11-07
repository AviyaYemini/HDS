/**
 * The W3C date-picker keyboard pattern (specs/007-ux-redesign FR-052, D3), shared by the single
 * and the range picker: arrows move a day (the forward arrow is the one that points forward in the
 * language, so Left goes to the next day in Hebrew) or a week, Home and End go to the week's ends,
 * Page Up and Page Down move a month (a year with Shift), Enter or Space choose, Escape leaves.
 */
import { useTranslation } from "react-i18next";

import { isRtlLanguage } from "../../../i18n";
import { addMonths, compareDates, daysInMonth, endOfWeek, parts, startOfWeek } from "../../../locale";
import type { WebKeyEvent } from "../../web";
import { stepDate } from "./CalendarMonth";

export interface CalendarKeyHandlers {
  onMove: (date: string) => void;
  onChoose: (date: string) => void;
  onEscape?: () => void;
  minDate?: string | null;
  maxDate?: string | null;
}

/** The same day in the month `months` away, clamped to that month's length. */
export function sameDayInMonth(date: string, months: number): string {
  const { day } = parts(date);
  const first = addMonths(date, months);
  const { year, month } = parts(first);
  const last = daysInMonth(year, month);
  return `${first.slice(0, 8)}${String(Math.min(day, last)).padStart(2, "0")}`;
}

function clamp(date: string, min: string | null | undefined, max: string | null | undefined): string {
  if (min && compareDates(date, min) < 0) return min;
  if (max && compareDates(date, max) > 0) return max;
  return date;
}

/** A key handler for the calendar grid. */
export function useCalendarKeys({ onMove, onChoose, onEscape, minDate, maxDate }: CalendarKeyHandlers) {
  const { i18n } = useTranslation();
  const rtl = isRtlLanguage(i18n.language);
  return (event: WebKeyEvent, date: string): void => {
    const forward = rtl ? "ArrowLeft" : "ArrowRight";
    const back = rtl ? "ArrowRight" : "ArrowLeft";
    let next: string | null = null;
    switch (event.key) {
      case forward:
        next = stepDate(date, 1, minDate ?? null, maxDate ?? null);
        break;
      case back:
        next = stepDate(date, -1, minDate ?? null, maxDate ?? null);
        break;
      case "ArrowDown":
        next = stepDate(date, 7, minDate ?? null, maxDate ?? null);
        break;
      case "ArrowUp":
        next = stepDate(date, -7, minDate ?? null, maxDate ?? null);
        break;
      case "Home":
        next = clamp(startOfWeek(date), minDate, maxDate);
        break;
      case "End":
        next = clamp(endOfWeek(date), minDate, maxDate);
        break;
      case "PageDown":
        next = clamp(sameDayInMonth(date, event.shiftKey ? 12 : 1), minDate, maxDate);
        break;
      case "PageUp":
        next = clamp(sameDayInMonth(date, event.shiftKey ? -12 : -1), minDate, maxDate);
        break;
      case "Enter":
      case " ":
        event.preventDefault();
        onChoose(date);
        return;
      case "Escape":
        if (onEscape) {
          event.preventDefault();
          event.stopPropagation();
          onEscape();
        }
        return;
      default:
        return;
    }
    event.preventDefault();
    if (next) onMove(next);
  };
}
