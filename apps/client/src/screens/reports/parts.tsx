/**
 * The reports' shared parts (specs/007-ux-redesign FR-050, FR-025, D4, T088): the range, defaulting
 * to this pay period and never reaching past today; a row of key figures, each exactly what the API
 * answered (the client never adds them up); bars that open into their rows; and a page of rows.
 */
import { useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Pressable, StyleSheet, View } from "react-native";

import { Button, DateRangeField, Txt } from "../../design/components";
import { useTheme } from "../../design/theme/ThemeProvider";
import { webProps } from "../../design/web";
import { DEFAULT_TIME_ZONE, compareDates, presetRange, todayIn, type DateRange } from "../../locale";

/** The report's range: this pay period by default; what is asked never ends after today. */
export function useReportRange(initial: "this-pay-period" | "this-month" = "this-pay-period"): { range: DateRange; setRange: (range: DateRange) => void; asked: { from: string; to: string }; today: string } {
  const today = todayIn(DEFAULT_TIME_ZONE);
  const [range, setRange] = useState<DateRange>(() => presetRange(initial, today));
  const to = compareDates(range.end, today) > 0 ? today : range.end;
  return { range, setRange, asked: { from: range.start, to }, today };
}

export function ReportRange({ range, setRange, today }: { range: DateRange; setRange: (range: DateRange) => void; today: string }) {
  const { t } = useTranslation();
  return <DateRangeField label={t("reports.range_label")} value={range} onChange={setRange} payPeriods maxDate={today} />;
}

/** One key figure: its label, the value shown, and the API's value as `data-value`. */
export interface Figure {
  key: string;
  label: string;
  shown: string;
  value: string;
}

export function Figures({ figures }: { figures: readonly Figure[] }) {
  const { colors } = useTheme();
  return (
    <View style={styles.figures}>
      {figures.map((f) => (
        <View
          key={f.key}
          testID={`figure-${f.key}`}
          {...webProps({ dataSet: { value: f.value } })}
          style={[styles.figure, { borderColor: colors.border.subtle, backgroundColor: colors.bg.surface }]}
        >
          <Txt role="eyebrow" tone="secondary">
            {f.label}
          </Txt>
          <Txt role="display" numeric>
            {f.shown}
          </Txt>
        </View>
      ))}
    </View>
  );
}

/** A bar: a group's name, its share against the largest, its own figures, and its rows when open. */
export interface Bar {
  id: string;
  name: string;
  /** What sizes the bar, a figure the API answered (a group's hours, a project's people). */
  hours: number;
  detail: string;
  rows: ReactNode;
}

export function Bars({ bars, title, largest, empty }: { bars: readonly Bar[]; title: string; largest: number; empty: string }) {
  const { colors } = useTheme();
  const [open, setOpen] = useState<string | null>(null);
  // Every bar is sized against the report's largest, on every page alike (group G review).
  const scale = Math.max(1, largest);
  if (bars.length === 0) {
    return (
      <Txt role="body" tone="secondary">
        {empty}
      </Txt>
    );
  }
  return (
    <View style={styles.bars}>
      <Txt role="section" heading={2}>
        {title}
      </Txt>
      {bars.map((bar) => {
        const expanded = open === bar.id;
        return (
          <View key={bar.id} style={styles.barBlock}>
            <Pressable
              role="button"
              testID="report-bar"
              accessibilityState={{ expanded }}
              aria-expanded={expanded}
              onPress={() => setOpen(expanded ? null : bar.id)}
              style={[styles.bar, { borderColor: colors.border.subtle }]}
            >
              <View style={styles.barText}>
                <Txt role="label" numberOfLines={1} style={styles.grow}>
                  {bar.name}
                </Txt>
                <Txt role="meta" tone="secondary" numeric>
                  {bar.detail}
                </Txt>
              </View>
              <View style={[styles.track, { backgroundColor: colors.bg.selected }]}>
                <View style={[styles.fill, { width: `${(bar.hours / scale) * 100}%`, backgroundColor: colors.accent.default }]} />
              </View>
            </Pressable>
            {expanded ? (
              <View testID="report-rows" style={styles.rows}>
                {bar.rows}
              </View>
            ) : null}
          </View>
        );
      })}
    </View>
  );
}

/**
 * A page of `items` at a time, with the pager below it; a new report (`key`: its range, grouping or
 * scope) opens on its first page (group G review).
 */
export function usePage<T>(items: readonly T[], key: string, size = 25): { shown: T[]; pager: ReactNode } {
  const { t } = useTranslation();
  const [at, setAt] = useState<{ key: string; page: number }>({ key, page: 0 });
  const page = at.key === key ? at.page : 0;
  const setPage = (next: number) => setAt({ key, page: next });
  const pages = Math.max(1, Math.ceil(items.length / size));
  const current = Math.min(page, pages - 1);
  const shown = items.slice(current * size, current * size + size);
  const pager =
    pages > 1 ? (
      <View testID="pagination" style={styles.pager}>
        <Button variant="quiet" icon="chevron-back" label={t("reports.previous")} disabled={current === 0} onPress={() => setPage(current - 1)} testID="page-previous" />
        <Txt role="meta" numeric>
          {t("reports.page", { page: current + 1, pages })}
        </Txt>
        <Button variant="quiet" icon="chevron-next" iconPosition="trailing" label={t("reports.next")} disabled={current >= pages - 1} onPress={() => setPage(current + 1)} testID="page-next" />
      </View>
    ) : (
      <View testID="pagination" />
    );
  return { shown, pager };
}

const styles = StyleSheet.create({
  figures: { flexDirection: "row", flexWrap: "wrap", gap: 12 },
  figure: { flexGrow: 1, flexShrink: 1, flexBasis: 140, gap: 4, padding: 12, borderWidth: 1 },
  bars: { gap: 8 },
  barBlock: { gap: 4 },
  bar: { gap: 6, padding: 10, borderWidth: 1, minHeight: 44 },
  barText: { flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 8 },
  grow: { flexGrow: 1, flexShrink: 1, flexBasis: 120 },
  track: { height: 8, overflow: "hidden" },
  fill: { height: 8 },
  rows: { gap: 4, paddingStart: 12 },
  pager: { flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 8 },
});

/** A bar's rows, a page at a time (a project with many people). */
export function PagedRows({ rows, pageKey }: { rows: readonly ReactNode[]; pageKey: string }) {
  const { shown, pager } = usePage(rows, pageKey);
  return (
    <>
      {shown}
      {pager}
    </>
  );
}
