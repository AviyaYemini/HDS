/**
 * The deck's building blocks for the CEO demo (specs/demo-ceo/plan.md, slides 9 and 14): numbered
 * key figures ("01 HOURS" over a big tabular number), grey-and-black paired bars with lighter
 * forecast columns, and a bordered section heading. Every number shown is one the API answered;
 * the client never adds money up.
 */
import type { ReactNode } from "react";
import { StyleSheet, View } from "react-native";

import { Txt } from "../../design/components";
import { useTheme } from "../../design/theme/ThemeProvider";
import { webProps } from "../../design/web";

/** One numbered key figure. */
export interface KeyFigure {
  key: string;
  label: string;
  value: string;
  /** A smaller word after the value ("/ 14 active"). */
  unit?: string;
  detail?: string;
  /** The API's own value, as `data-value` for the tests. */
  raw?: string;
  tone?: "primary" | "danger" | "success";
}

/** "01", "02"… the deck's numbering. */
export function ordinal(index: number): string {
  return String(index + 1).padStart(2, "0");
}

export function KeyFigures({ figures }: { figures: readonly KeyFigure[] }) {
  const { colors } = useTheme();
  return (
    <View style={styles.figures} testID="key-figures">
      {figures.map((f, i) => (
        <View key={f.key} testID={`figure-${f.key}`} {...webProps({ dataSet: { value: f.raw ?? f.value } })} style={[styles.figure, { borderTopColor: colors.border.strong }]}>
          <View style={styles.figureLabel}>
            <Txt role="eyebrow" tone="secondary" numeric>
              {ordinal(i)}
            </Txt>
            <Txt role="eyebrow" tone="accent">
              {f.label}
            </Txt>
          </View>
          <View style={styles.valueRow}>
            <Txt role="display" numeric tone={f.tone ?? "primary"} dir="ltr">
              {f.value}
            </Txt>
            {f.unit ? (
              <Txt role="meta" tone="secondary">
                {f.unit}
              </Txt>
            ) : null}
          </View>
          {f.detail ? (
            <Txt role="meta" tone="secondary">
              {f.detail}
            </Txt>
          ) : null}
        </View>
      ))}
    </View>
  );
}

/** A section's heading: an eyebrow over a strong rule, with an optional note at the far end. */
export function SectionHead({ title, note }: { title: string; note?: string }) {
  const { colors } = useTheme();
  return (
    <View style={[styles.section, { borderBottomColor: colors.border.strong }]}>
      <Txt role="eyebrow" tone="secondary" heading={2}>
        {title}
      </Txt>
      {note ? (
        <Txt role="meta" tone="secondary">
          {note}
        </Txt>
      ) : null}
    </View>
  );
}

/** One column of bars: a dark one and an optional light one beside it. */
export interface BarColumn {
  key: string;
  label: string;
  dark: number;
  light?: number | null;
  /** Drawn lighter: a forecast. */
  faded?: boolean;
  /** Read aloud and shown on hover. */
  title: string;
}

/** Vertical paired bars, scaled against the largest value shown. */
export function ColumnBars({ columns, legend, height = 120 }: { columns: readonly BarColumn[]; legend: string; height?: number }) {
  const { colors } = useTheme();
  const largest = Math.max(1, ...columns.flatMap((c) => [c.dark, c.light ?? 0]));
  const pct = (v: number) => `${Math.max(0, (v / largest) * 100)}%` as const;
  return (
    <View style={styles.barsBlock}>
      <View style={[styles.bars, { height, borderBottomColor: colors.border.strong }]} testID="column-bars">
        {columns.map((c) => (
          <View key={c.key} style={[styles.column, c.faded ? styles.faded : null]} accessibilityLabel={c.title} {...webProps({ title: c.title, dataSet: { forecast: String(!!c.faded) } })} testID="bar-column">
            <View style={[styles.bar, { height: pct(c.dark), backgroundColor: colors.text.primary }]} />
            {c.light != null ? <View style={[styles.bar, { height: pct(c.light), backgroundColor: colors.border.strong }]} /> : null}
          </View>
        ))}
      </View>
      <View style={styles.labels}>
        {columns.map((c) => (
          <Txt key={c.key} role="meta" tone="secondary" numeric style={styles.label} numberOfLines={2}>
            {c.label}
          </Txt>
        ))}
      </View>
      <Txt role="meta" tone="secondary">
        {legend}
      </Txt>
    </View>
  );
}

/** Two areas side by side on a desktop, stacked on a phone. */
export function Split({ children }: { children: ReactNode }) {
  const { isDesktop } = useTheme();
  return <View style={isDesktop ? styles.splitRow : styles.splitCol}>{children}</View>;
}

const styles = StyleSheet.create({
  figures: { flexDirection: "row", flexWrap: "wrap", gap: 24 },
  figure: { flexGrow: 1, flexShrink: 1, flexBasis: 180, borderTopWidth: 1, paddingTop: 8, gap: 2 },
  figureLabel: { flexDirection: "row", gap: 6, alignItems: "center" },
  valueRow: { flexDirection: "row", alignItems: "baseline", gap: 6, flexWrap: "wrap" },
  section: { flexDirection: "row", justifyContent: "space-between", alignItems: "flex-end", borderBottomWidth: 1, paddingBottom: 6, gap: 12, flexWrap: "wrap" },
  barsBlock: { gap: 6 },
  bars: { flexDirection: "row", alignItems: "flex-end", gap: 12, borderBottomWidth: 1 },
  column: { flex: 1, flexDirection: "row", alignItems: "flex-end", gap: 3, height: "100%" },
  faded: { opacity: 0.45 },
  bar: { flex: 1 },
  labels: { flexDirection: "row", gap: 12 },
  label: { flex: 1, textAlign: "center" },
  splitRow: { flexDirection: "row", gap: 32, alignItems: "flex-start" },
  splitCol: { gap: 24 },
});
