/**
 * The deck's building blocks for the CEO demo (specs/demo-ceo/plan.md, slides 9 and 14): numbered
 * key figures ("01 HOURS" over a big tabular number), grey-and-black paired bars with lighter
 * forecast columns, and a bordered section heading. Every number shown is one the API answered;
 * the client never adds money up.
 */
import { Children, cloneElement, isValidElement, useState, type ReactNode } from "react";
import { Pressable, StyleSheet, View, type StyleProp, type ViewStyle } from "react-native";
import Svg, { Circle, Polyline } from "react-native-svg";

import { Button, Skeleton, SkeletonRows, Txt } from "../../design/components";
import { useTheme } from "../../design/theme/ThemeProvider";
import { webProps } from "../../design/web";
import { sparkPoints } from "./polish";

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
  /** Against the previous period: "▲ 12% vs the previous month", in a neutral colour. */
  change?: string | null;
  /** Counts up to `to` on entrance, drawn by `format`; `value` once the entrance is done. */
  count?: { to: number; format: (n: number) => string };
  /** A chip beside the value ("Optimal"). */
  chip?: ReactNode;
  /** The value's own test id, where a screen's tests know it by another name. */
  valueTestID?: string;
  /** The figure's own test id, in place of `figure-<key>`. */
  testID?: string;
}

/** "01", "02"… the deck's numbering. */
export function ordinal(index: number): string {
  return String(index + 1).padStart(2, "0");
}

/**
 * The figure's size: smaller than a page title, as on the deck, and smaller again when a value is
 * long (an amount with its agorot is never rounded to fit; FR-058).
 */
export function figureSize(values: readonly string[], desktop: boolean): { fontSize: number; lineHeight: number } {
  const long = values.some((v) => v.length > 12);
  const size = desktop ? (long ? 28 : 34) : long ? 22 : 26;
  return { fontSize: size, lineHeight: Math.round(size * 1.15) };
}

export function KeyFigures({ figures, progress = 1 }: { figures: readonly KeyFigure[]; progress?: number }) {
  const { colors, isDesktop } = useTheme();
  const size = figureSize(figures.map((f) => f.value), isDesktop);
  return (
    <View style={styles.figures} testID="key-figures">
      {figures.map((f, i) => (
        <View key={f.key} testID={f.testID ?? `figure-${f.key}`} {...webProps({ dataSet: { value: f.raw ?? f.value } })} style={[styles.figure, { borderTopColor: colors.border.strong }]}>
          <View style={styles.figureLabel}>
            <Txt role="eyebrow" tone="secondary" numeric>
              {ordinal(i)}
            </Txt>
            <Txt role="eyebrow" tone="accent">
              {f.label}
            </Txt>
          </View>
          <View style={styles.valueRow}>
            <Txt role="display" numeric tone={f.tone ?? "primary"} dir="ltr" style={size} testID={f.valueTestID}>
              {f.count && progress < 1 ? f.count.format(f.count.to * progress) : f.value}
            </Txt>
            {f.chip ?? null}
            {f.unit ? (
              <Txt role="meta" tone="secondary">
                {f.unit}
              </Txt>
            ) : null}
          </View>
          {f.change ? (
            <Txt role="meta" tone="secondary" numeric testID={`figure-${f.key}-change`}>
              {f.change}
            </Txt>
          ) : null}
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

/** Small numbered figures in a row (a week's summary): "01 PLACES" over a modest number. */
export function SmallFigures({ items, testID }: { items: readonly { key: string; label: string; value: string }[]; testID?: string }) {
  const { colors } = useTheme();
  return (
    <View style={styles.small} testID={testID}>
      {items.map((f, i) => (
        <View key={f.key} style={[styles.smallItem, { borderTopColor: colors.border.strong }]} testID={`total-${f.key}`}>
          <View style={styles.figureLabel}>
            <Txt role="eyebrow" tone="secondary" numeric>
              {ordinal(i)}
            </Txt>
            <Txt role="eyebrow" tone="accent">
              {f.label}
            </Txt>
          </View>
          <Txt role="section" numeric dir="ltr">
            {f.value}
          </Txt>
        </View>
      ))}
    </View>
  );
}

/**
 * One quiet line with a "show" toggle for what would otherwise be a wall of names (the owner,
 * 2026-11-13): the summary always, the rest only when asked.
 */
export function Disclosure({ summary, children, testID, showLabel, hideLabel }: { summary: string; children: ReactNode; testID: string; showLabel: string; hideLabel: string }) {
  const [open, setOpen] = useState(false);
  return (
    <View testID={testID} style={styles.disclosure}>
      <View style={styles.disclosureLine}>
        <Txt role="body" tone="secondary" style={styles.disclosureText}>
          {summary}
        </Txt>
        <Button
          variant="quiet"
          icon={open ? "chevron-up" : "chevron-down"}
          label={open ? hideLabel : showLabel}
          onPress={() => setOpen(!open)}
          testID={`${testID}-toggle`}
          web={{ "aria-expanded": String(open) }}
        />
      </View>
      {open ? children : null}
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
export function ColumnBars({ columns, legend, height = 120, progress = 1 }: { columns: readonly BarColumn[]; legend: string; height?: number; progress?: number }) {
  const { colors } = useTheme();
  const largest = Math.max(1, ...columns.flatMap((c) => [c.dark, c.light ?? 0]));
  // The bars grow from the baseline on entrance; the box keeps its height, so nothing moves.
  const pct = (v: number) => `${Math.max(0, (v / largest) * 100 * progress)}%` as const;
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

/** One card of a phone list: the name on top, a few key numbers, and chips. */
export interface CardItem {
  key: string;
  title: string;
  subtitle?: string;
  figures: readonly { label: string; value: string; tone?: "primary" | "danger" }[];
  chips?: ReactNode;
  /** What a tap opens (what the desktop row opens); none when the row opens nothing. */
  onPress?: () => void;
}

/** Below the desktop width a table becomes cards: nothing cramped, nothing scrolling sideways. */
export function CardList({ items, testID }: { items: readonly CardItem[]; testID: string }) {
  const { colors } = useTheme();
  return (
    <View role="list" testID={testID} style={styles.cards}>
      {items.map((item) => {
        const body = (
          <>
            <Txt role="label" numberOfLines={2}>
              {item.title}
            </Txt>
            {item.subtitle ? (
              <Txt role="meta" tone="secondary" numberOfLines={1}>
                {item.subtitle}
              </Txt>
            ) : null}
            <View style={styles.cardFigures}>
              {item.figures.map((f) => (
                <View key={f.label} style={styles.cardFigure}>
                  <Txt role="eyebrow" tone="secondary">
                    {f.label}
                  </Txt>
                  <Txt role="body" numeric tone={f.tone ?? "primary"} numberOfLines={1}>
                    {f.value}
                  </Txt>
                </View>
              ))}
            </View>
            {item.chips ? <View style={styles.cardChips}>{item.chips}</View> : null}
          </>
        );
        return item.onPress ? (
          <Pressable
            key={item.key}
            role="listitem"
            testID="card"
            onPress={item.onPress}
            style={({ pressed }) => [styles.card, { borderBottomColor: colors.border.subtle }, pressed ? { backgroundColor: colors.bg.selected } : null]}
          >
            {body}
          </Pressable>
        ) : (
          <View key={item.key} role="listitem" testID="card" style={[styles.card, { borderBottomColor: colors.border.subtle }]}>
            {body}
          </View>
        );
      })}
    </View>
  );
}

/**
 * Two or more areas side by side on a desktop, stacked on a phone. Stacked, an area's own `flex`
 * (its share of the row) would stretch it down the column, so it takes its natural height instead.
 */
export function Split({ children }: { children: ReactNode }) {
  const { isDesktop } = useTheme();
  if (isDesktop) return <View style={styles.splitRow}>{children}</View>;
  return (
    <View style={styles.splitCol}>
      {Children.map(children, (child) =>
        isValidElement<{ style?: StyleProp<ViewStyle> }>(child) ? cloneElement(child, { style: [child.props.style, styles.stacked] }) : child,
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  figures: { flexDirection: "row", flexWrap: "wrap", gap: 24 },
  figure: { flexGrow: 1, flexShrink: 1, flexBasis: 200, borderTopWidth: 1, paddingTop: 8, gap: 2, minWidth: 0 },
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
  small: { flexDirection: "row", flexWrap: "wrap", gap: 20 },
  disclosure: { gap: 6 },
  disclosureLine: { flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 8 },
  disclosureText: { flexShrink: 1 },
  skeleton: { gap: 28 },
  skeletonChart: { flex: 1, minWidth: 0 },
  skeletonGap: { marginTop: 8 },
  smallItem: { borderTopWidth: 1, paddingTop: 6, minWidth: 96, flexGrow: 1, flexBasis: 96 },
  hbars: { gap: 10 },
  hrow: { flexDirection: "row", alignItems: "center", gap: 16 },
  hrowPhone: { gap: 4 },
  hlabel: { width: "34%", flexShrink: 0 },
  htrackRow: { flex: 1, flexDirection: "row", alignItems: "center", gap: 10 },
  htrack: { flex: 1, height: 10 },
  hfill: { height: 10 },
  hvalue: { minWidth: 64, textAlign: "left" },
  cards: { borderTopWidth: 0 },
  card: { gap: 6, paddingVertical: 12, borderBottomWidth: 1, minHeight: 48 },
  cardFigures: { flexDirection: "row", flexWrap: "wrap", gap: 20 },
  cardFigure: { gap: 0, minWidth: 64 },
  cardChips: { flexDirection: "row", flexWrap: "wrap", gap: 6 },
  splitRow: { flexDirection: "row", gap: 32, alignItems: "flex-start" },
  splitCol: { gap: 24 },
  stacked: { flexGrow: 0, flexShrink: 0, flexBasis: "auto" },
});

/** One horizontal bar: a full name, its share against the largest, and its value. */
export interface HBar {
  key: string;
  label: string;
  value: number;
  valueText: string;
  /** Drawn light (training against security). */
  light?: boolean;
  onPress?: () => void;
}

/** Horizontal bars with the full names, nothing cut (the owner, 2026-11-13); they grow on entrance. */
export function HBars({ bars, legend, progress = 1, testID }: { bars: readonly HBar[]; legend: string; progress?: number; testID?: string }) {
  const { colors, isDesktop } = useTheme();
  const largest = Math.max(1, ...bars.map((b) => b.value));
  return (
    <View style={styles.hbars} testID={testID}>
      {bars.map((b) => {
        const fill = <View style={[styles.hfill, { width: `${(b.value / largest) * 100 * progress}%`, backgroundColor: b.light ? colors.border.strong : colors.text.primary }]} />;
        const row = (
          <View style={isDesktop ? styles.hrow : styles.hrowPhone}>
            <Txt role="label" style={isDesktop ? styles.hlabel : null}>
              {b.label}
            </Txt>
            <View style={styles.htrackRow}>
              <View style={[styles.htrack, { backgroundColor: colors.bg.selected }]}>{fill}</View>
              <Txt role="meta" tone="secondary" numeric style={styles.hvalue}>
                {b.valueText}
              </Txt>
            </View>
          </View>
        );
        return b.onPress ? (
          <Pressable key={b.key} role="link" testID="hbar" onPress={b.onPress} accessibilityLabel={`${b.label}: ${b.valueText}`}>
            {row}
          </Pressable>
        ) : (
          <View key={b.key} testID="hbar">
            {row}
          </View>
        );
      })}
      <Txt role="meta" tone="secondary">
        {legend}
      </Txt>
    </View>
  );
}

/** A small trend line (hours by week), time running with the reading direction. */
export function Sparkline({ values, rtl, width = 72, height = 22, testID }: { values: readonly number[]; rtl: boolean; width?: number; height?: number; testID?: string }) {
  const { colors } = useTheme();
  if (values.length < 2) return null;
  // Oldest first, left to right; in Hebrew the oldest sits on the right, as the bars and candles do.
  const points = sparkPoints(rtl ? [...values].reverse() : values, width, height);
  const last = points.split(" ")[rtl ? 0 : values.length - 1]!.split(",");
  return (
    <View testID={testID} accessibilityElementsHidden importantForAccessibility="no-hide-descendants" {...webProps({ "aria-hidden": String(true) })}>
      <Svg width={width} height={height}>
        <Polyline points={points} fill="none" stroke={colors.border.strong} strokeWidth={1.5} />
        <Circle cx={Number(last[0])} cy={Number(last[1])} r={2.5} fill={colors.text.primary} />
      </Svg>
    </View>
  );
}

/**
 * A dashboard's loading shape (the owner, 2026-11-13): the strip of figures, the chart blocks and
 * the table's rows where they will be, so nothing jumps when the answer comes.
 */
export function DashboardSkeleton({ figures = 4, charts = 2, chartHeight = 150, rows = 6, testID = "dashboard-skeleton" }: { figures?: number; charts?: number; chartHeight?: number; rows?: number; testID?: string }) {
  const { colors } = useTheme();
  return (
    <View style={styles.skeleton} testID={testID}>
      <View style={styles.figures}>
        {Array.from({ length: figures }, (_, i) => (
          <View key={i} style={[styles.figure, { borderTopColor: colors.border.strong }]}>
            <Skeleton width={90} height={10} />
            <Skeleton width="70%" height={30} style={styles.skeletonGap} />
            <Skeleton width="55%" height={10} style={styles.skeletonGap} />
          </View>
        ))}
      </View>
      {charts > 0 ? (
        <Split>
          {Array.from({ length: charts }, (_, i) => (
            <View key={i} style={styles.skeletonChart}>
              <Skeleton width={140} height={10} />
              <Skeleton height={chartHeight} style={styles.skeletonGap} />
            </View>
          ))}
        </Split>
      ) : null}
      {rows > 0 ? <SkeletonRows rows={rows} /> : null}
    </View>
  );
}
