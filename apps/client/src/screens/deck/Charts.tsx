/**
 * The overview's charts in the deck's light, low-contrast look (the owner, 2026-11-13): a donut
 * with its legend and weekly candles of daily billing. Grey and black; the accent only where a
 * slice is the point (temporary workers). Each chart is one image to assistive technology, read
 * through a one-line summary; the legend's text stays readable on its own.
 */
import { useState } from "react";
import { StyleSheet, View, type LayoutChangeEvent } from "react-native";
import Svg, { G, Line, Path, Rect, Text as SvgText } from "react-native-svg";

import { isRtl } from "../../i18n";
import { Txt } from "../../design/components";
import { useTheme } from "../../design/theme/ThemeProvider";
import { webProps } from "../../design/web";
import { arcPath, niceTicks, type Candle, type Slice } from "./chartMath";

/** The greys a donut's slices take in order, dark to light; "others" takes the lightest. */
export function useSliceColors(): string[] {
  const { colors } = useTheme();
  return [colors.text.primary, colors.text.secondary, colors.border.strong, colors.border.subtle, colors.bg.selected];
}

/** One image to assistive technology, named by its summary. */
function ChartImage({ summary, testID, children }: { summary: string; testID: string; children: React.ReactNode }) {
  return (
    <View role="img" accessibilityLabel={summary} testID={testID} {...webProps({ "aria-label": summary, title: summary })}>
      {children}
    </View>
  );
}

export interface DonutProps {
  slices: readonly Slice[];
  /** A colour per slice key, overriding the greys (the accent for a highlight). */
  colorOf?: (slice: Slice, index: number) => string;
  /** The legend's amount for a slice (₪ or a count). */
  amountText: (slice: Slice) => string;
  /** In the hole: the whole, as text. */
  centre?: string;
  summary: string;
  testID: string;
}

export function Donut({ slices, colorOf, amountText, centre, summary, testID }: DonutProps) {
  const { colors } = useTheme();
  const greys = useSliceColors();
  const size = 148;
  const outer = size / 2 - 2;
  const inner = outer * 0.62;
  const colour = (s: Slice, i: number) => colorOf?.(s, i) ?? (s.others ? greys[greys.length - 1]! : greys[Math.min(i, greys.length - 2)]!);
  // Each slice starts where the ones before it end, clockwise from 12 o'clock.
  const starts = slices.map((_, i) => slices.slice(0, i).reduce((a, p) => a + (p.pct / 100) * Math.PI * 2, 0));
  const arcs = slices.map((s, i) => {
    const a0 = starts[i]!;
    const path = arcPath(size / 2, size / 2, outer, inner, a0, a0 + (s.pct / 100) * Math.PI * 2);
    return <Path key={s.key} d={path} fill={colour(s, i)} stroke={colors.bg.canvas} strokeWidth={slices.length > 1 ? 1.5 : 0} />;
  });
  return (
    <View style={styles.donut}>
      <ChartImage summary={summary} testID={testID}>
        <View style={{ width: size, height: size }}>
          <Svg width={size} height={size} viewBox={`0 0 ${size} ${size}`}>
            <G>{arcs}</G>
          </Svg>
          {centre ? (
            <View style={styles.centre} pointerEvents="none">
              <Txt role="label" numeric dir="ltr">
                {centre}
              </Txt>
            </View>
          ) : null}
        </View>
      </ChartImage>
      <View style={styles.legend}>
        {slices.map((s, i) => (
          <View key={s.key} style={styles.legendRow} testID={`${testID}-legend`}>
            <View style={[styles.swatch, { backgroundColor: colour(s, i), borderColor: colors.border.strong }]} />
            {/* The name on its own line, the share and the amount under it: nothing cut or wrapped mid-number. */}
            <View style={styles.legendText}>
              <Txt role="meta" numberOfLines={1}>
                {s.label}
              </Txt>
              <Txt role="meta" tone="secondary" numeric numberOfLines={1}>
                {`${Math.round(s.pct)}% · ${amountText(s)}`}
              </Txt>
            </View>
          </View>
        ))}
      </View>
    </View>
  );
}

export interface CandlesProps {
  candles: readonly Candle[];
  /** A week's label under its candle ("1.11"). */
  labelOf: (candle: Candle) => string;
  /** A scale tick as text ("₪12K"). */
  tickText: (value: number) => string;
  summary: string;
  testID: string;
  height?: number;
}

/** Weekly candles, time running from the reading start (right to left in Hebrew), with a ₪ scale. */
export function Candles({ candles, labelOf, tickText, summary, testID, height = 180 }: CandlesProps) {
  const { colors } = useTheme();
  const [width, setWidth] = useState(320);
  const onLayout = (e: LayoutChangeEvent) => setWidth(Math.max(200, Math.round(e.nativeEvent.layout.width)));
  const rtl = isRtl();
  const axis = 52;
  const bottom = 22;
  const top = 6;
  const lows = candles.map((c) => c.low);
  const highs = candles.map((c) => c.high);
  // The scale spans the candles, not from zero, so a week's shape can be read.
  const ticks = niceTicks(Math.min(...lows), Math.max(...highs, Math.min(...lows) + 1));
  const min = ticks[0]!;
  const max = ticks[ticks.length - 1]!;
  const plot = width - axis;
  const y = (v: number) => top + (1 - (v - min) / (max - min || 1)) * (height - top - bottom);
  const slot = plot / Math.max(1, candles.length);
  const body = Math.max(4, Math.min(22, slot * 0.5));
  // The scale sits on the left; time runs from the reading start: left to right in English, right
  // to left in Hebrew (matching the bars), so the latest week is nearest the scale there.
  const centreX = (i: number) => (rtl ? width - (i + 0.5) * slot : axis + (i + 0.5) * slot);
  return (
    <ChartImage summary={summary} testID={testID}>
      <View onLayout={onLayout} style={styles.candles}>
        <Svg width={width} height={height}>
          {ticks.map((v) => (
            <G key={v}>
              <Line x1={axis} x2={width} y1={y(v)} y2={y(v)} stroke={colors.border.subtle} strokeWidth={1} />
              <SvgText x={axis - 6} y={y(v) + 4} fontSize={10} fill={colors.text.secondary} textAnchor="end">
                {tickText(v)}
              </SvgText>
            </G>
          ))}
          {candles.map((c, i) => {
            const x = centreX(i);
            const bodyTop = y(Math.max(c.open, c.close));
            const bodyHeight = Math.max(1.5, Math.abs(y(c.open) - y(c.close)));
            return (
              <G key={c.week} opacity={c.forecast ? 0.45 : 1}>
                <Line x1={x} x2={x} y1={y(c.high)} y2={y(c.low)} stroke={colors.text.primary} strokeWidth={1} />
                <Rect
                  x={x - body / 2}
                  y={bodyTop}
                  width={body}
                  height={bodyHeight}
                  fill={c.rising ? colors.text.primary : colors.bg.canvas}
                  stroke={colors.text.primary}
                  strokeWidth={1}
                />
                <SvgText x={x} y={height - 6} fontSize={10} fill={colors.text.secondary} textAnchor="middle">
                  {labelOf(c)}
                </SvgText>
              </G>
            );
          })}
        </Svg>
      </View>
    </ChartImage>
  );
}

const styles = StyleSheet.create({
  donut: { flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 16 },
  centre: { position: "absolute", top: 0, bottom: 0, start: 0, end: 0, alignItems: "center", justifyContent: "center" },
  legend: { flexGrow: 1, flexShrink: 1, flexBasis: 150, gap: 8 },
  legendRow: { flexDirection: "row", alignItems: "flex-start", gap: 8 },
  legendText: { flexGrow: 1, flexShrink: 1, minWidth: 0 },
  swatch: { width: 10, height: 10, borderWidth: 1, marginTop: 5 },
  candles: { width: "100%" },
});
