/**
 * The loading bar (specs/007-ux-redesign FR-074, research R15, task T022).
 *
 * A 3 px bar: a `cta.bg` track with an `accent` fill whose leading end is clipped. Indeterminate,
 * a 30 % segment sweeps across (reduce-motion: a still, half-filled bar); determinate, the fill
 * grows to the value with the side text beside it. Exposed as a progress bar with its value, and
 * the value is announced at every quarter. Rendered only while the store says so, so a fast load
 * never shows it (300 ms grace, 400 ms minimum once shown).
 */
import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { AccessibilityInfo, Animated, Platform, StyleSheet, View, type LayoutChangeEvent } from "react-native";
import Svg, { Polygon } from "react-native-svg";

import { isRtlLanguage } from "../../i18n";
import { useLoadingState } from "../loading/loadingStore";
import { startLoop, useReducedMotion } from "../motion";
import { useTheme } from "../theme/ThemeProvider";
import { component } from "../theme/theme.generated";
import { webProps } from "../web";
import { Txt } from "./Text";

const HEIGHT = component.loadingBar.height;
const SEGMENT = 0.3;
const SWEEP_MS = 1200;

function Fill({ width, color, rtl }: { width: number; color: string; rtl: boolean }) {
  // The leading end is cut at 45°: the top leading corner moves in by the bar's height.
  const points = rtl
    ? `0,0 ${width - HEIGHT},0 ${width},${HEIGHT} 0,${HEIGHT}`
    : `${HEIGHT},0 ${width},0 ${width},${HEIGHT} 0,${HEIGHT}`;
  return (
    <Svg width={width} height={HEIGHT} viewBox={`0 0 ${width} ${HEIGHT}`} pointerEvents="none">
      <Polygon points={points} fill={color} />
    </Svg>
  );
}

/** The bar; mount it once, under the header. */
export function LoadingBar({ testID = "loading-bar" }: { testID?: string }) {
  const state = useLoadingState();
  const { colors } = useTheme();
  const { t, i18n } = useTranslation();
  const reduced = useReducedMotion();
  const rtl = isRtlLanguage(i18n.language);
  const [width, setWidth] = useState(0);
  const [sweep] = useState(() => new Animated.Value(0));
  const lastQuarter = useRef(-1);

  const indeterminate = state.visible && state.value === null;
  useEffect(() => {
    if (!indeterminate || reduced || width === 0) {
      sweep.stopAnimation();
      sweep.setValue(0);
      return undefined;
    }
    return startLoop(sweep, SWEEP_MS);
  }, [indeterminate, reduced, width, sweep]);

  const percent = state.value === null ? null : Math.round(state.value * 100);
  const label = state.text ?? t("common.loading");
  const liveProps = webProps({ "aria-live": "polite" });
  const valueTextProps = webProps({ "aria-valuetext": percent === null ? label : undefined });
  useEffect(() => {
    if (percent === null) {
      lastQuarter.current = -1;
      return;
    }
    const quarter = Math.floor(percent / 25);
    if (quarter !== lastQuarter.current) {
      lastQuarter.current = quarter;
      if (Platform.OS !== "web") AccessibilityInfo.announceForAccessibility(`${percent}%`);
    }
  }, [percent]);

  if (!state.visible) return null;

  const segment = Math.max(1, Math.round(width * SEGMENT));
  const translateX = sweep.interpolate({
    inputRange: [0, 1],
    outputRange: rtl ? [width, -segment] : [-segment, width],
  });
  const fillWidth = percent === null ? (reduced ? Math.round(width / 2) : segment) : Math.round((width * percent) / 100);
  const onLayout = (event: LayoutChangeEvent): void => setWidth(Math.round(event.nativeEvent.layout.width));

  return (
    <View style={styles.row} testID={`${testID}-row`}>
      <View
        testID={testID}
        role="progressbar"
        accessibilityLabel={label}
        accessibilityValue={percent === null ? { text: label } : { min: 0, max: 100, now: percent }}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={percent ?? undefined}
        aria-busy
        {...valueTextProps}
        onLayout={onLayout}
        style={[styles.track, { backgroundColor: colors.cta.bg }]}
      >
        {width > 0 ? (
          percent === null && !reduced ? (
            <Animated.View style={[styles.fill, { width: segment, transform: [{ translateX }] }]}>
              <Fill width={segment} color={colors.accent.default} rtl={rtl} />
            </Animated.View>
          ) : (
            <View style={[styles.fill, { width: fillWidth, start: 0 }]}>
              {fillWidth > 0 ? <Fill width={fillWidth} color={colors.accent.default} rtl={rtl} /> : null}
            </View>
          )
        ) : null}
      </View>
      {state.text ? (
        <Txt role="meta" tone="secondary" numeric style={styles.text}>
          {state.text}
        </Txt>
      ) : null}
      <Txt role="meta" tone="secondary" accessibilityLiveRegion="polite" style={styles.hidden} {...liveProps}>
        {percent === null ? label : `${percent}%`}
      </Txt>
    </View>
  );
}

const styles = StyleSheet.create({
  row: { flexDirection: "row", alignItems: "center", gap: 12 },
  track: { flex: 1, height: HEIGHT, overflow: "hidden" },
  fill: { position: "absolute", top: 0, bottom: 0 },
  text: { flexShrink: 0 },
  hidden: { position: "absolute", width: 1, height: 1, overflow: "hidden", opacity: 0 },
});
