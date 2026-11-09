/**
 * Buttons (specs/007-ux-redesign FR-063, FR-064, FR-008; docs/plans/ux-working-plan.md section 3).
 *
 * `primary` is the one action a screen leads with: `cta.bg` with a 14 px clipped corner.
 * `secondary` has a 1 px strong border, `quiet` has none, `link` is text in the accent. Every
 * variant has the same states: default, hover, pressed, focused (a 2 px ring, keyboard only on the
 * web), disabled but legible (with a reason when there is one), and busy (a spinner beside the
 * label, which stays readable). Targets are 44 px tall at comfortable density, 36 px at compact.
 */
import { useContext, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  ActivityIndicator,
  Animated,
  Pressable,
  StyleSheet,
  View,
  type PressableStateCallbackType,
  type StyleProp,
  type ViewStyle,
} from "react-native";

import { useKeyboardMode } from "../focus";
import { Icon, type IconName } from "../icons/Icon";
import { animateTo, useMotion } from "../motion";
import { useTheme } from "../theme/ThemeProvider";
import { component } from "../theme/theme.generated";
import { webProps, type WebOnlyProps } from "../web";
import { ClippedBox } from "./ClippedBox";
import { SurfaceContext, Txt } from "./Text";

/** The kinds of button. */
export type ButtonVariant = "primary" | "secondary" | "quiet" | "link";

/** React Native Web adds `hovered` and `focused` to the press state. */
export type PressState = PressableStateCallbackType & { hovered?: boolean; focused?: boolean };

export interface ButtonProps {
  label: string;
  onPress?: () => void;
  variant?: ButtonVariant;
  icon?: IconName;
  iconPosition?: "leading" | "trailing";
  /** Blocked for now; say why with `disabledReason` (FR-008). */
  disabled?: boolean;
  disabledReason?: string;
  /** Working; the label stays, a spinner joins it, presses are ignored. */
  busy?: boolean;
  /** Stretch to the container's width. */
  block?: boolean;
  /** Let a long label wrap onto more lines instead of cutting it to one (a list of places). */
  wrap?: boolean;
  testID?: string;
  accessibilityLabel?: string;
  accessibilityHint?: string;
  /** Web-only attributes for the pressable (`aria-haspopup`, `aria-expanded`). */
  web?: WebOnlyProps;
  style?: StyleProp<ViewStyle>;
}

/** The ring every focused control draws on the web in keyboard mode (X4). */
export function focusRing(color: string): ViewStyle {
  return { outlineStyle: "solid", outlineWidth: 2, outlineColor: color, outlineOffset: 2 };
}

/** A button. */
export function Button({
  label,
  onPress,
  variant = "secondary",
  icon,
  iconPosition = "leading",
  disabled = false,
  disabledReason,
  busy = false,
  block,
  wrap = false,
  testID,
  accessibilityLabel,
  accessibilityHint,
  web,
  style,
}: ButtonProps) {
  const { colors, density, isDesktop } = useTheme();
  const surface = useContext(SurfaceContext);
  const keyboard = useKeyboardMode();
  const motion = useMotion();
  const { t } = useTranslation();
  const scale = useRef(new Animated.Value(1)).current;
  const [hover, setHover] = useState(false);
  const inert = disabled || busy;
  const height = density === "compact" && isDesktop ? 36 : component.target.phone;

  const press = (to: number): void => {
    void animateTo(scale, to, motion.transition("feedback"));
  };

  const textColor = (state: PressState): string => {
    if (variant === "primary") return colors.cta.text;
    // On a dark (inverse) surface, as a toast's action, the accent fails contrast (008 R57): a
    // link or quiet button takes the surface's own text colours, and the link keeps its underline.
    if (surface === "inverse" && (variant === "link" || variant === "quiet")) {
      return disabled ? colors.inverse.textSecondary : colors.inverse.text;
    }
    if (disabled) return colors.text.secondary;
    if (variant === "secondary") return colors.text.primary;
    return state.pressed ? colors.accent.pressed : colors.accent.default;
  };

  const frame = (state: PressState): ViewStyle => {
    const active = !inert && (state.pressed || state.hovered || hover);
    return {
      minHeight: variant === "link" && isDesktop ? undefined : height,
      // An icon-only button is as wide as it is tall, so it stays a full target.
      ...(label === "" ? { minWidth: height, alignItems: "center" as const } : {}),
      paddingHorizontal: variant === "link" ? 0 : variant === "quiet" ? 12 : 16,
      paddingVertical: variant === "link" ? 2 : 0,
      justifyContent: "center",
      ...(variant === "secondary"
        ? {
            borderWidth: 1,
            borderColor: disabled ? colors.border.subtle : state.pressed ? colors.text.primary : colors.border.strong,
          }
        : {}),
      ...(active && variant !== "primary" && variant !== "link" ? { backgroundColor: colors.bg.selected } : {}),
      ...(active && variant === "primary" ? { opacity: state.pressed ? 0.82 : 0.92 } : {}),
      ...(keyboard && state.focused ? focusRing(colors.focus.ring) : {}),
    };
  };

  const content = (state: PressState) => {
    const color = textColor(state);
    const glyph = busy ? (
      motion.reduced ? (
        <Icon name="more" size={18} color={color} />
      ) : (
        <ActivityIndicator size="small" color={color} accessibilityLabel={t("common.loading")} />
      )
    ) : icon ? (
      <Icon name={icon} size={18} color={color} />
    ) : null;
    // Always underlined on an inverse surface, where its colour is the text's (R57).
    const underline = variant === "link" && (surface === "inverse" || state.hovered || hover || state.pressed);
    return (
      <View style={styles.row}>
        {iconPosition === "leading" ? glyph : null}
        <Txt
          role="label"
          tone="inherit"
          testID={variant === "link" ? testID : undefined}
          style={[{ color }, underline ? styles.underline : null, variant === "link" ? styles.linkText : null, wrap ? styles.wrap : null]}
          numberOfLines={wrap ? undefined : 1}
        >
          {label}
        </Txt>
        {iconPosition === "trailing" ? glyph : null}
      </View>
    );
  };

  return (
    <View style={[block ? styles.block : styles.inline, style]}>
      <Pressable
        testID={variant === "primary" || variant === "link" ? undefined : testID}
        role="button"
        accessibilityLabel={accessibilityLabel}
        accessibilityHint={accessibilityHint ?? disabledReason}
        accessibilityState={{ disabled: inert, busy }}
        aria-disabled={inert}
        aria-busy={busy}
        disabled={inert}
        onPress={onPress}
        onPressIn={() => press(0.97)}
        onPressOut={() => press(1)}
        onHoverIn={() => setHover(true)}
        onHoverOut={() => setHover(false)}
        {...webProps({ title: disabledReason, ...web })}
        style={(state) => [styles.pressable, variant === "primary" ? null : frame(state as PressState)]}
      >
        {(state) => (
          <Animated.View style={{ transform: [{ scale }] }}>
            {variant === "primary" ? (
              <ClippedBox
                testID={testID}
                clip={component.clip.cta}
                color={disabled ? colors.border.subtle : colors.cta.bg}
                style={frame(state as PressState)}
              >
                {content(state as PressState)}
              </ClippedBox>
            ) : (
              content(state as PressState)
            )}
          </Animated.View>
        )}
      </Pressable>
      {disabled && disabledReason ? (
        <Txt role="meta" tone="secondary" style={styles.reason} testID={testID ? `${testID}-reason` : undefined}>
          {disabledReason}
        </Txt>
      ) : null}
      {busy ? (
        <Txt role="meta" tone="secondary" style={styles.hidden} accessibilityLiveRegion="polite">
          {t("common.loading")}
        </Txt>
      ) : null}
    </View>
  );
}

const styles = StyleSheet.create({
  inline: { alignSelf: "flex-start" },
  block: { alignSelf: "stretch" },
  pressable: { alignSelf: "stretch" },
  row: { flexDirection: "row", alignItems: "center", justifyContent: "center", gap: 8, minHeight: 20 },
  wrap: { flexShrink: 1 },
  underline: { textDecorationLine: "underline" },
  linkText: { textDecorationLine: "none" },
  reason: { marginTop: 4 },
  hidden: { position: "absolute", width: 1, height: 1, overflow: "hidden", opacity: 0 },
});
