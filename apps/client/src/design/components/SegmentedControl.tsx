/**
 * A segmented control for two to five choices (specs/007-ux-redesign FR-044, FR-063).
 *
 * The active segment takes `bg.selected`, a 2 px accent rule on top and the `accent.onSelected`
 * text, like the header's active item. A radio group for assistive technology: arrow keys move the
 * choice on the web, one segment is in the tab order at a time.
 */
import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { Pressable, ScrollView, StyleSheet, View, type StyleProp, type ViewStyle } from "react-native";

import { isRtlLanguage } from "../../i18n";
import { useKeyboardMode } from "../focus";
import { Icon, type IconName } from "../icons/Icon";
import { useTheme } from "../theme/ThemeProvider";
import { component } from "../theme/theme.generated";
import { webProps, type WebKeyEvent } from "../web";
import { focusRing, type PressState } from "./Button";
import { Txt } from "./Text";

/** Anything the web can move focus to. */
type Focusable = { focus?: () => void };

export interface Segment<T extends string> {
  value: T;
  label: string;
  icon?: IconName;
  testID?: string;
}

export interface SegmentedControlProps<T extends string> {
  options: readonly Segment<T>[];
  value: T;
  onChange: (value: T) => void;
  /** Names the group for assistive technology. */
  label: string;
  disabled?: boolean;
  testID?: string;
  style?: StyleProp<ViewStyle>;
}

/** A segmented control. */
export function SegmentedControl<T extends string>({
  options,
  value,
  onChange,
  label,
  disabled = false,
  testID,
  style,
}: SegmentedControlProps<T>) {
  const { colors, density, isDesktop } = useTheme();
  const keyboard = useKeyboardMode();
  const { i18n } = useTranslation();
  const rtl = isRtlLanguage(i18n.language);
  const refs = useRef<(Focusable | null)[]>([]);
  const height = density === "compact" && isDesktop ? 36 : component.target.phone;

  const [focusIndex, setFocusIndex] = useState<number | null>(null);
  useEffect(() => {
    if (focusIndex !== null) refs.current[focusIndex]?.focus?.();
  }, [focusIndex]);

  const move = (from: number, step: number): void => {
    const next = (from + step + options.length) % options.length;
    const option = options[next]!;
    onChange(option.value);
    setFocusIndex(next);
  };

  // The forward arrow is the one that points forward in the language: Left in Hebrew.
  const forward = rtl ? "ArrowLeft" : "ArrowRight";
  const back = rtl ? "ArrowRight" : "ArrowLeft";
  const onKeyDown = (index: number) => (event: WebKeyEvent) => {
    if (event.key === forward || event.key === "ArrowDown") {
      event.preventDefault();
      move(index, 1);
    } else if (event.key === back || event.key === "ArrowUp") {
      event.preventDefault();
      move(index, -1);
    } else if (event.key === "Home") {
      event.preventDefault();
      move(0, 0);
    } else if (event.key === "End") {
      event.preventDefault();
      move(options.length - 1, 0);
    }
  };

  const group = (
    <View
      testID={testID}
      role="radiogroup"
      accessibilityLabel={label}
      style={[styles.group, { borderColor: colors.border.strong }, isDesktop ? style : null]}
    >
      {options.map((option, index) => {
        const selected = option.value === value;
        return (
          <Pressable
            key={option.value}
            ref={(node) => {
              refs.current[index] = node as unknown as Focusable | null;
            }}
            testID={option.testID}
            role="radio"
            accessibilityLabel={option.label}
            accessibilityState={{ checked: selected, disabled }}
            aria-checked={selected}
            disabled={disabled}
            onPress={() => onChange(option.value)}
            {...webProps({ tabIndex: selected ? 0 : -1, onKeyDown: onKeyDown(index) })}
            style={(state) => {
              const s = state as PressState;
              return [
                styles.segment,
                { minHeight: height, borderTopColor: selected ? colors.accent.default : "transparent" },
                selected ? { backgroundColor: colors.bg.selected } : null,
                !selected && (s.hovered || s.pressed) && !disabled ? { backgroundColor: colors.bg.surface } : null,
                index > 0 ? { borderStartWidth: 1, borderStartColor: colors.border.subtle } : null,
                keyboard && s.focused ? focusRing(colors.focus.ring) : null,
              ];
            }}
          >
            {option.icon ? (
              <Icon name={option.icon} size={16} color={selected ? colors.accent.onSelected : colors.text.secondary} />
            ) : null}
            <Txt
              role="label"
              tone="inherit"
              numberOfLines={1}
              style={{ color: disabled ? colors.text.secondary : selected ? colors.accent.onSelected : colors.text.primary }}
            >
              {option.label}
            </Txt>
          </Pressable>
        );
      })}
    </View>
  );
  // Five Hebrew labels overflow a phone; the group then scrolls sideways rather than clipping.
  if (isDesktop) return group;
  return (
    <ScrollView horizontal showsHorizontalScrollIndicator={false} style={style} contentContainerStyle={styles.scroll}>
      {group}
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  group: { flexDirection: "row", borderWidth: 1, alignSelf: "flex-start" },
  scroll: { flexGrow: 0 },
  segment: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "center",
    gap: 6,
    paddingHorizontal: 14,
    borderTopWidth: component.header.activeRule,
  },
});
