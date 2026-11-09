/**
 * One cell of the availability grid (specs/007-ux-redesign FR-013 to FR-016): its state in words and
 * colour, a tap to cycle and a long press for the choices; locked once the window closes.
 */
import { useTranslation } from "react-i18next";
import { Pressable, StyleSheet } from "react-native";

import { Txt, focusRing, type ChipStatus, type PressState } from "../../../design/components";
import { useKeyboardMode } from "../../../design/focus";
import { useTheme } from "../../../design/theme/ThemeProvider";
import { component } from "../../../design/theme/theme.generated";
import { webProps } from "../../../design/web";
import type { CellKey, CellState } from "./model";

/** Each state's chip tone, in the grid and its legend. */
export const STATE_CHIP: Record<CellState, ChipStatus> = { unmarked: "neutral", open: "success", close: "warning", blocked: "danger" };

export function Cell({ cellKey, state, label, disabled, onPress, onLongPress }: { cellKey: CellKey; state: CellState; label: string; disabled: boolean; onPress: () => void; onLongPress: () => void }) {
  const { colors } = useTheme();
  const keyboard = useKeyboardMode();
  const { t } = useTranslation();
  const marked = state !== "unmarked";
  const tone = marked ? colors.status[STATE_CHIP[state] as "success" | "warning" | "danger"] : colors.text.secondary;
  return (
    <Pressable
      role="button"
      testID="availability-cell"
      accessibilityLabel={label}
      accessibilityState={{ disabled }}
      aria-disabled={disabled}
      disabled={disabled}
      onPress={onPress}
      onLongPress={onLongPress}
      {...webProps({ dataSet: { date: cellKey.date, shiftType: cellKey.shiftType, state } })}
      style={(pressState) => [
        styles.cell,
        { borderColor: marked ? tone : colors.border.subtle, backgroundColor: marked ? colors.bg.selected : colors.bg.canvas },
        disabled ? styles.cellDisabled : null,
        (pressState as PressState).hovered || pressState.pressed ? { borderColor: colors.border.strong } : null,
        keyboard && (pressState as PressState).focused ? focusRing(colors.focus.ring) : null,
      ]}
    >
      <Txt role="eyebrow" style={{ color: tone }} numberOfLines={1}>
        {t(`availability.state_short.${state}`)}
      </Txt>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  cell: { flex: 1, minWidth: 0, minHeight: component.target.phone, margin: 2, borderWidth: 1, alignItems: "center", justifyContent: "center" },
  cellDisabled: { opacity: 0.6 },
});
