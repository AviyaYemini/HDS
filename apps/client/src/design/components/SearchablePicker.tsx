/**
 * The searchable picker for long lists (specs/007-ux-redesign FR-044, A6): employees, projects,
 * clients and sites. A field-like trigger opens a sheet with a search box and the matches, recent
 * items first, each with a second line. Typing part of a Hebrew or English name matches, since an
 * item carries every name it goes by. Two to five options should use a `SegmentedControl` instead.
 */
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Pressable, StyleSheet, View, type StyleProp, type ViewStyle } from "react-native";

import { useKeyboardMode } from "../focus";
import { Icon } from "../icons/Icon";
import { useTheme } from "../theme/ThemeProvider";
import { component } from "../theme/theme.generated";
import { webProps, type WebKeyEvent } from "../web";
import { focusRing, type PressState } from "./Button";
import { Sheet } from "./Sheet";
import { Txt } from "./Text";
import { TextField } from "./TextField";

export interface PickerItem {
  id: string;
  label: string;
  /** The second line: a role, a site, a client. */
  secondary?: string;
  /** Other names it answers to (the Hebrew or English spelling, a code). */
  keywords?: readonly string[];
}

export interface SearchablePickerProps {
  label: string;
  items: readonly PickerItem[];
  value: string | null;
  onChange: (id: string) => void;
  /** Ids chosen lately, listed first. */
  recentIds?: readonly string[];
  placeholder?: string;
  disabled?: boolean;
  error?: string | null;
  testID?: string;
  style?: StyleProp<ViewStyle>;
}

const MARKS = /[֑-ׇ̀-ͯ]/g;

/** Text lowered, without vowel points or punctuation, for matching. */
export function normalise(text: string): string {
  return text
    .normalize("NFD")
    .replace(MARKS, "")
    .toLowerCase()
    .replace(/["'`״׳.,\-_/()]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/** Whether `item` matches `query`: a word of any of its names starts with the query, or contains it. */
export function matches(item: PickerItem, query: string): boolean {
  const q = normalise(query);
  if (!q) return true;
  const names = [item.label, item.secondary ?? "", ...(item.keywords ?? [])].map(normalise);
  return names.some((name) => name.includes(q) || name.split(" ").some((word) => word.startsWith(q)));
}

/** The items to show for `query`: recent matches first, then the rest, in their given order. */
export function order(items: readonly PickerItem[], recentIds: readonly string[], query: string): PickerItem[] {
  const found = items.filter((item) => matches(item, query));
  const recent = recentIds.map((id) => found.find((item) => item.id === id)).filter((item): item is PickerItem => !!item);
  const rest = found.filter((item) => !recentIds.includes(item.id));
  return [...recent, ...rest];
}

/** A searchable picker. */
export function SearchablePicker({
  label,
  items,
  value,
  onChange,
  recentIds = [],
  placeholder,
  disabled = false,
  error,
  testID,
  style,
}: SearchablePickerProps) {
  const { colors, density, isDesktop } = useTheme();
  const { t } = useTranslation();
  const keyboard = useKeyboardMode();
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [highlight, setHighlight] = useState(0);
  const chosen = items.find((item) => item.id === value) ?? null;
  const shown = useMemo(() => order(items, recentIds, query), [items, recentIds, query]);
  const height = density === "compact" && isDesktop ? 36 : component.target.phone;
  const triggerWebProps = webProps({ "aria-haspopup": "dialog" });

  const choose = (item: PickerItem): void => {
    onChange(item.id);
    setOpen(false);
    setQuery("");
  };

  const onKeyDown = (event: WebKeyEvent): void => {
    if (event.key === "ArrowDown") {
      event.preventDefault();
      setHighlight((h) => Math.min(shown.length - 1, h + 1));
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      setHighlight((h) => Math.max(0, h - 1));
    } else if (event.key === "Enter" && shown[highlight]) {
      event.preventDefault();
      choose(shown[highlight]);
    }
  };

  const searchWebProps = webProps({ onKeyDown });

  return (
    <View style={[styles.root, style]}>
      <Txt role="meta" tone="secondary">
        {label}
      </Txt>
      <Pressable
        testID={testID}
        role="button"
        accessibilityLabel={`${label}: ${chosen?.label ?? placeholder ?? t("picker.choose")}`}
        accessibilityState={{ disabled, expanded: open }}
        aria-expanded={open}
        aria-disabled={disabled}
        disabled={disabled}
        onPress={() => {
          setHighlight(0);
          setOpen(true);
        }}
        {...triggerWebProps}
        style={(state) => [
          styles.trigger,
          { minHeight: height, borderColor: error ? colors.status.danger : (state as PressState).hovered ? colors.text.primary : colors.border.strong },
          keyboard && (state as PressState).focused ? focusRing(colors.focus.ring) : null,
        ]}
      >
        <View style={styles.triggerText}>
          <Txt role="body" tone={chosen ? "primary" : "secondary"} numberOfLines={1}>
            {chosen?.label ?? placeholder ?? t("picker.choose")}
          </Txt>
          {chosen?.secondary ? (
            <Txt role="meta" tone="secondary" numberOfLines={1}>
              {chosen.secondary}
            </Txt>
          ) : null}
        </View>
        <Icon name="chevron-down" size={18} color={colors.text.secondary} />
      </Pressable>
      {error ? (
        <Txt role="meta" tone="danger" a11yRole="alert">
          {error}
        </Txt>
      ) : null}
      <Sheet open={open} onClose={() => setOpen(false)} title={label} testID={testID ? `${testID}-sheet` : undefined}>
        <TextField
          label={t("picker.search")}
          hideLabel
          placeholder={t("picker.search")}
          value={query}
          onChangeText={(text) => {
            setQuery(text);
            setHighlight(0);
          }}
          autoFocus
          autoCorrect={false}
          testID={testID ? `${testID}-search` : undefined}
          trailing={<Icon name="search" size={18} color={colors.text.secondary} style={styles.searchIcon} />}
          {...searchWebProps}
        />
        <View role="list" accessibilityLabel={label} style={styles.list}>
          {shown.length === 0 ? (
            <Txt role="body" tone="secondary" style={styles.nothing}>
              {t("picker.nothing")}
            </Txt>
          ) : null}
          {shown.map((item, index) => {
            const selected = item.id === value;
            const recent = recentIds.includes(item.id);
            return (
              <Pressable
                key={item.id}
                role="listitem"
                testID={testID ? `${testID}-option-${item.id}` : undefined}
                accessibilityLabel={item.secondary ? `${item.label}, ${item.secondary}` : item.label}
                accessibilityState={{ selected }}
                aria-selected={selected}
                onPress={() => choose(item)}
                onHoverIn={() => setHighlight(index)}
                style={(state) => [
                  styles.option,
                  { minHeight: component.target.phone, borderBottomColor: colors.border.subtle },
                  selected ? { backgroundColor: colors.bg.selected } : null,
                  !selected && (highlight === index || (state as PressState).pressed) ? { backgroundColor: colors.bg.surface } : null,
                ]}
              >
                <View style={styles.optionText}>
                  <Txt role="label" numberOfLines={1}>
                    {item.label}
                  </Txt>
                  {item.secondary ? (
                    <Txt role="meta" tone="secondary" numberOfLines={1}>
                      {item.secondary}
                    </Txt>
                  ) : null}
                </View>
                {recent && !query ? (
                  <Txt role="eyebrow" tone="secondary">
                    {t("picker.recent")}
                  </Txt>
                ) : null}
                {selected ? <Icon name="check" size={18} color={colors.accent.default} /> : null}
              </Pressable>
            );
          })}
        </View>
      </Sheet>
    </View>
  );
}

const styles = StyleSheet.create({
  root: { gap: 6, alignSelf: "stretch" },
  trigger: { flexDirection: "row", alignItems: "center", gap: 8, borderWidth: 1, paddingHorizontal: 12, paddingVertical: 4 },
  triggerText: { flex: 1 },
  searchIcon: { marginEnd: 8 },
  list: { marginTop: 4 },
  nothing: { paddingVertical: 16 },
  option: { flexDirection: "row", alignItems: "center", gap: 12, paddingHorizontal: 8, paddingVertical: 6, borderBottomWidth: 1 },
  optionText: { flex: 1 },
});
