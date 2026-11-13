/**
 * The table pattern (specs/007-ux-redesign FR-043, FR-047, FR-063; docs/plans/ux-working-plan.md
 * section 3): uppercase 12 px headers over a strong rule, rows separated by subtle rules, a
 * readable first column, sortable headers with their sort announced, density from the theme
 * (compact on desktop, comfortable on phones) with targets never under 24 px, skeleton rows on the
 * first load and an empty state when there is nothing.
 *
 * Wide tables scroll sideways; `frozenFirstColumn` keeps the first column in view by drawing a
 * visual copy over the leading edge. Assistive technology reads the one real table underneath.
 */
import { useState, type ReactNode } from "react";
import { Pressable, ScrollView, StyleSheet, View, type StyleProp, type ViewStyle } from "react-native";

import { useKeyboardMode } from "../focus";
import { Icon } from "../icons/Icon";
import { useTheme } from "../theme/ThemeProvider";
import { component } from "../theme/theme.generated";
import { webProps } from "../web";
import { focusRing, type PressState } from "./Button";
import { SkeletonRows } from "./Skeleton";
import { Txt } from "./Text";

/** How a column sorts. */
export type SortDirection = "asc" | "desc";

export interface Column<Row> {
  key: string;
  title: string;
  /** Fixed width in px; columns without one share the remaining space. */
  width?: number;
  /** Numbers align to the end and use tabular figures. */
  numeric?: boolean;
  sortable?: boolean;
  /** The cell; the row's value under `key` by default. */
  render?: (row: Row) => ReactNode;
  /** The cell as plain text, where `render` draws more than text (a phone's list line). */
  text?: (row: Row) => string;
}

export interface TableProps<Row> {
  columns: readonly Column<Row>[];
  rows: readonly Row[];
  keyOf: (row: Row) => string;
  /** Names the table for assistive technology. */
  caption: string;
  sort?: { key: string; direction: SortDirection } | null;
  onSort?: (key: string, direction: SortDirection) => void;
  onRowPress?: (row: Row) => void;
  selectedKey?: string | null;
  /** First load: skeleton rows, never "No records" (FR-047). */
  loading?: boolean;
  /** Shown when there are no rows and nothing is loading. */
  empty?: ReactNode;
  /** Let the table scroll sideways; its columns then need widths. */
  scrollable?: boolean;
  frozenFirstColumn?: boolean;
  testID?: string;
  style?: StyleProp<ViewStyle>;
}

const HIDDEN = { "aria-hidden": "true" } as const;

function ariaSort(active: SortDirection | null): string {
  return active === "asc" ? "ascending" : active === "desc" ? "descending" : "none";
}

function cellValue<Row>(row: Row, column: Column<Row>): ReactNode {
  if (column.render) return column.render(row);
  const value = (row as Record<string, unknown>)[column.key];
  return value === null || value === undefined ? "" : String(value);
}

/** A table. */
export function Table<Row>({
  columns,
  rows,
  keyOf,
  caption,
  sort = null,
  onSort,
  onRowPress,
  selectedKey = null,
  loading = false,
  empty,
  scrollable = false,
  frozenFirstColumn = false,
  testID = "data-table",
  style,
}: TableProps<Row>) {
  const { colors, spacing, density, isDesktop } = useTheme();
  const keyboard = useKeyboardMode();
  const [hovered, setHovered] = useState<string | null>(null);
  // The header's drawn height: a title may wrap to two lines, and the frozen copy must match it.
  const [headHeight, setHeadHeight] = useState<number | null>(null);
  const rowHeight = Math.max(spacing.rowHeight, component.target.dense);
  const first = columns[0];
  const frozen = scrollable && frozenFirstColumn && first?.width !== undefined;

  const widthOf = (column: Column<Row>): ViewStyle =>
    column.width !== undefined ? { width: column.width, flexGrow: 0, flexShrink: 0 } : { flex: 1, minWidth: 0 };

  const header = (column: Column<Row>, index: number, plain = false, interactive = true) => {
    const active = sort?.key === column.key ? sort.direction : null;
    const next: SortDirection = active === "asc" ? "desc" : "asc";
    const sortable = interactive && column.sortable && onSort;
    const sortProps = webProps({ "aria-sort": sortable ? ariaSort(active) : undefined });
    const label = (
      // Up to two lines, never cut to "…" (the owner, 2026-11-13); the full title on hover as well.
      <Txt role="eyebrow" tone={active ? "primary" : "secondary"} numberOfLines={2} style={[styles.headLabel, column.numeric ? styles.end : null]} {...webProps({ title: column.title })}>
        {column.title}
      </Txt>
    );
    return (
      <View
        key={column.key}
        role={plain ? undefined : "columnheader"}
        testID={sortable ? `sort-${column.key}` : undefined}
        {...sortProps}
        style={[styles.cell, widthOf(column), index === 0 ? styles.firstCell : null]}
      >
        {sortable ? (
          <Pressable
            role="button"
            accessibilityLabel={column.title}
            accessibilityState={{ selected: active !== null }}
            onPress={() => onSort(column.key, next)}
            style={(state) => [
              styles.sortButton,
              isDesktop
                ? { minHeight: component.target.dense }
                : { minHeight: component.target.phone, minWidth: component.target.phone },
              column.numeric ? styles.rowEnd : null,
              keyboard && (state as PressState).focused ? focusRing(colors.focus.ring) : null,
            ]}
          >
            {label}
            <Icon
              name={active === "asc" ? "sort-asc" : active === "desc" ? "sort-desc" : "sort"}
              size={14}
              color={active ? colors.text.primary : colors.text.secondary}
            />
          </Pressable>
        ) : (
          label
        )}
      </View>
    );
  };

  const cell = (row: Row, column: Column<Row>, index: number) => {
    const value = cellValue(row, column);
    return (
      <View key={column.key} role="cell" style={[styles.cell, widthOf(column), index === 0 ? styles.firstCell : null]}>
        {typeof value === "string" ? (
          <Txt
            role={column.numeric ? "numeric" : index === 0 ? "label" : "body"}
            numeric={column.numeric}
            numberOfLines={1}
            style={[column.numeric ? styles.end : null, density === "compact" ? styles.compactText : null]}
          >
            {value}
          </Txt>
        ) : (
          value
        )}
      </View>
    );
  };

  const line = (row: Row, only?: readonly Column<Row>[], interactive = true) => {
    const key = keyOf(row);
    const selected = key === selectedKey;
    const pressable = interactive && !!onRowPress;
    const content = (state?: PressState) => (
      <View
        style={[
          styles.row,
          { minHeight: rowHeight, borderBottomColor: colors.border.subtle },
          selected ? { backgroundColor: colors.bg.selected } : null,
          !selected && (hovered === key || state?.pressed) ? { backgroundColor: colors.bg.surface } : null,
          keyboard && state?.focused ? focusRing(colors.focus.ring) : null,
        ]}
      >
        {(only ?? columns).map((column, index) => cell(row, column, only ? 0 : index))}
      </View>
    );
    if (!pressable) {
      return (
        <View
          key={key}
          role={interactive ? "row" : undefined}
          testID={interactive ? "table-row" : undefined}
          {...webProps(interactive ? { dataSet: { id: key } } : {})}
          aria-selected={interactive && selected ? true : undefined}
        >
          {content()}
        </View>
      );
    }
    return (
      <Pressable
        key={key}
        role="row"
        testID="table-row"
        {...webProps({ dataSet: { id: key } })}
        aria-selected={selected}
        accessibilityState={{ selected }}
        onPress={() => onRowPress(row)}
        onHoverIn={() => setHovered(key)}
        onHoverOut={() => setHovered((current) => (current === key ? null : current))}
      >
        {(state) => content(state as PressState)}
      </Pressable>
    );
  };

  const body = loading ? (
    <SkeletonRows rows={6} rowHeight={rowHeight} testID={testID ? `${testID}-loading` : undefined} />
  ) : rows.length === 0 ? (
    <View style={styles.empty}>{empty}</View>
  ) : (
    rows.map((row) => line(row))
  );

  const table = (
    <View
      testID={testID}
      role={loading ? undefined : "table"}
      accessibilityLabel={caption}
      style={[styles.table, !scrollable ? styles.fill : null]}
    >
      <View
        role={loading ? undefined : "row"}
        onLayout={frozen ? (e) => setHeadHeight(e.nativeEvent.layout.height) : undefined}
        style={[styles.row, styles.headerRow, { borderBottomColor: colors.border.strong, minHeight: isDesktop ? 36 : 44 }]}
      >
        {columns.map((column, index) => header(column, index, loading))}
      </View>
      {body}
    </View>
  );

  if (!scrollable) return <View style={[styles.fill, style]}>{table}</View>;
  return (
    <View style={[styles.fill, style]}>
      <ScrollView horizontal showsHorizontalScrollIndicator contentContainerStyle={styles.scrollContent}>
        {table}
      </ScrollView>
      {frozen && first ? (
        <View
          pointerEvents="none"
          accessibilityElementsHidden
          importantForAccessibility="no-hide-descendants"
          {...webProps(HIDDEN)}
          style={[styles.frozen, { width: first.width, backgroundColor: colors.bg.canvas, borderEndColor: colors.border.subtle }]}
        >
          <View style={[styles.row, styles.headerRow, { borderBottomColor: colors.border.strong, minHeight: isDesktop ? 36 : 44 }, headHeight !== null ? { height: headHeight } : null]}>
            {header(first, 0, true, false)}
          </View>
          {loading || rows.length === 0 ? null : rows.map((row) => line(row, [first], false))}
        </View>
      ) : null}
    </View>
  );
}

const styles = StyleSheet.create({
  headLabel: { flexShrink: 1 },
  fill: { alignSelf: "stretch" },
  table: { alignSelf: "stretch" },
  scrollContent: { flexGrow: 1 },
  row: { flexDirection: "row", alignItems: "center", borderBottomWidth: 1 },
  headerRow: { borderBottomWidth: 1 },
  cell: { paddingHorizontal: 12, paddingVertical: 6, justifyContent: "center" },
  firstCell: { paddingStart: 4 },
  end: { textAlign: "right" },
  rowEnd: { justifyContent: "flex-end" },
  // The whole header cell sorts, not just its words (a press anywhere on the column's title).
  sortButton: { flexDirection: "row", alignItems: "center", gap: 4, alignSelf: "stretch" },
  compactText: { fontSize: 14, lineHeight: 20 },
  empty: { paddingVertical: 24 },
  frozen: { position: "absolute", top: 0, bottom: 0, start: 0, borderEndWidth: 1 },
});
