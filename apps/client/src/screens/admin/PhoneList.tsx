/**
 * The admin table on a phone (specs/007-ux-redesign FR-043 to FR-049): one card per record, its first
 * column as the title and the others in a line, with the sort as a picker and a checkbox for bulk
 * actions; and a column's value as plain text.
 */
import { useTranslation } from "react-i18next";
import { Pressable, StyleSheet, View } from "react-native";

import { Button, SearchablePicker, SkeletonRows, Txt, type Column, type SortDirection } from "../../design/components";
import { Icon } from "../../design/icons/Icon";
import { useTheme } from "../../design/theme/ThemeProvider";
import { webProps } from "../../design/web";
import { type TableState } from "./tableState";

/** A column's value as text (every admin column renders text, so a phone can show it in a line). */
export function cellText<Row>(column: Column<Row>, row: Row): string {
  const value = column.render ? column.render(row) : (row as Record<string, unknown>)[column.key];
  return typeof value === "string" || typeof value === "number" ? String(value) : "";
}

/**
 * Below the phone breakpoint an admin list is a list (R166, the reviewer's ruling): each row's first
 * column is its title and the others a second line that wraps; sorting is a "Sort by" control above
 * it. Nothing runs off the side or is cut.
 */
export function PhoneList<Row>({
  columns,
  rows,
  loading,
  keyOf,
  sort,
  onSort,
  onOpen,
  emptyText,
  selecting = false,
  selected,
  onToggle,
}: {
  columns: readonly Column<Row>[];
  rows: readonly Row[];
  loading: boolean;
  keyOf: (row: Row) => string;
  sort: TableState["sort"];
  onSort: (key: string, direction: SortDirection) => void;
  onOpen: (row: Row) => void;
  emptyText: string;
  /** In Select mode a tap selects a row instead of opening it (the bulk-action bar). */
  selecting?: boolean;
  selected?: ReadonlySet<string>;
  onToggle?: (row: Row) => void;
}) {
  const { t } = useTranslation();
  const { colors } = useTheme();
  const sortable = columns.filter((c) => c.sortable);
  const [first, ...rest] = columns;
  const direction: SortDirection = sort?.direction ?? "asc";
  return (
    <View style={styles.phone}>
      {sortable.length > 0 ? (
        <View style={styles.sortRow}>
          <SearchablePicker
            label={t("admin.sort_by")}
            items={sortable.map((c) => ({ id: c.key, label: c.title }))}
            value={sort?.key ?? null}
            onChange={(key) => onSort(key, direction)}
            placeholder={t("admin.sort_none")}
            testID="sort-by"
            style={styles.sortPick}
          />
          <Button
            variant="secondary"
            icon={direction === "asc" ? "sort-asc" : "sort-desc"}
            label={direction === "asc" ? t("admin.sort_asc") : t("admin.sort_desc")}
            disabled={!sort}
            onPress={() => sort && onSort(sort.key, direction === "asc" ? "desc" : "asc")}
            testID="sort-direction"
          />
        </View>
      ) : null}
      {loading ? (
        <SkeletonRows rows={6} />
      ) : rows.length === 0 ? (
        <Txt role="body" tone="secondary">
          {emptyText}
        </Txt>
      ) : (
        <View role="list" style={styles.list}>
          {rows.map((row) => (
            <Pressable
              key={keyOf(row)}
              role={selecting ? "checkbox" : "listitem"}
              testID="table-row"
              {...webProps({ dataSet: { id: keyOf(row) } })}
              {...(selecting ? { "aria-checked": selected?.has(keyOf(row)) ?? false, accessibilityState: { checked: selected?.has(keyOf(row)) ?? false } } : {})}
              onPress={() => (selecting ? onToggle?.(row) : onOpen(row))}
              style={({ pressed }) => [
                styles.item,
                { borderBottomColor: colors.border.subtle },
                pressed || (selecting && selected?.has(keyOf(row))) ? { backgroundColor: colors.bg.selected } : null,
              ]}
            >
              <View style={styles.itemHead}>
                {selecting ? (
                  <View style={[styles.checkbox, { borderColor: colors.border.strong }, selected?.has(keyOf(row)) ? { backgroundColor: colors.accent.default } : null]}>
                    {selected?.has(keyOf(row)) ? <Icon name="check" size={14} color={colors.bg.canvas} /> : null}
                  </View>
                ) : null}
                <Txt role="label" style={styles.itemTitle}>
                  {first ? cellText(first, row) : ""}
                </Txt>
              </View>
              <Txt role="meta" tone="secondary">
                {rest.map((c) => cellText(c, row)).filter((v) => v && v !== "—").join(" · ")}
              </Txt>
            </Pressable>
          ))}
        </View>
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  list: { gap: 0 },
  phone: { gap: 12 },
  sortPick: { flexGrow: 1, flexShrink: 1, flexBasis: 180 },
  sortRow: { flexDirection: "row", flexWrap: "wrap", alignItems: "flex-end", gap: 8 },
  item: { gap: 2, paddingVertical: 10, minHeight: 48, borderBottomWidth: 1 },
  itemHead: { flexDirection: "row", alignItems: "center", gap: 8 },
  itemTitle: { flexGrow: 1, flexShrink: 1 },
  checkbox: { width: 22, height: 22, borderWidth: 2, alignItems: "center", justifyContent: "center" },
});
