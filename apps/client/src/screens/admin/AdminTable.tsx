/**
 * One table pattern for every admin list (specs/007-ux-redesign FR-043, FR-044, FR-047 to FR-049,
 * T083 to T085). Sortable headers, a search, saved views as tabs, and the open record in a side
 * panel, all kept in the web address so a reload or a shared link shows the same; skeleton rows on
 * the first load (never "No records" while loading), and an empty state that names one next action.
 */
import { router, useLocalSearchParams } from "expo-router";
import { useEffect, useRef, useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Pressable, ScrollView, StyleSheet, View } from "react-native";

import { call, codeOf, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import {
  Button,
  EmptyState,
  InlineBanner,
  PageTitle,
  SearchablePicker,
  Sheet,
  SidePanel,
  SkeletonRows,
  Table,
  TextField,
  Txt,
  showToast,
  type Column,
  type SortDirection,
} from "../../design/components";
import { Screen } from "../../design/components/Screen";
import { Icon, type IconName } from "../../design/icons/Icon";
import { useTheme } from "../../design/theme/ThemeProvider";
import { webProps } from "../../design/web";
import { hold } from "../../design/heldActions";
import { mayLeaveFor } from "../../design/unsaved";
import { useLoad } from "../useLoad";
import { fromViewQuery, readState, searched, sorted, toParams, viewQuery, type TableState } from "./tableState";

export interface PanelArgs<Row> {
  /** The open record, or null for a new one. */
  row: Row | null;
  /** Close the panel. */
  close: () => void;
  /** After a save: reload the list, and open the saved record. */
  saved: (id: string) => Promise<void>;
}

export interface AdminTableProps<Row> {
  /** The saved views' screen key (`admin_staff`). */
  screen: string;
  testID: string;
  title: string;
  /** What one record is called, for the create button and the panel ("Client"). */
  noun: string;
  icon: IconName;
  columns: readonly Column<Row>[];
  load: () => Promise<Row[]>;
  keyOf: (row: Row) => string;
  nameOf: (row: Row) => string;
  searchText: (row: Row) => string;
  sortValue: (row: Row, key: string) => string | number | null;
  panel: (args: PanelArgs<Row>) => ReactNode;
  canCreate?: boolean;
  /** Shown only to whoever may delete (an Admin); the record is removed after a confirmation. */
  /** `urgent` when the page or app is going away: the request must outlive it (keepalive). */
  onDelete?: ((row: Row, urgent: boolean) => Promise<void>) | null;
  /**
   * A record's active state, where it has one: offered as Activate and Deactivate on several.
   * `keep` names a row a bulk change leaves alone and why (your own account), or null.
   */
  active?: { of: (row: Row) => boolean; set: (row: Row, active: boolean) => Promise<void>; keep?: (row: Row) => string | null } | null;
  emptyTitle: string;
  emptyBody: string;
}

type Saved = { id: string; name: string; query: string };

/** A column's value as text (every admin column renders text, so a phone can show it in a line). */
function cellText<Row>(column: Column<Row>, row: Row): string {
  const value = column.render ? column.render(row) : (row as Record<string, unknown>)[column.key];
  return typeof value === "string" || typeof value === "number" ? String(value) : "";
}

/**
 * Below the phone breakpoint an admin list is a list (R166, the reviewer's ruling): each row's first
 * column is its title and the others a second line that wraps; sorting is a "Sort by" control above
 * it. Nothing runs off the side or is cut.
 */
function PhoneList<Row>({
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

/** The pause after typing before the search reaches the address. */
const SEARCH_PAUSE_MS = 250;
/** How long a removal can be undone, the toast's time with an action. */
const UNDO_MS = 6000;
/** The longest view query the API keeps (`saved_views.query`). */
const VIEW_QUERY_MAX = 2000;

export function AdminTable<Row>({
  screen,
  testID,
  title,
  noun,
  icon,
  columns,
  load,
  keyOf,
  nameOf,
  searchText,
  sortValue,
  panel,
  canCreate = true,
  onDelete = null,
  active = null,
  emptyTitle,
  emptyBody,
}: AdminTableProps<Row>) {
  const { t, i18n } = useTranslation();
  const session = useSession();
  const errorText = useErrorText();
  const { colors, isDesktop } = useTheme();
  const params = useLocalSearchParams<Record<string, string>>();
  const sortable = columns.filter((c) => c.sortable).map((c) => c.key);
  const state = readState(params, sortable);
  const go = (next: Partial<TableState>) => router.setParams(toParams({ ...state, ...next }) as never);

  const rows = useLoad(load, []);
  const views = useLoad(async () => (await call(session.api.GET("/me/saved-views", { params: { query: { screen } } }))).items as Saved[], [screen]);

  // Typed here and written to the address after a pause, so fast typing never fights the address
  // (group F review); an address that changes from elsewhere (a view tab) wins.
  const [typed, setTyped] = useState(state.q);
  const lastWritten = useRef(state.q);
  useEffect(() => {
    if (state.q !== lastWritten.current) {
      lastWritten.current = state.q;
      setTyped(state.q);
    }
  }, [state.q]);
  useEffect(() => {
    if (typed === lastWritten.current) return undefined;
    const handle = setTimeout(() => {
      lastWritten.current = typed;
      go({ q: typed, view: null });
    }, SEARCH_PAUSE_MS);
    return () => clearTimeout(handle);
  });
  // Records being removed: hidden at once, deleted when the undo toast's time is up (FR-046).
  const [removing, setRemoving] = useState<ReadonlySet<string>>(new Set());
  // The bulk-action bar (FR-043; the owner, PR 5): chosen rows, and a phone's Select mode.
  const [chosen, setChosen] = useState<ReadonlySet<string>>(new Set());
  const [selecting, setSelecting] = useState(false);
  const [confirmingBulk, setConfirmingBulk] = useState(false);
  const bulkable = onDelete !== null || active !== null;

  const [naming, setNaming] = useState(false);
  const [viewName, setViewName] = useState("");
  const [deleting, setDeleting] = useState<Row | null>(null);

  const listed = (rows.data ?? []).filter((r) => !removing.has(keyOf(r)));
  const shown = sorted(searched(listed, state.q, searchText), state.sort, sortValue, i18n.language);
  const openRow = state.open && state.open !== "new" ? ((rows.data ?? []).find((r) => keyOf(r) === state.open) ?? null) : null;
  const panelOpen = state.open === "new" || openRow !== null;
  // Closing a record with unsaved changes asks first, as leaving the screen does (FR-045).
  const closePanel = () => {
    const close = () => go({ open: null });
    if (mayLeaveFor(close)) close();
  };
  const panelTitle = openRow ? nameOf(openRow) : t("admin.new", { noun });
  const panelBody = panelOpen
    ? panel({
        row: openRow,
        close: closePanel,
        saved: async (id) => {
          await rows.reload();
          showToast(t("admin.saved"));
          go({ open: id });
        },
      })
    : null;
  const visibleChosen = shown.filter((r) => chosen.has(keyOf(r)));
  const toggle = (row: Row) =>
    setChosen((held) => {
      const next = new Set(held);
      if (next.has(keyOf(row))) next.delete(keyOf(row));
      else next.add(keyOf(row));
      return next;
    });
  const clearChosen = () => {
    setChosen(new Set());
    setSelecting(false);
  };
  // What went wrong, said once: "2 of 3 removed; 1 could not be: <reason>".
  const summarise = (verb: "removed" | "changed", total: number, failed: { code: string }[]) => {
    if (failed.length === 0) return;
    showToast(t(`admin.bulk_partial_${verb}`, { done: total - failed.length, total, count: failed.length, reason: errorText(failed[0]!.code) }), { tone: "warning", testID: "toast-bulk-partial" });
  };
  const bulkRemove = () => {
    if (!onDelete) return;
    const picked = [...visibleChosen];
    const keys = picked.map(keyOf);
    clearChosen();
    go({ open: null });
    setRemoving((held) => new Set([...held, ...keys]));
    const restore = (back: string[]) =>
      setRemoving((held) => {
        const next = new Set(held);
        for (const key of back) next.delete(key);
        return next;
      });
    // One held action for all of them, with one undo (heldActions, R163). Every delete is sent at
    // once, so a page closing during an urgent flush still sends them all (code review, PR 5).
    const undo = hold(
      async (urgent) => {
        const settled = await Promise.allSettled(picked.map((row) => onDelete(row, urgent)));
        const failed = settled.flatMap((r) => (r.status === "rejected" ? [{ code: codeOf(r.reason) }] : []));
        summarise("removed", picked.length, failed);
        restore(keys);
        try {
          await rows.reload();
        } catch {
          // The list says so itself on its next read; the summary has gone out.
        }
      },
      UNDO_MS,
      () => restore(keys),
    );
    showToast(t("admin.bulk_removed", { count: picked.length }), {
      duration: UNDO_MS,
      action: { label: t("common.undo"), onPress: () => undo() && restore(keys) },
      testID: "toast-deleted",
    });
  };
  const bulkActive = async (to: boolean) => {
    if (!active) return;
    // Rows a bulk change leaves alone (your own account) are named, never changed.
    const kept = visibleChosen.map((r) => active.keep?.(r) ?? null).filter((reason): reason is string => reason !== null);
    const allowed = visibleChosen.filter((r) => (active.keep?.(r) ?? null) === null);
    const picked = allowed.filter((r) => active.of(r) !== to);
    clearChosen();
    if (kept.length > 0) showToast(kept[0]!, { tone: "info" });
    if (picked.length === 0) {
      showToast(t(to ? "admin.bulk_already_active" : "admin.bulk_already_inactive"));
      return;
    }
    const failed: { code: string }[] = [];
    for (const row of picked) {
      try {
        await active.set(row, to);
      } catch (failure) {
        failed.push({ code: codeOf(failure) });
      }
    }
    if (failed.length === 0) showToast(t(to ? "admin.bulk_activated" : "admin.bulk_deactivated", { count: picked.length }));
    else summarise("changed", picked.length, failed);
    try {
      await rows.reload();
    } catch {
      // The list says so on its next read.
    }
  };
  const bulkBar =
    bulkable && visibleChosen.length > 0 ? (
      <View testID="bulk-bar" role="toolbar" accessibilityLabel={t("admin.bulk_actions")} style={[styles.bulkBar, { borderColor: colors.accent.default, backgroundColor: colors.bg.surface }]}>
        <Txt role="label" testID="bulk-count" style={styles.bulkCount}>
          {t("admin.bulk_selected", { count: visibleChosen.length, title })}
        </Txt>
        {visibleChosen.length < shown.length ? (
          <Button
            variant="quiet"
            label={state.q ? t("admin.select_all_found", { count: shown.length }) : t("admin.select_all", { count: shown.length })}
            onPress={() => setChosen(new Set(shown.map(keyOf)))}
            testID="bulk-select-all"
          />
        ) : null}
        {onDelete ? <Button label={t("admin.bulk_remove")} icon="close" onPress={() => setConfirmingBulk(true)} testID="bulk-remove" /> : null}
        {active ? <Button label={t("admin.bulk_activate")} onPress={() => void bulkActive(true)} testID="bulk-activate" /> : null}
        {active ? <Button label={t("admin.bulk_deactivate")} onPress={() => void bulkActive(false)} testID="bulk-deactivate" /> : null}
        <Button variant="quiet" label={t("admin.bulk_clear")} onPress={clearChosen} testID="bulk-clear" />
      </View>
    ) : null;
  // A desktop selects by a checkbox column ahead of the others, while no record is open (the bar
  // is hidden then, so nothing may be chosen out of sight; code review, PR 5).
  const tableColumns: readonly Column<Row>[] =
    bulkable && isDesktop && !panelOpen
      ? [
          {
            key: "__select",
            title: t("admin.select"),
            width: 56,
            render: (row) => (
              <Pressable
                role="checkbox"
                aria-checked={chosen.has(keyOf(row))}
                accessibilityState={{ checked: chosen.has(keyOf(row)) }}
                accessibilityLabel={t("admin.select_row", { name: nameOf(row) })}
                testID="row-select"
                {...webProps({ dataSet: { id: keyOf(row) } })}
                onPress={() => toggle(row)}
                style={[styles.checkbox, { borderColor: colors.border.strong }, chosen.has(keyOf(row)) ? { backgroundColor: colors.accent.default } : null]}
              >
                {chosen.has(keyOf(row)) ? <Icon name="check" size={14} color={colors.bg.canvas} /> : null}
              </Pressable>
            ),
          },
          ...columns,
        ]
      : columns;
  const deleteButton =
    openRow && onDelete ? <Button label={t("admin.delete", { noun })} icon="close" onPress={() => setDeleting(openRow)} testID="action-delete" /> : null;

  const saveView = async () => {
    if (viewQuery(state).length > VIEW_QUERY_MAX) {
      showToast(t("admin.view_too_long"), { tone: "warning" });
      return;
    }
    try {
      const made = await call(session.api.POST("/me/saved-views", { body: { screen, name: viewName.trim(), query: viewQuery(state) } }));
      await views.reload();
      setNaming(false);
      setViewName("");
      go({ view: made.id });
    } catch (failure) {
      showToast(errorText(codeOf(failure)), { tone: "warning" });
    }
  };

  const tab = (id: string | null, label: string, query: string) => {
    const selected = state.view === id;
    return (
      <Pressable
        key={id ?? "all"}
        role="tab"
        testID={id ? "view-tab" : "view-tab-all"}
        aria-selected={selected}
        accessibilityState={{ selected }}
        onPress={() => go({ ...fromViewQuery(query, sortable), view: id })}
        style={[styles.tab, { borderBottomColor: selected ? colors.accent.default : "transparent" }]}
      >
        <Txt role="label" tone={selected ? "primary" : "secondary"} numberOfLines={1}>
          {label}
        </Txt>
      </Pressable>
    );
  };

  return (
    <Screen testID={testID}>
      <PageTitle eyebrow={[t("app.name"), t("nav.group.admin")]} title={title}>
        {canCreate ? <Button variant="primary" icon="plus" label={t("admin.new", { noun })} onPress={() => go({ open: "new" })} testID="action-new" /> : null}
      </PageTitle>
      {isDesktop ? (
        <ScrollView horizontal role="tablist" accessibilityLabel={t("admin.views")} contentContainerStyle={styles.tabs} style={styles.tabsFrame}>
          {tab(null, t("admin.all"), "")}
          {(views.data ?? []).map((v) => tab(v.id, v.name, v.query))}
        </ScrollView>
      ) : (
        // A phone wraps the views onto more lines rather than scrolling them sideways (R166).
        <View role="tablist" accessibilityLabel={t("admin.views")} style={styles.tabsWrap}>
          {tab(null, t("admin.all"), "")}
          {(views.data ?? []).map((v) => tab(v.id, v.name, v.query))}
        </View>
      )}
      <View style={styles.tools}>
        <TextField
          label={t("admin.search")}
          hideLabel
          placeholder={t("admin.search")}
          value={typed}
          onChangeText={setTyped}
          testID="table-search"
          containerStyle={styles.search}
        />
        {state.q ? (
          <View testID="filters-on" style={[styles.chip, { borderColor: colors.accent.default }]}>
            <Txt role="meta">{t("admin.filtered", { shown: shown.length, all: rows.data?.length ?? 0 })}</Txt>
            <Button variant="quiet" label={t("admin.clear")} onPress={() => go({ q: "", view: null })} testID="action-clear-filters" />
          </View>
        ) : null}
        <Button label={t("admin.save_view")} icon="star" onPress={() => setNaming(true)} testID="action-save-view" />
        {state.view ? (
          <Button
            variant="quiet"
            label={t("admin.remove_view")}
            onPress={() => {
              const id = state.view!;
              void call(session.api.DELETE("/me/saved-views/{id}", { params: { path: { id } } }))
                .then(async () => {
                  go({ view: null });
                  await views.reload();
                })
                .catch((failure: unknown) => showToast(errorText(codeOf(failure)), { tone: "warning" }));
            }}
            testID="action-remove-view"
          />
        ) : null}
      </View>
      {bulkable && !isDesktop && !panelOpen ? (
        <Button
          variant="quiet"
          label={selecting ? t("admin.select_done") : t("admin.select_mode")}
          onPress={() => (selecting ? clearChosen() : setSelecting(true))}
          testID="action-select-mode"
        />
      ) : null}
      {panelOpen ? null : bulkBar}
      {!isDesktop && panelOpen ? (
        <View testID="side-panel" style={styles.page}>
          <View style={styles.pageHead}>
            <Button variant="quiet" icon="arrow-back" label={t("common.back")} onPress={closePanel} testID="action-close-panel" />
            <Txt role="section" heading={2} style={styles.pageTitle}>
              {panelTitle}
            </Txt>
          </View>
          {panelBody}
          {deleteButton}
        </View>
      ) : rows.error ? (
        <InlineBanner tone="warning" title={errorText(rows.error)} action={{ label: t("common.retry"), onPress: () => void rows.reload() }} />
      ) : rows.data !== null && rows.data.length === 0 ? (
        <EmptyState
          icon={icon}
          title={emptyTitle}
          body={emptyBody}
          action={canCreate ? { label: t("admin.new", { noun }), onPress: () => go({ open: "new" }), variant: "primary", testID: "action-new-empty" } : undefined}
        />
      ) : !isDesktop ? (
        <PhoneList
          columns={columns}
          rows={shown}
          loading={rows.data === null}
          keyOf={keyOf}
          sort={state.sort}
          onSort={(key, direction) => go({ sort: { key, direction }, view: null })}
          onOpen={(row) => {
            const openIt = () => go({ open: keyOf(row) });
            if (mayLeaveFor(openIt)) openIt();
          }}
          emptyText={t("admin.no_match")}
          selecting={selecting}
          selected={chosen}
          onToggle={toggle}
        />
      ) : (
        <Table
          caption={title}
          columns={tableColumns}
          rows={shown}
          keyOf={keyOf}
          loading={rows.data === null}
          sort={state.sort}
          onSort={(key: string, direction: SortDirection) => go({ sort: { key, direction }, view: null })}
          onRowPress={(row) => {
            // Another record while this one has unsaved edits: ask first (group F review).
            const openIt = () => go({ open: keyOf(row) });
            if (mayLeaveFor(openIt)) openIt();
          }}
          selectedKey={openRow ? keyOf(openRow) : null}
          empty={<Txt role="body" tone="secondary">{t("admin.no_match")}</Txt>}
        />
      )}
      {isDesktop ? (
        <SidePanel open={panelOpen} onClose={closePanel} title={panelTitle} testID="side-panel" footer={deleteButton ?? undefined}>
          {panelBody}
        </SidePanel>
      ) : null}
      <Sheet open={naming} onClose={() => setNaming(false)} title={t("admin.save_view")} testID="save-view">
        <View style={styles.sheet}>
          <TextField label={t("admin.view_name")} value={viewName} onChangeText={setViewName} testID="save-view-name" />
          <Button variant="primary" label={t("common.save")} onPress={() => void saveView()} disabled={!viewName.trim()} testID="action-confirm-save-view" />
        </View>
      </Sheet>
      <Sheet open={confirmingBulk} onClose={() => setConfirmingBulk(false)} title={t("admin.bulk_remove")} testID="confirm-bulk-delete">
        <View style={styles.sheet}>
          <Txt role="body">{t("admin.bulk_remove_body", { count: visibleChosen.length, title })}</Txt>
          <Button
            variant="primary"
            label={t("admin.bulk_remove_confirm", { count: visibleChosen.length })}
            onPress={() => {
              setConfirmingBulk(false);
              bulkRemove();
            }}
            testID="action-confirm-bulk-delete"
          />
        </View>
      </Sheet>
      <Sheet open={deleting !== null} onClose={() => setDeleting(null)} title={t("admin.delete", { noun })} testID="confirm-delete">
        <View style={styles.sheet}>
          <Txt role="body">{t("admin.delete_body", { name: deleting ? nameOf(deleting) : "" })}</Txt>
          <Button
            variant="primary"
            label={t("admin.delete", { noun })}
            onPress={() => {
              const row = deleting;
              setDeleting(null);
              if (!row || !onDelete) return;
              // Gone from the list at once, deleted when the toast's undo has passed: the API's
              // delete is final, so the undo is the wait before it (FR-046).
              // Held in one place for every screen: sent when the undo time is up, or at once as the
              // page hides, the app leaves the foreground, before sign-out and before a language
              // restart; dropped if the session ends first (R163).
              const key = keyOf(row);
              go({ open: null });
              setRemoving((held) => new Set(held).add(key));
              const restore = () => setRemoving((held) => {
                const next = new Set(held);
                next.delete(key);
                return next;
              });
              const undo = hold(
                async (urgent) => {
                  await onDelete(row, urgent);
                  await rows.reload();
                  restore();
                },
                UNDO_MS,
                (failure) => {
                  restore();
                  showToast(errorText(codeOf(failure)), { tone: "warning" });
                },
              );
              showToast(t("admin.deleted", { name: nameOf(row) }), {
                duration: UNDO_MS,
                action: {
                  label: t("common.undo"),
                  onPress: () => {
                    if (undo()) restore();
                  },
                },
                testID: "toast-deleted",
              });
            }}
            testID="action-confirm-delete"
          />
        </View>
      </Sheet>
    </Screen>
  );
}

const styles = StyleSheet.create({
  tabsFrame: { flexGrow: 0 },
  tabsWrap: { flexDirection: "row", flexWrap: "wrap", gap: 4 },
  phone: { gap: 12 },
  sortRow: { flexDirection: "row", flexWrap: "wrap", alignItems: "flex-end", gap: 8 },
  sortPick: { flexGrow: 1, flexShrink: 1, flexBasis: 180 },
  list: { gap: 0 },
  item: { gap: 2, paddingVertical: 10, minHeight: 48, borderBottomWidth: 1 },
  tabs: { gap: 4 },
  tab: { minHeight: 44, justifyContent: "center", paddingHorizontal: 12, borderBottomWidth: 2 },
  tools: { flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 8 },
  search: { flexGrow: 1, flexShrink: 1, flexBasis: 220 },
  chip: { flexDirection: "row", alignItems: "center", gap: 4, paddingStart: 8, borderWidth: 1 },
  sheet: { gap: 16 },
  bulkBar: { flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 8, padding: 8, borderWidth: 1 },
  bulkCount: { flexGrow: 1, flexShrink: 1, flexBasis: 160 },
  checkbox: { width: 22, height: 22, borderWidth: 2, alignItems: "center", justifyContent: "center" },
  itemHead: { flexDirection: "row", alignItems: "center", gap: 8 },
  itemTitle: { flexGrow: 1, flexShrink: 1 },
  page: { gap: 16 },
  pageHead: { flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 8 },
  pageTitle: { flexGrow: 1, flexShrink: 1, flexBasis: 160 },
});

/** A value shown in a cell, or a dash for none. */
export function orDash(value: string | null | undefined): string {
  return value === null || value === undefined || value === "" ? "—" : value;
}
