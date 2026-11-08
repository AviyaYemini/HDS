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
  Sheet,
  SidePanel,
  Table,
  TextField,
  Txt,
  showToast,
  type Column,
  type SortDirection,
} from "../../design/components";
import { Screen } from "../../design/components/Screen";
import type { IconName } from "../../design/icons/Icon";
import { useTheme } from "../../design/theme/ThemeProvider";
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
  onDelete?: ((row: Row) => Promise<void>) | null;
  emptyTitle: string;
  emptyBody: string;
}

type Saved = { id: string; name: string; query: string };

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
      <ScrollView horizontal role="tablist" accessibilityLabel={t("admin.views")} contentContainerStyle={styles.tabs} style={styles.tabsFrame}>
        {tab(null, t("admin.all"), "")}
        {(views.data ?? []).map((v) => tab(v.id, v.name, v.query))}
      </ScrollView>
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
      ) : (
        <Table
          caption={title}
          columns={columns}
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
          scrollable={!isDesktop}
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
              const key = keyOf(row);
              let undone = false;
              go({ open: null });
              setRemoving((held) => new Set(held).add(key));
              const restore = () => setRemoving((held) => {
                const next = new Set(held);
                next.delete(key);
                return next;
              });
              showToast(t("admin.deleted", { name: nameOf(row) }), {
                duration: UNDO_MS,
                action: {
                  label: t("common.undo"),
                  onPress: () => {
                    undone = true;
                    restore();
                  },
                },
                testID: "toast-deleted",
              });
              setTimeout(() => {
                if (undone) return;
                void onDelete(row)
                  .then(async () => {
                    await rows.reload();
                    restore();
                  })
                  .catch((failure: unknown) => {
                    restore();
                    showToast(errorText(codeOf(failure)), { tone: "warning" });
                  });
              }, UNDO_MS);
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
  tabs: { gap: 4 },
  tab: { minHeight: 44, justifyContent: "center", paddingHorizontal: 12, borderBottomWidth: 2 },
  tools: { flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 8 },
  search: { flexGrow: 1, flexShrink: 1, flexBasis: 220 },
  chip: { flexDirection: "row", alignItems: "center", gap: 4, paddingStart: 8, borderWidth: 1 },
  sheet: { gap: 16 },
  page: { gap: 16 },
  pageHead: { flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 8 },
  pageTitle: { flexGrow: 1, flexShrink: 1, flexBasis: 160 },
});

/** A value shown in a cell, or a dash for none. */
export function orDash(value: string | null | undefined): string {
  return value === null || value === undefined || value === "" ? "—" : value;
}
