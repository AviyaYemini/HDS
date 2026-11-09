/**
 * Shift templates: a table with a record panel (specs/007-ux-redesign FR-043 to FR-049).
 */
import { useTranslation } from "react-i18next";

import { call } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import type { Column } from "../../design/components";
import { isolateLtr } from "../../locale";
import { AdminTable } from "./AdminTable";
import { RecordForm, type FieldSpec } from "./RecordForm";

export function ShiftTemplatesScreen() {
  const { t } = useTranslation();
  const { api } = useSession();
  type Row = Awaited<ReturnType<typeof loadTypes>>[number];
  async function loadTypes() {
    return (await call(api.GET("/api/v1/shift-types"))).items;
  }
  const fields: FieldSpec[] = [
    { name: "name", label: t("entity.name"), required: true },
    { name: "starts_at_local", label: t("entity.starts"), kind: "time", required: true, helper: t("entity.time_help") },
    {
      name: "ends_at_local",
      label: t("entity.ends"),
      kind: "time",
      required: true,
      validate: (end, all) => (end === all.starts_at_local ? t("shift_types.times_equal") : null),
    },
  ];
  const columns: Column<Row>[] = [
    { key: "name", title: t("entity.name"), sortable: true, render: (s) => s.name },
    { key: "starts", title: t("entity.starts"), sortable: true, render: (s) => isolateLtr(s.starts_at_local.slice(0, 5)) },
    { key: "ends", title: t("entity.ends"), sortable: true, render: (s) => isolateLtr(`${s.ends_at_local.slice(0, 5)}${s.crosses_midnight ? " +1" : ""}`) },
    { key: "active", title: t("entity.status"), sortable: true, render: (s) => (s.is_active ? t("entity.active") : t("entity.inactive")) },
  ];
  return (
    <AdminTable<Row>
      screen="admin_shift_templates"
      testID="screen-admin-shift-templates"
      title={t("nav.shift_templates")}
      noun={t("entity.noun.shift_template")}
      icon="clock"
      columns={columns}
      load={loadTypes}
      keyOf={(s) => s.id}
      nameOf={(s) => s.name}
      searchText={(s) => s.name}
      sortValue={(s, key) => (key === "starts" ? s.starts_at_local : key === "ends" ? s.ends_at_local : key === "active" ? (s.is_active ? 1 : 0) : s.name)}
      active={{
        of: (s) => s.is_active,
        set: async (s, to) => void (await call(api.PATCH("/api/v1/shift-types/{id}", { params: { path: { id: s.id } }, body: { is_active: to } }))),
      }}
      emptyTitle={t("admin.empty.shift_templates_title")}
      emptyBody={t("admin.empty.shift_templates_body")}
      panel={({ row, saved }) => (
        <RecordForm
          key={row?.id ?? "new"}
          existing={row !== null}
          fields={fields}
          initial={{ name: row?.name ?? "", starts_at_local: row?.starts_at_local.slice(0, 5) ?? "", ends_at_local: row?.ends_at_local.slice(0, 5) ?? "" }}
          onSave={async (v) => {
            const body = { name: v.name!.trim(), starts_at_local: v.starts_at_local!, ends_at_local: v.ends_at_local! };
            if (row) {
              await call(api.PATCH("/api/v1/shift-types/{id}", { params: { path: { id: row.id } }, body }));
              await saved(row.id);
            } else {
              const made = await call(api.POST("/api/v1/shift-types", { body }));
              await saved(made.id);
            }
          }}
        />
      )}
    />
  );
}
