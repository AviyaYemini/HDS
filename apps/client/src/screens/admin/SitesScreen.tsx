/**
 * Sites: a table with a record panel (specs/007-ux-redesign FR-043 to FR-049).
 */
import { useTranslation } from "react-i18next";

import { call } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import type { Column } from "../../design/components";
import { isolateLtr } from "../../locale";
import { AdminTable } from "./AdminTable";
import { RecordForm, type FieldSpec } from "./RecordForm";

export function SitesScreen() {
  const { t } = useTranslation();
  const { api } = useSession();
  type Row = Awaited<ReturnType<typeof loadSites>>[number];
  async function loadSites() {
    return (await call(api.GET("/api/v1/sites"))).items;
  }
  const fields: FieldSpec[] = [
    { name: "name", label: t("entity.name"), required: true },
    { name: "address", label: t("entity.address"), required: true },
    { name: "timezone", label: t("entity.timezone"), required: true, helper: t("entity.timezone_help") },
  ];
  const columns: Column<Row>[] = [
    { key: "name", title: t("entity.name"), sortable: true, render: (s) => s.name },
    { key: "address", title: t("entity.address"), sortable: true, render: (s) => s.address },
    { key: "timezone", title: t("entity.timezone"), sortable: true, render: (s) => isolateLtr(s.timezone) },
  ];
  return (
    <AdminTable<Row>
      screen="admin_sites"
      testID="screen-admin-sites"
      title={t("nav.sites")}
      noun={t("entity.noun.site")}
      icon="building"
      columns={columns}
      load={loadSites}
      keyOf={(s) => s.id}
      nameOf={(s) => s.name}
      searchText={(s) => `${s.name} ${s.address} ${s.timezone}`}
      sortValue={(s, key) => (key === "address" ? s.address : key === "timezone" ? s.timezone : s.name)}
      emptyTitle={t("admin.empty.sites_title")}
      emptyBody={t("admin.empty.sites_body")}
      panel={({ row, saved }) => (
        <RecordForm
          key={row?.id ?? "new"}
          existing={row !== null}
          fields={fields}
          initial={{ name: row?.name ?? "", address: row?.address ?? "", timezone: row?.timezone ?? "Asia/Jerusalem" }}
          onSave={async (v) => {
            const body = { name: v.name!.trim(), address: v.address!.trim(), timezone: v.timezone!.trim() };
            if (row) {
              await call(api.PATCH("/api/v1/sites/{id}", { params: { path: { id: row.id } }, body }));
              await saved(row.id);
            } else {
              const made = await call(api.POST("/api/v1/sites", { body }));
              await saved(made.id);
            }
          }}
        />
      )}
    />
  );
}
