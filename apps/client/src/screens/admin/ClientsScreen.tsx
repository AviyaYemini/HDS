/**
 * Clients: a table with a record panel; deleting only for whoever may (specs/007-ux-redesign FR-043 to FR-049).
 */
import { useTranslation } from "react-i18next";

import { call } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { useCapabilities } from "../../capabilities";
import type { Column } from "../../design/components";
import { formatNumericDate } from "../../locale";
import { AdminTable, orDash } from "./AdminTable";
import { RecordForm, type FieldSpec } from "./RecordForm";
import { orNull } from "./text";

export function ClientsScreen() {
  const { t } = useTranslation();
  const { api } = useSession();
  const capabilities = useCapabilities();
  type Row = Awaited<ReturnType<typeof loadClients>>[number];
  async function loadClients() {
    return (await call(api.GET("/api/v1/clients"))).items;
  }
  const fields: FieldSpec[] = [
    { name: "name", label: t("entity.name"), required: true },
    { name: "contract_start", label: t("entity.contract_start"), kind: "date", required: true },
    {
      name: "contract_end",
      label: t("entity.contract_end"),
      kind: "date",
      validate: (end, all) => (all.contract_start && end < all.contract_start ? t("clients.date_range_invalid") : null),
    },
    { name: "contact_name", label: t("entity.contact_name") },
    { name: "contact_email", label: t("entity.contact_email"), kind: "email" },
    { name: "contact_phone", label: t("entity.contact_phone") },
  ];
  const columns: Column<Row>[] = [
    { key: "name", title: t("entity.name"), sortable: true, render: (c) => c.name },
    { key: "contact", title: t("entity.contact_name"), sortable: true, render: (c) => orDash(c.contact_name) },
    { key: "start", title: t("entity.contract_start"), sortable: true, render: (c) => formatNumericDate(c.contract_start) },
    { key: "end", title: t("entity.contract_end"), sortable: true, render: (c) => (c.contract_end ? formatNumericDate(c.contract_end) : "—") },
  ];
  return (
    <AdminTable<Row>
      screen="admin_clients"
      testID="screen-admin-clients"
      title={t("nav.clients")}
      noun={t("entity.noun.client")}
      icon="briefcase"
      columns={columns}
      load={loadClients}
      keyOf={(c) => c.id}
      nameOf={(c) => c.name}
      searchText={(c) => `${c.name} ${c.contact_name ?? ""} ${c.contact_email ?? ""}`}
      sortValue={(c, key) => (key === "contact" ? (c.contact_name ?? null) : key === "start" ? c.contract_start : key === "end" ? (c.contract_end ?? null) : c.name)}
      emptyTitle={t("admin.empty.clients_title")}
      emptyBody={t("admin.empty.clients_body")}
      active={{
        of: (c) => c.is_active,
        set: async (c, to) => void (await call(api.PATCH("/api/v1/clients/{id}", { params: { path: { id: c.id } }, body: { is_active: to } }))),
      }}
      onDelete={capabilities.has("clients.delete") ? async (c, urgent) => void (await call(api.DELETE("/api/v1/clients/{id}", { params: { path: { id: c.id } }, keepalive: urgent }))) : null}
      panel={({ row, saved }) => (
        <RecordForm
          key={row?.id ?? "new"}
          existing={row !== null}
          fields={fields}
          initial={{
            name: row?.name ?? "",
            contract_start: row?.contract_start ?? "",
            contract_end: row?.contract_end ?? "",
            contact_name: row?.contact_name ?? "",
            contact_email: row?.contact_email ?? "",
            contact_phone: row?.contact_phone ?? "",
          }}
          onSave={async (v) => {
            const body = {
              name: v.name!.trim(),
              contract_start: v.contract_start!,
              contract_end: orNull(v.contract_end),
              contact_name: orNull(v.contact_name),
              contact_email: orNull(v.contact_email),
              contact_phone: orNull(v.contact_phone),
            };
            if (row) {
              await call(api.PATCH("/api/v1/clients/{id}", { params: { path: { id: row.id } }, body }));
              await saved(row.id);
            } else {
              const made = await call(api.POST("/api/v1/clients", { body }));
              await saved(made.id);
            }
          }}
        />
      )}
    />
  );
}
