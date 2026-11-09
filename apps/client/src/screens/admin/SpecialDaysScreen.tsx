/**
 * Special days: a table with a record panel (specs/007-ux-redesign FR-043 to FR-049).
 */
import { useTranslation } from "react-i18next";

import { call } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import type { Column } from "../../design/components";
import { formatNumericDate } from "../../locale";
import { AdminTable } from "./AdminTable";
import { RecordForm, type FieldSpec } from "./RecordForm";
import { orNull } from "./text";

export function SpecialDaysScreen() {
  const { t } = useTranslation();
  const { api } = useSession();
  type Row = Awaited<ReturnType<typeof loadDays>>[number];
  async function loadDays() {
    return (await call(api.GET("/api/v1/special-days"))).items;
  }
  // Once saved, a day keeps a rate: clearing it would not change it, so it is required then.
  const fields = (existing: boolean): FieldSpec[] => [
    { name: "name", label: t("entity.name"), required: true },
    { name: "day", label: t("entity.day"), kind: "date", required: true },
    { name: "rate_pct", label: t("entity.rate_pct"), kind: "percent", required: existing, helper: t("entity.rate_pct_help"), scope: t("scope.special_day") },
  ];
  const columns: Column<Row>[] = [
    { key: "day", title: t("entity.day"), sortable: true, render: (d) => formatNumericDate(d.day) },
    { key: "name", title: t("entity.name"), sortable: true, render: (d) => d.name },
    { key: "rate", title: t("entity.rate_pct"), sortable: true, numeric: true, render: (d) => `${d.rate_pct}%` },
  ];
  return (
    <AdminTable<Row>
      screen="admin_special_days"
      testID="screen-admin-special-days"
      title={t("nav.special_days")}
      noun={t("entity.noun.special_day")}
      icon="star"
      columns={columns}
      load={loadDays}
      keyOf={(d) => d.id}
      nameOf={(d) => d.name}
      searchText={(d) => `${d.name} ${d.day}`}
      sortValue={(d, key) => (key === "name" ? d.name : key === "rate" ? Number(d.rate_pct) : d.day)}
      emptyTitle={t("admin.empty.special_days_title")}
      emptyBody={t("admin.empty.special_days_body")}
      onDelete={async (d, urgent) => void (await call(api.DELETE("/api/v1/special-days/{id}", { params: { path: { id: d.id } }, keepalive: urgent })))}
      panel={({ row, saved }) => (
        <RecordForm
          key={row?.id ?? "new"}
          existing={row !== null}
          fields={fields(row !== null)}
          initial={{ name: row?.name ?? "", day: row?.day ?? "", rate_pct: row?.rate_pct ?? "" }}
          onSave={async (v) => {
            if (row) {
              await call(api.PATCH("/api/v1/special-days/{id}", { params: { path: { id: row.id } }, body: { name: v.name!.trim(), day: v.day!, rate_pct: orNull(v.rate_pct) ?? undefined } }));
              await saved(row.id);
            } else {
              const made = await call(api.POST("/api/v1/special-days", { body: { name: v.name!.trim(), day: v.day!, rate_pct: orNull(v.rate_pct) } }));
              await saved(made.id);
            }
          }}
        />
      )}
    />
  );
}
