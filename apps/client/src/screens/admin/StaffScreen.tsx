/**
 * Staff: the people of the organisation as a table with a record panel (specs/007-ux-redesign FR-043 to FR-049).
 */
import { useTranslation } from "react-i18next";

import { call } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { useCapabilities } from "../../capabilities";
import type { Column } from "../../design/components";
import { formatMoney, isolateLtr } from "../../locale";
import { AdminTable } from "./AdminTable";
import { RecordForm, moneyText, type FieldSpec } from "./RecordForm";
import { orNull } from "./text";

export function StaffScreen() {
  const { t, i18n } = useTranslation();
  const session = useSession();
  const { api } = session;
  const capabilities = useCapabilities();
  type Row = Awaited<ReturnType<typeof loadStaff>>[number];
  async function loadStaff() {
    return (await call(api.GET("/api/v1/employees"))).items.filter((u) => u.role !== "client");
  }
  const roles = ["employee", "manager", "admin"] as const;
  const fields = (existing: boolean): FieldSpec[] => [
    { name: "full_name", label: t("entity.full_name"), required: true },
    { name: "email", label: t("entity.email"), kind: "email", required: true, fixedOnceSaved: true },
    { name: "role", label: t("entity.role"), kind: "choice", options: roles.map((r) => ({ value: r, label: t(`role.${r}`) })) },
    { name: "phone", label: t("entity.phone") },
    { name: "default_hourly_rate", label: t("entity.default_hourly_rate"), kind: "money", scope: t("scope.hourly_rate") },
    ...(existing ? [] : [{ name: "initial_password", label: t("entity.initial_password"), required: true, helper: t("entity.initial_password_help") }]),
  ];
  const columns: Column<Row>[] = [
    { key: "name", title: t("entity.full_name"), sortable: true, render: (u) => u.full_name },
    { key: "email", title: t("entity.email"), sortable: true, render: (u) => isolateLtr(u.email) },
    { key: "role", title: t("entity.role"), sortable: true, render: (u) => t(`role.${u.role}`) },
    { key: "rate", title: t("entity.default_hourly_rate"), sortable: true, numeric: true, render: (u) => (u.default_hourly_rate ? formatMoney(moneyText(u.default_hourly_rate), i18n.language) : "—") },
    { key: "active", title: t("entity.status"), sortable: true, render: (u) => (u.is_active ? t("entity.active") : t("entity.inactive")) },
  ];
  return (
    <AdminTable<Row>
      screen="admin_staff"
      testID="screen-admin-staff"
      title={t("nav.staff")}
      noun={t("entity.noun.staff")}
      icon="people"
      columns={columns}
      load={loadStaff}
      keyOf={(u) => u.id}
      nameOf={(u) => u.full_name}
      searchText={(u) => `${u.full_name} ${u.email} ${u.phone ?? ""}`}
      sortValue={(u, key) => (key === "rate" ? (u.default_hourly_rate ? Number(u.default_hourly_rate) : null) : key === "email" ? u.email : key === "role" ? u.role : key === "active" ? (u.is_active ? 1 : 0) : u.full_name)}
      emptyTitle={t("admin.empty.staff_title")}
      emptyBody={t("admin.empty.staff_body")}
      active={{
        of: (u) => u.is_active,
        // Never your own account: a bulk deactivation would sign you out partway (code review, PR 5).
        keep: (u) => (u.id === session.user?.id ? t("admin.not_yourself") : null),
        set: async (u, to) => void (await call(to ? api.POST("/api/v1/employees/{id}/reactivate", { params: { path: { id: u.id } } }) : api.POST("/api/v1/employees/{id}/deactivate", { params: { path: { id: u.id } } }))),
      }}
      onDelete={capabilities.has("staff.delete") ? async (u, urgent) => void (await call(api.DELETE("/api/v1/employees/{id}", { params: { path: { id: u.id } }, keepalive: urgent }))) : null}
      panel={({ row, saved }) => (
        <RecordForm
          key={row?.id ?? "new"}
          existing={row !== null}
          fields={fields(row !== null)}
          initial={{
            full_name: row?.full_name ?? "",
            email: row?.email ?? "",
            role: row?.role ?? "employee",
            phone: row?.phone ?? "",
            default_hourly_rate: row?.default_hourly_rate ? moneyText(row.default_hourly_rate) : "",
          }}
          onSave={async (v) => {
            const role = v.role as (typeof roles)[number];
            // The rate only when it changed: a Manager is not shown another person's rate, and sending
            // its empty field would refuse the whole edit (users.rate_admin_only; CodeRabbit, PR 20).
            const before = row?.default_hourly_rate ? moneyText(row.default_hourly_rate) : "";
            const rate = (v.default_hourly_rate ?? "") !== before ? { default_hourly_rate: orNull(v.default_hourly_rate) } : {};
            if (row) {
              await call(api.PATCH("/api/v1/employees/{id}", { params: { path: { id: row.id } }, body: { full_name: v.full_name!.trim(), role, phone: orNull(v.phone), ...rate } }));
              await saved(row.id);
            } else {
              const made = await call(api.POST("/api/v1/employees", { body: { full_name: v.full_name!.trim(), email: v.email!.trim(), role, phone: orNull(v.phone), ...rate, initial_password: v.initial_password ?? "" } }));
              await saved(made.id);
            }
          }}
        />
      )}
    />
  );
}
