/**
 * The admin screens on the one table and form pattern (specs/007-ux-redesign FR-043 to FR-049, T083
 * to T085): staff, clients, sites, projects, shift templates and special days as tables with a
 * record panel; staffing needs and pay settings as forms. Deleting shows only for whoever may (an
 * Admin); a Manager creates and edits.
 */
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { call, codeOf, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { useCapabilities } from "../../capabilities";
import { Button, InlineBanner, PageTitle, SkeletonRows, TextField, Txt, showToast, type Column } from "../../design/components";
import { Screen } from "../../design/components/Screen";
import { useTheme } from "../../design/theme/ThemeProvider";
import { formatMoney, formatNumericDate, weekdayNames } from "../../locale";
import { useLoad } from "../useLoad";
import { AdminTable, orDash } from "./AdminTable";
import { RecordForm, moneyText, type FieldSpec, type Values } from "./RecordForm";

/** Empty text as null, for the API's optional fields. */
const orNull = (value: string | undefined): string | null => (value && value.trim() ? value.trim() : null);

// ── Staff ───────────────────────────────────────────────────────────────────

export function StaffScreen() {
  const { t, i18n } = useTranslation();
  const { api } = useSession();
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
    { name: "default_hourly_rate", label: t("entity.default_hourly_rate"), kind: "money" },
    ...(existing ? [] : [{ name: "initial_password", label: t("entity.initial_password"), required: true, helper: t("entity.initial_password_help") }]),
  ];
  const columns: Column<Row>[] = [
    { key: "name", title: t("entity.full_name"), sortable: true, render: (u) => u.full_name },
    { key: "email", title: t("entity.email"), sortable: true, render: (u) => <Txt role="body" dir="ltr" numberOfLines={1}>{u.email}</Txt> },
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
      onDelete={capabilities.has("staff.delete") ? async (u) => void (await call(api.DELETE("/api/v1/employees/{id}", { params: { path: { id: u.id } } }))) : null}
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
            if (row) {
              await call(api.PATCH("/api/v1/employees/{id}", { params: { path: { id: row.id } }, body: { full_name: v.full_name!.trim(), role, phone: orNull(v.phone), default_hourly_rate: orNull(v.default_hourly_rate) } }));
              await saved(row.id);
            } else {
              const made = await call(api.POST("/api/v1/employees", { body: { full_name: v.full_name!.trim(), email: v.email!.trim(), role, phone: orNull(v.phone), default_hourly_rate: orNull(v.default_hourly_rate), initial_password: v.initial_password ?? "" } }));
              await saved(made.id);
            }
          }}
        />
      )}
    />
  );
}

// ── Clients ─────────────────────────────────────────────────────────────────

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
      onDelete={capabilities.has("clients.delete") ? async (c) => void (await call(api.DELETE("/api/v1/clients/{id}", { params: { path: { id: c.id } } }))) : null}
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

// ── Sites ───────────────────────────────────────────────────────────────────

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
    { key: "timezone", title: t("entity.timezone"), sortable: true, render: (s) => <Txt role="body" dir="ltr" numberOfLines={1}>{s.timezone}</Txt> },
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

// ── Projects ────────────────────────────────────────────────────────────────

/**
 * Who manages a project, chosen by name: a search box (`searchable-picker`) whose matches show
 * each person's role and e-mail, in Hebrew or English. Only a Manager or an Admin may manage one, so
 * anyone else is listed with the reason and cannot be chosen.
 */
function ManagersEditor({ projectId }: { projectId: string }) {
  const { t } = useTranslation();
  const { api } = useSession();
  const errorText = useErrorText();
  const { colors } = useTheme();
  const [query, setQuery] = useState("");
  const people = useLoad(async () => (await call(api.GET("/api/v1/employees", { params: { query: { is_active: true } } }))).items, []);
  const managers = useLoad(async () => (await call(api.GET("/api/v1/projects/{id}/managers", { params: { path: { id: projectId } } }))).items, [projectId]);
  const chosen = new Set((managers.data ?? []).map((m) => m.id));
  const matches = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return [];
    return (people.data ?? []).filter((p) => p.role !== "client" && `${p.full_name} ${p.email}`.toLowerCase().includes(q)).slice(0, 8);
  }, [people.data, query]);
  const replace = async (ids: string[]) => {
    try {
      await call(api.PUT("/api/v1/projects/{id}/managers", { params: { path: { id: projectId } }, body: { user_ids: ids } }));
      await managers.reload();
      setQuery("");
    } catch (failure) {
      showToast(errorText(codeOf(failure)), { tone: "warning" });
    }
  };
  return (
    <View style={styles.block}>
      <Txt role="label" heading={3}>
        {t("projects.managers")}
      </Txt>
      {(managers.data ?? []).map((m) => (
        <View key={m.id} style={styles.inline}>
          <Txt role="body" style={styles.grow}>
            {m.full_name}
          </Txt>
          <Button variant="quiet" label={t("common.remove")} onPress={() => void replace([...chosen].filter((id) => id !== m.id))} testID="action-remove-manager" />
        </View>
      ))}
      <TextField label={t("projects.add_manager")} value={query} onChangeText={setQuery} testID="searchable-picker" />
      {matches.map((person) => {
        const can = person.role === "manager" || person.role === "admin";
        return (
          <View key={person.id} testID="picker-option" style={[styles.option, { borderColor: colors.border.subtle }]}>
            <View style={styles.grow}>
              <Txt role="label">{person.full_name}</Txt>
              <Txt role="meta" tone="secondary" testID="picker-option-detail">
                {`${t(`role.${person.role}`)} · ${person.email}`}
              </Txt>
            </View>
            {chosen.has(person.id) ? (
              <Txt role="meta" tone="secondary">
                {t("projects.is_manager")}
              </Txt>
            ) : (
              <Button
                label={t("projects.make_manager")}
                disabled={!can}
                disabledReason={can ? undefined : t("project_managers.not_staff")}
                onPress={() => void replace([...chosen, person.id])}
                testID="action-add-manager"
              />
            )}
          </View>
        );
      })}
    </View>
  );
}

export function ProjectsScreen() {
  const { t, i18n } = useTranslation();
  const { api } = useSession();
  const capabilities = useCapabilities();
  const lists = useLoad(async () => {
    const [clients, sites] = await Promise.all([call(api.GET("/api/v1/clients")), call(api.GET("/api/v1/sites"))]);
    return { clients: clients.items, sites: sites.items };
  }, []);
  type Row = Awaited<ReturnType<typeof loadProjects>>[number];
  async function loadProjects() {
    return (await call(api.GET("/api/v1/projects"))).items;
  }
  const clientName = (id: string) => lists.data?.clients.find((c) => c.id === id)?.name ?? "";
  const siteName = (id: string) => lists.data?.sites.find((s) => s.id === id)?.name ?? "";
  const fields = (existing: boolean): FieldSpec[] => [
    { name: "name", label: t("entity.name"), required: true },
    { name: "type", label: t("entity.type"), kind: "choice", fixedOnceSaved: true, options: (["security", "training"] as const).map((v) => ({ value: v, label: t(`project.type.${v}`) })) },
    ...(existing
      ? [{ name: "status", label: t("entity.status"), kind: "choice" as const, options: (["active", "ended"] as const).map((v) => ({ value: v, label: t(`project.status.${v}`) })) }]
      : [
          {
            name: "client_id",
            label: t("entity.client"),
            kind: "choice" as const,
            required: true,
            options: (lists.data?.clients ?? []).slice(0, 5).map((c) => ({ value: c.id, label: c.name })),
          },
          { name: "site_id", label: t("entity.site"), kind: "choice" as const, required: true, options: (lists.data?.sites ?? []).slice(0, 5).map((s) => ({ value: s.id, label: s.name })) },
        ]),
    { name: "start_date", label: t("entity.start_date"), kind: "date", required: true, fixedOnceSaved: true },
    { name: "end_date", label: t("entity.end_date"), kind: "date", validate: (end, all) => (all.start_date && end < all.start_date ? t("projects.date_range_invalid") : null) },
    { name: "billing_rate", label: t("entity.billing_rate"), kind: "money", required: true, validate: (rate) => (Number(rate) > 0 ? null : t("projects.billing_rate_positive")) },
  ];
  const columns: Column<Row>[] = [
    { key: "name", title: t("entity.name"), sortable: true, render: (p) => p.name },
    { key: "client", title: t("entity.client"), sortable: true, render: (p) => orDash(clientName(p.client_id)) },
    { key: "site", title: t("entity.site"), sortable: true, render: (p) => orDash(siteName(p.site_id)) },
    { key: "type", title: t("entity.type"), sortable: true, render: (p) => t(`project.type.${p.type}`) },
    { key: "status", title: t("entity.status"), sortable: true, render: (p) => t(`project.status.${p.status}`) },
    { key: "rate", title: t("entity.billing_rate"), sortable: true, numeric: true, render: (p) => formatMoney(moneyText(p.billing_rate), i18n.language) },
  ];
  return (
    <AdminTable<Row>
      screen="admin_projects"
      testID="screen-admin-projects"
      title={t("nav.projects")}
      noun={t("entity.noun.project")}
      icon="layers"
      columns={columns}
      load={loadProjects}
      keyOf={(p) => p.id}
      nameOf={(p) => p.name}
      searchText={(p) => `${p.name} ${clientName(p.client_id)} ${siteName(p.site_id)}`}
      sortValue={(p, key) =>
        key === "client" ? clientName(p.client_id) : key === "site" ? siteName(p.site_id) : key === "type" ? p.type : key === "status" ? p.status : key === "rate" ? Number(p.billing_rate) : p.name
      }
      emptyTitle={t("admin.empty.projects_title")}
      emptyBody={t("admin.empty.projects_body")}
      onDelete={capabilities.has("projects.delete") ? async (p) => void (await call(api.DELETE("/api/v1/projects/{id}", { params: { path: { id: p.id } } }))) : null}
      panel={({ row, saved }) =>
        lists.data === null ? (
          <SkeletonRows rows={4} />
        ) : (
          <RecordForm
            key={row?.id ?? "new"}
            existing={row !== null}
            fields={fields(row !== null)}
            initial={{
              name: row?.name ?? "",
              type: row?.type ?? "security",
              status: row?.status ?? "active",
              client_id: row?.client_id ?? lists.data.clients[0]?.id ?? "",
              site_id: row?.site_id ?? lists.data.sites[0]?.id ?? "",
              start_date: row?.start_date ?? "",
              end_date: row?.end_date ?? "",
              billing_rate: row?.billing_rate ? moneyText(row.billing_rate) : "",
            }}
            onSave={async (v) => {
              if (row) {
                await call(api.PATCH("/api/v1/projects/{id}", { params: { path: { id: row.id } }, body: { name: v.name!.trim(), status: v.status as "active" | "ended", end_date: orNull(v.end_date), billing_rate: v.billing_rate! } }));
                await saved(row.id);
              } else {
                const made = await call(
                  api.POST("/api/v1/projects", {
                    body: { name: v.name!.trim(), type: v.type as "security" | "training", client_id: v.client_id!, site_id: v.site_id!, start_date: v.start_date!, end_date: orNull(v.end_date), billing_rate: v.billing_rate! },
                  }),
                );
                await saved(made.id);
              }
            }}
          >
            {row ? <ManagersEditor projectId={row.id} /> : null}
          </RecordForm>
        )
      }
    />
  );
}

// ── Shift templates ─────────────────────────────────────────────────────────

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
    { key: "starts", title: t("entity.starts"), sortable: true, render: (s) => <Txt role="body" dir="ltr">{s.starts_at_local.slice(0, 5)}</Txt> },
    { key: "ends", title: t("entity.ends"), sortable: true, render: (s) => <Txt role="body" dir="ltr">{`${s.ends_at_local.slice(0, 5)}${s.crosses_midnight ? " +1" : ""}`}</Txt> },
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

// ── Special days ────────────────────────────────────────────────────────────

export function SpecialDaysScreen() {
  const { t } = useTranslation();
  const { api } = useSession();
  type Row = Awaited<ReturnType<typeof loadDays>>[number];
  async function loadDays() {
    return (await call(api.GET("/api/v1/special-days"))).items;
  }
  const fields: FieldSpec[] = [
    { name: "name", label: t("entity.name"), required: true },
    { name: "day", label: t("entity.day"), kind: "date", required: true },
    { name: "rate_pct", label: t("entity.rate_pct"), kind: "percent", helper: t("entity.rate_pct_help") },
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
      onDelete={async (d) => void (await call(api.DELETE("/api/v1/special-days/{id}", { params: { path: { id: d.id } } })))}
      panel={({ row, saved }) => (
        <RecordForm
          key={row?.id ?? "new"}
          existing={row !== null}
          fields={fields}
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

// ── Pay settings ────────────────────────────────────────────────────────────

export function PaySettingsScreen() {
  const { t } = useTranslation();
  const { api } = useSession();
  const errorText = useErrorText();
  const capabilities = useCapabilities();
  const settings = useLoad(async () => call(api.GET("/api/v1/organisation/pay-settings")), []);
  const canEdit = capabilities.has("pay_settings.edit");
  const fields: FieldSpec[] = [
    { name: "day_normal_minutes", label: t("pay_settings.day_normal_minutes"), kind: "minutes", required: true, helper: t("pay_settings.minutes_help") },
    { name: "night_normal_minutes", label: t("pay_settings.night_normal_minutes"), kind: "minutes", required: true, helper: t("pay_settings.minutes_help") },
    { name: "rest_pct", label: t("pay_settings.rest_pct"), kind: "percent", required: true },
  ];
  return (
    <Screen testID="screen-admin-pay-settings">
      <PageTitle eyebrow={[t("app.name"), t("nav.group.admin")]} title={t("nav.pay_settings")} />
      {settings.error ? (
        <InlineBanner tone="warning" title={errorText(settings.error)} action={{ label: t("common.retry"), onPress: () => void settings.reload() }} />
      ) : settings.data === null ? (
        <SkeletonRows rows={3} />
      ) : !canEdit ? (
        <View style={styles.block}>
          {fields.map((f) => (
            <Txt key={f.name} role="body">{`${f.label}: ${String((settings.data as unknown as Values)[f.name] ?? "")}`}</Txt>
          ))}
        </View>
      ) : (
        <RecordForm
          existing
          fields={fields}
          initial={{
            day_normal_minutes: String(settings.data.day_normal_minutes),
            night_normal_minutes: String(settings.data.night_normal_minutes),
            rest_pct: settings.data.rest_pct,
          }}
          onSave={async (v) => {
            await call(api.PATCH("/api/v1/organisation/pay-settings", { body: { day_normal_minutes: Number(v.day_normal_minutes), night_normal_minutes: Number(v.night_normal_minutes), rest_pct: v.rest_pct! } }));
            await settings.reload();
            showToast(t("admin.saved"));
          }}
        />
      )}
    </Screen>
  );
}

// ── Staffing needs ──────────────────────────────────────────────────────────

const WEEKDAYS = [0, 1, 2, 3, 4, 5, 6] as const;

/** One project's needs: people per shift template and weekday, saved together (004 FR-001). */
function NeedsForm({ projectId, saved }: { projectId: string; saved: (id: string) => Promise<void> }) {
  const { t, i18n } = useTranslation();
  const days = weekdayNames(i18n.language, "long");
  const { api } = useSession();
  const loaded = useLoad(async () => {
    const [types, needs] = await Promise.all([
      call(api.GET("/api/v1/projects/{id}/shift-types", { params: { path: { id: projectId } } })),
      call(api.GET("/api/v1/projects/{id}/requirements", { params: { path: { id: projectId } } })),
    ]);
    return { types: types.items, needs: needs.items };
  }, [projectId]);
  if (loaded.data === null) return <SkeletonRows rows={3} />;
  const { types, needs } = loaded.data;
  if (types.length === 0) return <Txt role="body" tone="secondary">{t("needs.no_types")}</Txt>;
  const fields: FieldSpec[] = types.flatMap((type) =>
    WEEKDAYS.map((day) => ({
      name: `${type.id}|${day}`,
      label: `${type.name} · ${days[day]}`,
      validate: (value: string) => (/^\d{1,3}$/.test(value) ? null : t("form.count_invalid")),
    })),
  );
  const initial: Values = Object.fromEntries(fields.map((f) => [f.name, "0"]));
  for (const need of needs) initial[`${need.shift_type_id}|${need.weekday}`] = String(need.headcount);
  return (
    <RecordForm
      existing
      fields={fields}
      initial={initial}
      onSave={async (v) => {
        const items = Object.entries(v)
          .map(([key, value]) => {
            const [shift_type_id, weekday] = key.split("|");
            return { shift_type_id: shift_type_id!, weekday: Number(weekday), headcount: Number(value) };
          })
          .filter((item) => item.headcount > 0);
        await call(api.PUT("/api/v1/projects/{id}/requirements", { params: { path: { id: projectId } }, body: { items } }));
        await saved(projectId);
      }}
    />
  );
}

export function StaffingNeedsScreen() {
  const { t } = useTranslation();
  const { api } = useSession();
  type Row = Awaited<ReturnType<typeof loadProjects>>[number];
  async function loadProjects() {
    return (await call(api.GET("/api/v1/projects"))).items.filter((p) => p.status === "active");
  }
  const columns: Column<Row>[] = [
    { key: "name", title: t("entity.name"), sortable: true, render: (p) => p.name },
    { key: "type", title: t("entity.type"), sortable: true, render: (p) => t(`project.type.${p.type}`) },
  ];
  return (
    <AdminTable<Row>
      screen="admin_staffing_needs"
      testID="screen-admin-staffing-needs"
      title={t("nav.staffing_needs")}
      noun={t("entity.noun.project")}
      icon="target"
      canCreate={false}
      columns={columns}
      load={loadProjects}
      keyOf={(p) => p.id}
      nameOf={(p) => p.name}
      searchText={(p) => p.name}
      sortValue={(p, key) => (key === "type" ? p.type : p.name)}
      emptyTitle={t("admin.empty.projects_title")}
      emptyBody={t("admin.empty.projects_body")}
      panel={({ row, saved }) => (row ? <NeedsForm key={row.id} projectId={row.id} saved={saved} /> : null)}
    />
  );
}

const styles = StyleSheet.create({
  block: { gap: 8 },
  inline: { flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 8 },
  grow: { flexGrow: 1, flexShrink: 1, flexBasis: 160 },
  option: { flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 8, padding: 8, borderWidth: 1 },
});
