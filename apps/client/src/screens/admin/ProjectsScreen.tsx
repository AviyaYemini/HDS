/**
 * Projects: a table with a record panel, and who manages each (specs/007-ux-redesign FR-043 to FR-049).
 */
import { useTranslation } from "react-i18next";

import { call, useErrorText } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { useCapabilities } from "../../capabilities";
import { InlineBanner, SkeletonRows, type Column } from "../../design/components";
import { formatMoney } from "../../locale";
import { useLoad } from "../useLoad";
import { AdminTable, orDash } from "./AdminTable";
import { ManagersEditor } from "./ManagersEditor";
import { RecordForm, moneyText, type FieldSpec } from "./RecordForm";
import { orNull } from "./text";

export function ProjectsScreen() {
  const { t, i18n } = useTranslation();
  const { api } = useSession();
  const errorText = useErrorText();
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
          // Every client and site, by name, however many (group F review).
          { name: "client_id", label: t("entity.client"), kind: "pick" as const, required: true, items: (lists.data?.clients ?? []).map((c) => ({ id: c.id, label: c.name, secondary: c.contact_name ?? undefined })) },
          { name: "site_id", label: t("entity.site"), kind: "pick" as const, required: true, items: (lists.data?.sites ?? []).map((s) => ({ id: s.id, label: s.name, secondary: s.address })) },
        ]),
    { name: "start_date", label: t("entity.start_date"), kind: "date", required: true, fixedOnceSaved: true },
    { name: "end_date", label: t("entity.end_date"), kind: "date", validate: (end, all) => (all.start_date && end < all.start_date ? t("projects.date_range_invalid") : null) },
    { name: "billing_rate", label: t("entity.billing_rate"), kind: "money", required: true, scope: t("scope.billing_rate"), validate: (rate) => (Number(rate) > 0 ? null : t("projects.billing_rate_positive")) },
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
      onDelete={capabilities.has("projects.delete") ? async (p, urgent) => void (await call(api.DELETE("/api/v1/projects/{id}", { params: { path: { id: p.id } }, keepalive: urgent }))) : null}
      panel={({ row, saved }) =>
        lists.error ? (
          <InlineBanner tone="warning" title={errorText(lists.error)} action={{ label: t("common.retry"), onPress: () => void lists.reload() }} />
        ) : lists.data === null ? (
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
