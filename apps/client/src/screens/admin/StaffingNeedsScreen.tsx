/**
 * Staffing needs: the active projects as a table, each opening its needs form (specs/007-ux-redesign FR-043 to FR-049).
 */
import { useTranslation } from "react-i18next";

import { call } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import type { Column } from "../../design/components";
import { AdminTable } from "./AdminTable";
import { NeedsForm } from "./NeedsForm";

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
