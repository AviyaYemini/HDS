/**
 * This month's snapshot for the admin tables (the owner, 2026-11-13): each project's hours, billing
 * and terms from GET /reports/overview, so clients and projects can show what they bring in. An Admin
 * sees every project; a Manager their own. A failed read leaves the columns empty, never the table.
 */
import { call } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { DEFAULT_TIME_ZONE, todayIn } from "../../locale";
import { periodRange, type OverviewProject } from "../overview/model";
import { useLoad } from "../useLoad";

export function useMonthOverview(): Map<string, OverviewProject> | null {
  return useMonthOverviewWithReload().byId;
}

/** The same, with a way to read it again after a change it shows (a project's managers). */
export function useMonthOverviewWithReload(): { byId: Map<string, OverviewProject> | null; reload: () => Promise<void> } {
  const { api } = useSession();
  const month = periodRange("month", todayIn(DEFAULT_TIME_ZONE));
  const overview = useLoad(async () => (await call(api.GET("/api/v1/reports/overview", { params: { query: month } }))).projects, [month.from]);
  const byId = overview.error ? new Map<string, OverviewProject>() : overview.data ? new Map(overview.data.map((p) => [p.id, p])) : null;
  return { byId, reload: overview.reload };
}
