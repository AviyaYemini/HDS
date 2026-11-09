/**
 * What the roster week reads (specs/007-ux-redesign FR-025 to FR-041): the project's shift types,
 * every page of the week's shifts, the active people, and a name for anyone on the roster.
 */
import { useCallback, useMemo } from "react";

import { call } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { addDays } from "../../locale";
import { useLoad } from "../useLoad";
import type { BusyOn, Cover, Person, Shift } from "./model";

export function useRosterData(project: string | null, week: string) {
  const session = useSession();
  const types = useLoad(
    async () => (project ? (await call(session.api.GET("/api/v1/projects/{id}/shift-types", { params: { path: { id: project } } }))).items : []),
    [project],
  );
  // Every page of the week's shifts, so a busy week is never cut short.
  const shifts = useLoad(async () => {
    if (!project) return [];
    const all: Shift[] = [];
    let cursor: string | null = null;
    do {
      const query: { from: string; to: string; project_id: string; limit: number; cursor?: string } = { from: week, to: addDays(week, 6), project_id: project, limit: 200 };
      if (cursor) query.cursor = cursor;
      const page: { items: Shift[]; next_cursor?: string | null } = await call(session.api.GET("/api/v1/shifts", { params: { query } }));
      all.push(...page.items);
      cursor = page.next_cursor ?? null;
    } while (cursor);
    return all;
  }, [project, week]);
  const people = useLoad(async () => {
    const found = await call(session.api.GET("/api/v1/employees", { params: { query: { is_active: true } } }));
    return found.items.filter((u) => u.role !== "client").map((u): Person => ({ id: u.id, full_name: u.full_name }));
  }, []);

  // Names from the roster read itself (deactivated people included); the active list fills the rest.
  const names = useMemo(() => {
    const known = new Map((people.data ?? []).map((p) => [p.id, p.full_name]));
    for (const shift of shifts.data ?? []) for (const a of shift.assignments) if (a.full_name) known.set(a.user_id, a.full_name);
    return known;
  }, [people.data, shifts.data]);
  const nameOf = useCallback((id: string | null) => (id ? (names.get(id) ?? "") : ""), [names]);
  return { types, shifts, people, nameOf };
}

/**
 * What the assign sheet shows for ``shift`` (008 FR-034, T042f): its "Find someone" ranking, as a
 * proposal's "who else could cover", and who is already on another shift then, in any project.
 */
export function useAssignInputs(shift: Shift | null): { ranked: Cover | null; busy: BusyOn[] } {
  const session = useSession();
  const ranked = useLoad(
    async () => (shift ? await call(session.api.GET("/api/v1/shifts/{id}/cover", { params: { path: { id: shift.id } } })) : null),
    [shift?.id],
  );
  const busy = useLoad(
    async () => (shift ? (await call(session.api.GET("/api/v1/shifts/{id}/busy", { params: { path: { id: shift.id } } }))).items : []),
    [shift?.id],
  );
  return { ranked: ranked.data, busy: busy.data ?? [] };
}
