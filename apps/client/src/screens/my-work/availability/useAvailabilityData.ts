/**
 * What My availability reads and sends (specs/007-ux-redesign FR-013 to FR-020): the windows and
 * active shift types, the window shown (the one asked for, or the current one), the person's entries
 * over its weeks and their submission for it; and the writes, each sent against the entries read
 * afresh (R49, R50).
 */
import { useCallback, useMemo } from "react";

import { ApiError, call } from "../../../api/errors";
import { useSession } from "../../../api/SessionContext";
import { addDays, endOfWeek, startOfWeek } from "../../../locale";
import { useLoad } from "../../useLoad";
import type { PendingChange } from "./draftQueue";
import { pickWindow, planCellChange, type Entry, type PlannedCall } from "./model";
import { runPlan as runPlanCalls } from "./runPlan";

export function useAvailabilityData(askedWindow: string | undefined, userId: string, today: string) {
  const session = useSession();
  const setup = useLoad(async () => {
    const [windows, types] = await Promise.all([
      call(session.api.GET("/api/v1/availability-windows", { params: { query: { from: addDays(today, -60) } } })),
      call(session.api.GET("/api/v1/shift-types")),
    ]);
    return { windows: windows.items, types: types.items.filter((type) => type.is_active) };
  });
  const window = useMemo(() => (setup.data ? pickWindow(setup.data.windows, askedWindow) : null), [setup.data, askedWindow]);
  const shiftTypes = setup.data?.types ?? [];

  const range = window ? { from: addDays(startOfWeek(window.starts_on), -7), to: endOfWeek(window.ends_on) } : null;
  // The person's entries over the window's weeks: shown in the grid, and read afresh before a write.
  const readEntries = useCallback(
    async (): Promise<Entry[]> =>
      range ? (await call(session.api.GET("/api/v1/employees/{id}/constraints", { params: { path: { id: userId }, query: range } }))).items : [],
    // eslint-disable-next-line react-hooks/exhaustive-deps -- the range's two dates are the dependency
    [range?.from, range?.to, userId, session],
  );
  const entries = useLoad(readEntries, [range?.from, range?.to, userId]);
  const submission = useLoad(
    async () => (window ? call(session.api.GET("/me/availability-submissions", { params: { query: { window_id: window.id } } })) : null),
    [window?.id],
  );

  /** Run a plan's calls in order. */
  const runPlan = useCallback((calls: readonly PlannedCall[]): Promise<void> => runPlanCalls(session.api, userId, calls), [session.api, userId]);

  // A saved change is sent against the server's entries of that day, read afresh, so it lands right
  // however the week changed meanwhile (R49, R50).
  const sendPending = useCallback(
    async (change: PendingChange): Promise<void> => {
      if (!window) return;
      try {
        const fresh = await call(session.api.GET("/api/v1/employees/{id}/constraints", { params: { path: { id: userId }, query: { from: change.key.date, to: change.key.date } } }));
        await runPlan(planCellChange(fresh.items, change.key, change.state, window));
      } catch (failure) {
        if (failure instanceof ApiError && failure.status === 0) throw Object.assign(new Error("unreachable"), { retry: true });
        throw failure;
      }
    },
    [window, session.api, userId, runPlan],
  );

  return { setup, window, shiftTypes, entries, submission, readEntries, runPlan, sendPending };
}
