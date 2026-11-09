/**
 * What My availability reads (specs/007-ux-redesign FR-013 to FR-020): the windows and active shift
 * types, the window shown (the one asked for, or the current one), the person's entries over its
 * weeks, and their submission for it.
 */
import { useMemo } from "react";

import { call } from "../../../api/errors";
import { useSession } from "../../../api/SessionContext";
import { addDays, endOfWeek, startOfWeek } from "../../../locale";
import { useLoad } from "../../useLoad";
import { pickWindow } from "./model";

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
  const entries = useLoad(
    async () => (range ? (await call(session.api.GET("/api/v1/employees/{id}/constraints", { params: { path: { id: userId }, query: range } }))).items : []),
    [range?.from, range?.to, userId],
  );
  const submission = useLoad(
    async () => (window ? call(session.api.GET("/me/availability-submissions", { params: { query: { window_id: window.id } } })) : null),
    [window?.id],
  );
  return { setup, window, shiftTypes, range, entries, submission };
}
