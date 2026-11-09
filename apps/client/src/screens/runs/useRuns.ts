/**
 * The runs page's reads (specs/007-ux-redesign FR-042, FR-081): the runs, newest first, kept up while
 * one on screen is running; and the projects a run may cover.
 */
import { useCallback, useEffect, useState } from "react";

import { call, codeOf } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { useCapabilities } from "../../capabilities";
import { useLoad } from "../useLoad";
import { isActive, type JobSummary } from "./model";
import type { Offered } from "./GenerateForm";

const REFRESH_MS = 2000;

/**
 * The projects a run may cover, for the reader (FR-081): a Manager's own active projects, an
 * Admin's every active one, as the API scopes a run (T108); and every project's name, for the rows.
 */
export function useRunProjects(): { offered: Offered[] | null; names: Map<string, string>; error: string | null; reload: () => Promise<void> } {
  const session = useSession();
  const capabilities = useCapabilities();
  const projects = useLoad(async () => (await call(session.api.GET("/api/v1/projects"))).items, []);
  const all = projects.data ?? [];
  const offered = projects.data
    ? all
        .filter((p) => p.status === "active" && capabilities.hasOnProject("roster.edit:project", p.id))
        .map((p) => ({ id: p.id, name: p.name, type: p.type }))
    : null;
  return { offered, names: new Map(all.map((p) => [p.id, p.name])), error: projects.error, reload: projects.reload };
}

/**
 * The runs, newest first: the first page, and the next on request. While a run on screen is queued or
 * running, the first page is read again in the background, so its status keeps up.
 */
export function useRuns() {
  const session = useSession();
  const [items, setItems] = useState<JobSummary[] | null>(null);
  const [cursor, setCursor] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loadingMore, setLoadingMore] = useState(false);
  // A failed next page keeps the list on screen and says so beside the button (R118).
  const [moreError, setMoreError] = useState<string | null>(null);

  // State is set in the answer's callbacks, never in an effect's own body.
  const first = useCallback(
    (background: boolean) =>
      call((background ? session.quiet : session.api).GET("/api/v1/schedule-jobs", { params: { query: { limit: 50 } } })).then(
        (page) => {
          setItems((held) => {
            const fresh = new Set(page.items.map((job) => job.id));
            return [...page.items, ...(held ?? []).filter((job) => !fresh.has(job.id))];
          });
          // Only a first read sets where the next page starts; a refresh keeps the list's end.
          if (!background) setCursor(page.next_cursor);
          setError(null);
        },
        (failure: unknown) => {
          if (!background) setError(codeOf(failure));
        },
      ),
    [session],
  );

  useEffect(() => {
    void first(false);
  }, [first]);

  const running = (items ?? []).some(isActive);
  useEffect(() => {
    if (!running) return undefined;
    const timer = setInterval(() => void first(true), REFRESH_MS);
    return () => clearInterval(timer);
  }, [running, first]);

  const more = async () => {
    if (cursor === null) return;
    setLoadingMore(true);
    setMoreError(null);
    try {
      const page = await call(session.api.GET("/api/v1/schedule-jobs", { params: { query: { limit: 50, cursor } } }));
      setItems((held) => {
        const ids = new Set((held ?? []).map((job) => job.id));
        return [...(held ?? []), ...page.items.filter((job) => !ids.has(job.id))];
      });
      setCursor(page.next_cursor);
    } catch (failure) {
      setMoreError(codeOf(failure));
    } finally {
      setLoadingMore(false);
    }
  };

  return { data: items, error, reload: () => first(false), more: cursor === null ? null : more, loadingMore, moreError };
}
