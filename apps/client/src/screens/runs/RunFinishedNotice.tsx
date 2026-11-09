/**
 * The in-app notice when a run someone started finishes (specs/008-auto-scheduler-review FR-029):
 * wherever they are in the app, a toast says so and opens the proposal. No push or e-mail; those
 * come with feature 010. It reads the newest runs every 5 s while one of theirs is queued or
 * running, and every 30 s otherwise, in the background (no loading bar); one of theirs seen running
 * and since pushed off that page is read by itself.
 */
import { usePathname, useRouter } from "expo-router";
import { useEffect, useRef } from "react";
import { useTranslation } from "react-i18next";

import { call } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { showToast } from "../../design/components";

const WHILE_RUNNING_MS = 5_000;
const OTHERWISE_MS = 30_000;

export function RunFinishedNotice() {
  const { t } = useTranslation();
  const session = useSession();
  const router = useRouter();
  const pathname = usePathname();
  const here = useRef(pathname);
  useEffect(() => {
    here.current = pathname;
  }, [pathname]);
  // The person's runs seen queued or running, and every run seen at all, so each end is told once:
  // one seen running that has ended, or one that started and ended between two looks.
  const active = useRef<Set<string> | null>(null);
  const known = useRef<Set<string>>(new Set());

  useEffect(() => {
    let timer: ReturnType<typeof setTimeout> | null = null;
    let live = true;
    const me = session.user?.id;
    const look = async () => {
      let next = OTHERWISE_MS;
      try {
        const page = await call(session.quiet.GET("/api/v1/schedule-jobs", { params: { query: { limit: 20 } } }));
        const mine = page.items.filter((job) => job.requested_by === me);
        // A run of theirs seen running that newer runs pushed off the page is read by itself, so
        // its end is still told.
        const onPage = new Set(page.items.map((job) => job.id));
        const gone = [...(active.current ?? [])].filter((id) => !onPage.has(id));
        for (const id of gone) {
          mine.push(await call(session.quiet.GET("/api/v1/schedule-jobs/{id}", { params: { path: { id } } })));
        }
        const running = new Set(mine.filter((job) => job.status === "queued" || job.status === "running").map((job) => job.id));
        if (active.current !== null) {
          for (const job of mine) {
            const ended = !running.has(job.id);
            const news = active.current.has(job.id) || !known.current.has(job.id);
            if (!ended || !news) continue;
            // Ended since the last look: say so, unless they are on that run's page already.
            if (here.current === `/runs/${job.id}`) continue;
            showToast(t(job.status === "succeeded" ? "runs.notice_ready" : "runs.notice_ended"), {
              tone: job.status === "succeeded" ? "success" : "warning",
              action: { label: t("runs.notice_open"), onPress: () => router.push(`/runs/${job.id}`) },
              testID: "toast-run-finished",
            });
          }
        }
        active.current = running;
        for (const job of mine) known.current.add(job.id);
        if (running.size > 0) next = WHILE_RUNNING_MS;
      } catch {
        // A failed look waits for the next one; the notice is a courtesy, never an error.
      }
      if (live) timer = setTimeout(() => void look(), next);
    };
    void look();
    return () => {
      live = false;
      if (timer) clearTimeout(timer);
    };
  }, [session, router, t]);

  return null;
}
