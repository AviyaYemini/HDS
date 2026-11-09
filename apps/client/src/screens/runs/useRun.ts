/**
 * A run page's reads (specs/007-ux-redesign FR-042, FR-074; 008 FR-027): the job, polled in the
 * background while it runs; the determinate loading bar it drives; and the names its proposal shows.
 */
import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";

import { call, codeOf } from "../../api/errors";
import { useSession } from "../../api/SessionContext";
import { endProgress, setProgress, startLoading } from "../../design/loading/loadingStore";
import { useLoad } from "../useLoad";
import { elapsedNow, isActive, runProgress, type Job, type Names } from "./model";


const POLL_MS = 1000;
const RETRY_MS = 3000;
const TICK_MS = 250;

/** A job and when it was read, by the device's clock. */
export interface Reading {
  job: Job;
  readAt: number;
}

/** Hold the bar while the run goes and feed it elapsed (by the server's clock) over the limit. */
export function useRunBar(reading: Reading | null): void {
  const { t } = useTranslation();
  const latest = useRef(reading);
  useEffect(() => {
    latest.current = reading;
  });
  const job = reading?.job ?? null;
  const active = job !== null && isActive(job);
  const solver = job?.solver ?? null;
  const limit = job?.time_limit_seconds ?? 0;
  const text = t("runs.up_to", { count: limit });
  useEffect(() => {
    if (!active || solver === null) return undefined;
    const done = startLoading();
    let floor = 0;
    const tick = () => {
      const now = latest.current;
      const elapsed = now === null ? null : elapsedNow(now.job, now.readAt, Date.now());
      const value = runProgress({ status: "running", solver, time_limit_seconds: limit }, elapsed, floor);
      if (value !== null) floor = value;
      setProgress(value, value === null ? null : text);
    };
    tick();
    const timer = setInterval(tick, TICK_MS);
    return () => {
      clearInterval(timer);
      // Ended (or left): the bar completes at once and goes, and nothing after inherits it.
      endProgress();
      done();
    };
  }, [active, solver, limit, text]);
}

/**
 * Read the run, and keep reading it in the background while it runs. A failed background read keeps
 * the last answer on screen, says so, and tries again; only a failed first read replaces the page.
 */
export function useJob(id: string | undefined) {
  const session = useSession();
  const [reading, setReading] = useState<Reading | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [pollError, setPollError] = useState<string | null>(null);
  const [attempt, setAttempt] = useState(0);
  useEffect(() => {
    if (!id) return undefined;
    let stopped = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    let seen = false;
    const read = async (background: boolean) => {
      try {
        const client = background ? session.quiet : session.api;
        const next = await call(client.GET("/api/v1/schedule-jobs/{id}", { params: { path: { id } } }));
        if (stopped) return;
        seen = true;
        setReading({ job: next, readAt: Date.now() });
        setError(null);
        setPollError(null);
        if (isActive(next)) timer = setTimeout(() => void read(true), POLL_MS);
      } catch (failure) {
        if (stopped) return;
        if (!seen) {
          setError(codeOf(failure));
          return;
        }
        setPollError(codeOf(failure));
        timer = setTimeout(() => void read(true), RETRY_MS);
      }
    };
    void read(false);
    return () => {
      stopped = true;
      if (timer) clearTimeout(timer);
    };
  }, [id, session, attempt]);
  return {
    reading,
    error,
    pollError,
    setJob: (job: Job) => setReading({ job, readAt: Date.now() }),
    reload: () => setAttempt((n) => n + 1),
  };
}

/**
 * Project, shift type and people's names, to read the proposal by (FR-080). Everyone, deactivated
 * people included, so an older run still names who it proposed.
 */
export function useNames(): Names & { staff: { id: string; full_name: string }[] } {
  const session = useSession();
  const loaded = useLoad(async () => {
    const [projects, types, people] = await Promise.all([
      call(session.api.GET("/api/v1/projects")),
      call(session.api.GET("/api/v1/shift-types")),
      call(session.api.GET("/api/v1/employees")),
    ]);
    return {
      projects: new Map(projects.items.map((p) => [p.id, p.name])),
      types: new Map(types.items.map((type) => [type.id, type.name])),
      people: new Map(people.items.map((person) => [person.id, person.full_name])),
      // Who a proposal may place: active staff, never a client account (008 FR-008).
      staff: people.items.filter((person) => person.role !== "client" && person.is_active).map((person) => ({ id: person.id, full_name: person.full_name })),
    };
  });
  return loaded.data ?? { projects: new Map(), types: new Map(), people: new Map(), staff: [] };
}

