/** Loading a screen's data: the data, the code of a failure, and a way to load again. */
import { useCallback, useEffect, useRef, useState } from "react";

import { codeOf } from "../api/errors";

/** The state of a load. */
export interface Loaded<T> {
  data: T | null;
  error: string | null;
  reload: () => Promise<void>;
}

/**
 * Run `load` now, on `reload`, and again whenever a value in `deps` changes; a failure keeps its
 * translation key. When `deps` change the old data and error are cleared, so the screen shows it is
 * loading, and only the answer to the latest request is kept: an earlier one that arrives late is
 * dropped (005 review).
 */
export function useLoad<T>(load: () => Promise<T>, deps: readonly unknown[] = []): Loaded<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const latest = useRef(load);
  const mounted = useRef(true);
  const request = useRef(0);

  useEffect(() => {
    latest.current = load;
  });

  const reload = useCallback(async () => {
    const mine = ++request.current;
    try {
      const loaded = await latest.current();
      if (!mounted.current || mine !== request.current) return;
      setData(loaded);
      setError(null);
    } catch (failure) {
      if (mounted.current && mine === request.current) setError(codeOf(failure));
    }
  }, []);

  const key = JSON.stringify(deps);
  const first = useRef(true);
  useEffect(() => {
    mounted.current = true;
    if (!first.current) {
      setData(null);
      setError(null);
    }
    first.current = false;
    void reload();
    return () => {
      mounted.current = false;
    };
  }, [reload, key]);

  return { data, error, reload };
}
