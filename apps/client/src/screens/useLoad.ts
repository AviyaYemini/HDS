/** Loading a screen's data: the data, the code of a failure, and a way to load again. */
import { useCallback, useEffect, useRef, useState } from "react";

import { codeOf } from "../api/errors";

/** The state of a load. */
export interface Loaded<T> {
  data: T | null;
  error: string | null;
  reload: () => Promise<void>;
}

/** Run `load` now and on `reload`; a failure keeps its translation key. */
export function useLoad<T>(load: () => Promise<T>): Loaded<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const latest = useRef(load);
  const mounted = useRef(true);

  useEffect(() => {
    latest.current = load;
  });

  const reload = useCallback(async () => {
    try {
      const loaded = await latest.current();
      if (!mounted.current) return;
      setData(loaded);
      setError(null);
    } catch (failure) {
      if (mounted.current) setError(codeOf(failure));
    }
  }, []);

  useEffect(() => {
    mounted.current = true;
    void reload();
    return () => {
      mounted.current = false;
    };
  }, [reload]);

  return { data, error, reload };
}
