/**
 * The dashboards' entrance (the owner, 2026-11-13): numbers count up, bars and candles grow and
 * donuts sweep together, in about half a second, whenever `key` changes (new data). With reduced
 * motion the progress is 1 at once. Only drawing scales with it, never the layout, so nothing jumps.
 */
import { useEffect, useState } from "react";

import { useReducedMotion } from "../../design/motion";
import { easeOut } from "./polish";

export const ENTRANCE_MS = 500;

export function useEntrance(key: string | null): number {
  const reduced = useReducedMotion();
  const animate = !reduced && typeof requestAnimationFrame === "function";
  // The progress of the entrance for one key; a new key starts from 0 until its first frame.
  const [state, setState] = useState<{ key: string | null; progress: number }>({ key: null, progress: 0 });
  useEffect(() => {
    if (key === null || !animate) return undefined;
    let frame = 0;
    const start = Date.now();
    const tick = () => {
      const t = (Date.now() - start) / ENTRANCE_MS;
      setState({ key, progress: easeOut(t) });
      if (t < 1) frame = requestAnimationFrame(tick);
    };
    frame = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(frame);
  }, [key, animate]);
  if (!animate) return 1;
  return state.key === key ? state.progress : 0;
}
