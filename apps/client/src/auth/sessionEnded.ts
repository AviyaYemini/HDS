/**
 * The visible notice when a session ends on its own (specs/007-ux-redesign FR-010): the sign-in
 * screen says so once, whether the kept tokens no longer worked at start-up or a refresh was
 * refused while the app was open.
 */
import { useSyncExternalStore } from "react";

let ended = false;
const listeners = new Set<() => void>();

function set(value: boolean): void {
  if (value === ended) return;
  ended = value;
  for (const listener of listeners) listener();
}

/** The session ended without the person signing out. */
export function markSessionEnded(): void {
  set(true);
}

/** The person has seen the notice, or signed in again. */
export function clearSessionEnded(): void {
  set(false);
}

/** Whether to show the notice, re-rendering on change. */
export function useSessionEnded(): boolean {
  return useSyncExternalStore(
    (listener) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    () => ended,
    () => false,
  );
}
