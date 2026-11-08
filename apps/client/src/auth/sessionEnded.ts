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

/**
 * After the start-up restore: the notice shows only when kept tokens were refused by the server
 * (the session really ended), never when the server was out of reach and the tokens were kept.
 */
export function noteRestore(hadTokens: boolean, restored: boolean, endedUnexpectedly: boolean): void {
  if (restored) clearSessionEnded();
  else if (hadTokens && endedUnexpectedly) markSessionEnded();
}

/** The flag as it stands, for code outside a component (tests, the starter). */
export function isSessionEnded(): boolean {
  return ended;
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
