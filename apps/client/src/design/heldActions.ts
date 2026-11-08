/**
 * Actions held for an undo (specs/007-ux-redesign FR-046; reviewer's R163). A removal leaves the
 * list at once and is sent after its undo time, so a held action leaves this module only by Undo
 * or by being sent, never by being forgotten:
 *
 * - its timer sends it when the undo time is up (in-app navigation keeps the timer);
 * - it is sent at once when the page hides (web `pagehide`, with fetch keepalive), when the app goes
 *   to the background, before signing out and before the language restart reloads;
 * - it is never sent after the session has ended: then it is dropped (the server keeps the record).
 */
import { AppState, Platform } from "react-native";

/** Send the action; `urgent` when the page or app is going away (the request must outlive it). */
export type Send = (urgent: boolean) => Promise<void>;

interface Held {
  send: Send;
  timer: ReturnType<typeof setTimeout>;
  onError?: (failure: unknown) => void;
}

const held = new Map<number, Held>();
let next = 1;

function run(id: number, urgent: boolean): Promise<void> {
  const action = held.get(id);
  if (!action) return Promise.resolve();
  held.delete(id);
  clearTimeout(action.timer);
  return action.send(urgent).catch((failure: unknown) => action.onError?.(failure));
}

/**
 * Hold `send` for `ms`, then send it; answers the undo, which is true when it caught the action in
 * time (nothing is sent then).
 */
export function hold(send: Send, ms: number, onError?: (failure: unknown) => void): () => boolean {
  const id = next++;
  held.set(id, { send, onError, timer: setTimeout(() => void run(id, false), ms) });
  return () => {
    const action = held.get(id);
    if (!action) return false;
    clearTimeout(action.timer);
    held.delete(id);
    return true;
  };
}

/** Send every held action now (before signing out, before a restart, as the page hides). */
export function flushHeld(urgent = false): Promise<void> {
  return Promise.all([...held.keys()].map((id) => run(id, urgent))).then(() => undefined);
}

/** Forget every held action unsent: the session has ended, so nothing may go out in its name. */
export function dropHeld(): void {
  for (const action of held.values()) clearTimeout(action.timer);
  held.clear();
}

/** How many actions are held (for tests). */
export function heldCount(): number {
  return held.size;
}

/**
 * Send held actions when the page hides or the app leaves the foreground; call once at the root.
 * Returns the uninstall.
 */
export function installHeldFlush(): () => void {
  const cleanups: (() => void)[] = [];
  if (Platform.OS === "web" && typeof window !== "undefined") {
    const onHide = () => void flushHeld(true);
    window.addEventListener("pagehide", onHide);
    cleanups.push(() => window.removeEventListener("pagehide", onHide));
  }
  const subscription = AppState.addEventListener("change", (state) => {
    // Background only: iOS reports "inactive" for a pulled-down control centre too.
    if (state === "background") void flushHeld(true);
  });
  // Some environments (the test one) answer no subscription.
  cleanups.push(() => subscription?.remove?.());
  return () => {
    for (const cleanup of cleanups) cleanup();
  };
}
