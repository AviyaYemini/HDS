/**
 * Whether the app can reach the network (specs/007-ux-redesign FR-077, the offline banner).
 *
 * On the web the browser says so (`navigator.onLine` and its events). On phones no extra package
 * is pulled in: the API client reports every request that never reached the server, and every
 * answer that did, so the banner follows what the app actually experiences. A small external store
 * for `useSyncExternalStore`.
 */
import { useSyncExternalStore } from "react";
import { Platform } from "react-native";

let online = true;
let started = false;
const listeners = new Set<() => void>();

function set(value: boolean): void {
  if (value === online) return;
  online = value;
  for (const listener of listeners) listener();
}

function start(): void {
  if (started) return;
  started = true;
  if (Platform.OS !== "web" || typeof window === "undefined") return;
  if (typeof navigator !== "undefined" && typeof navigator.onLine === "boolean") online = navigator.onLine;
  window.addEventListener("online", () => set(true));
  window.addEventListener("offline", () => set(false));
}

/** Whether the network is reachable right now. */
export function isOnline(): boolean {
  start();
  return online;
}

/** Listen for the state changing; returns the unsubscribe. */
export function subscribeOnline(listener: () => void): () => void {
  start();
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

/** The connectivity, re-rendering on change. */
export function useOnline(): boolean {
  return useSyncExternalStore(subscribeOnline, isOnline, () => true);
}

/** A request never reached the server (the API client reports it). */
export function reportUnreachable(): void {
  start();
  set(false);
}

/** A request was answered by the server. */
export function reportReachable(): void {
  start();
  // The browser's own events own the web's state; a successful request only confirms them.
  if (Platform.OS === "web" && typeof navigator !== "undefined" && navigator.onLine === false) return;
  set(true);
}

/** For tests. */
export function setOnlineForTests(value: boolean): void {
  started = true;
  set(value);
}
