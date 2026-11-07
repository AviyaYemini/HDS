/**
 * The device's reduce-motion setting, live (specs/007-ux-redesign FR-072, X8).
 *
 * A small external store: on the web it follows `prefers-reduced-motion` and its change event; on
 * phones `AccessibilityInfo.isReduceMotionEnabled` and `reduceMotionChanged`. {@link useReducedMotion}
 * re-renders a component when the setting flips while the app is open.
 */
import { useSyncExternalStore } from "react";
import { AccessibilityInfo, Platform } from "react-native";

let reduced = false;
let started = false;
const listeners = new Set<() => void>();

function set(value: boolean): void {
  if (value === reduced) return;
  reduced = value;
  for (const listener of listeners) listener();
}

function start(): void {
  if (started) return;
  started = true;
  if (Platform.OS === "web") {
    if (typeof window === "undefined" || typeof window.matchMedia !== "function") return;
    const query = window.matchMedia("(prefers-reduced-motion: reduce)");
    set(query.matches);
    query.addEventListener?.("change", (event) => set(event.matches));
    return;
  }
  AccessibilityInfo.isReduceMotionEnabled().then(set, () => undefined);
  AccessibilityInfo.addEventListener("reduceMotionChanged", set);
}

/** Whether motion should be reduced right now. */
export function isReducedMotion(): boolean {
  start();
  return reduced;
}

/** Listen for the setting changing; returns the unsubscribe. */
export function subscribeReducedMotion(listener: () => void): () => void {
  start();
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

/** The setting, re-rendering when it changes. */
export function useReducedMotion(): boolean {
  return useSyncExternalStore(subscribeReducedMotion, isReducedMotion, () => false);
}

/** For tests: pretend the setting is `value`. */
export function setReducedMotionForTests(value: boolean): void {
  started = true;
  set(value);
}
