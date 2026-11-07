/**
 * Whether the person is working the web page by keyboard (specs/007-ux-redesign FR-064, X4).
 *
 * The focus ring shows when focus came from the keyboard, as `:focus-visible` would do, and stays
 * away after a pointer press so buttons do not light up on every click. A Tab or arrow key turns
 * keyboard mode on; a pointer press turns it off. Phones are never in keyboard mode.
 */
import { useSyncExternalStore } from "react";
import { Platform } from "react-native";

let keyboard = false;
let started = false;
const listeners = new Set<() => void>();

const KEYS = new Set(["Tab", "ArrowUp", "ArrowDown", "ArrowLeft", "ArrowRight", "Home", "End", "PageUp", "PageDown", "Enter", " ", "Escape"]);

function set(value: boolean): void {
  if (value === keyboard) return;
  keyboard = value;
  for (const listener of listeners) listener();
}

function start(): void {
  if (started || Platform.OS !== "web" || typeof window === "undefined") return;
  started = true;
  window.addEventListener("keydown", (event) => {
    if (KEYS.has(event.key)) set(true);
  });
  window.addEventListener("pointerdown", () => set(false), true);
}

/** Whether focus rings should show right now. */
export function isKeyboardMode(): boolean {
  start();
  return keyboard;
}

/** Listen for the mode changing; returns the unsubscribe. */
export function subscribeKeyboardMode(listener: () => void): () => void {
  start();
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

/** Whether to draw a focus ring on a focused control. */
export function useKeyboardMode(): boolean {
  return useSyncExternalStore(subscribeKeyboardMode, isKeyboardMode, () => false);
}
