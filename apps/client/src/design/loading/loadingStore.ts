/**
 * What the loading bar shows (specs/007-ux-redesign FR-074, research R15, task T022).
 *
 * Any pending work (a route change, an API request, an explicit task) raises a counter. The bar
 * appears only once something has been pending for `showAfter` (300 ms), so a fast load never
 * flashes, and once shown it stays at least `minVisible` (400 ms). A determinate value (schedule
 * runs) replaces the indeterminate sweep and carries a side text.
 */
import { useSyncExternalStore } from "react";

import { component } from "../theme/theme.generated";

/** The bar's state as a component sees it. */
export interface LoadingState {
  visible: boolean;
  /** 0 to 1, or null while indeterminate. */
  value: number | null;
  /** Text shown beside a determinate bar ("best so far: 12 / 14 filled"). */
  text: string | null;
}

const SHOW_AFTER = component.loadingBar.showAfter;
const MIN_VISIBLE = component.loadingBar.minVisible;

let pending = 0;
let state: LoadingState = { visible: false, value: null, text: null };
let showTimer: ReturnType<typeof setTimeout> | null = null;
let hideTimer: ReturnType<typeof setTimeout> | null = null;
let shownAt = 0;
const listeners = new Set<() => void>();

function emit(next: Partial<LoadingState>): void {
  state = { ...state, ...next };
  for (const listener of listeners) listener();
}

function clearTimers(): void {
  if (showTimer) clearTimeout(showTimer);
  if (hideTimer) clearTimeout(hideTimer);
  showTimer = null;
  hideTimer = null;
}

function reconsider(): void {
  if (pending > 0) {
    if (hideTimer) {
      clearTimeout(hideTimer);
      hideTimer = null;
    }
    if (!state.visible && !showTimer) {
      showTimer = setTimeout(() => {
        showTimer = null;
        if (pending > 0) {
          shownAt = Date.now();
          emit({ visible: true });
        }
      }, SHOW_AFTER);
    }
    return;
  }
  if (showTimer) {
    clearTimeout(showTimer);
    showTimer = null;
  }
  if (state.visible && !hideTimer) {
    const remaining = Math.max(0, MIN_VISIBLE - (Date.now() - shownAt));
    hideTimer = setTimeout(() => {
      hideTimer = null;
      if (pending === 0) emit({ visible: false, value: null, text: null });
    }, remaining);
  }
}

/** Note that something started; call the returned function when it ends. */
export function startLoading(): () => void {
  pending += 1;
  reconsider();
  let done = false;
  return () => {
    if (done) return;
    done = true;
    pending = Math.max(0, pending - 1);
    reconsider();
  };
}

/**
 * Run `work` under the bar; the promise is returned as it is. `background` work (a status poll, a
 * token refresh) is not counted, so it never keeps the bar showing (PR 1 review).
 */
export function track<T>(work: Promise<T>, { background = false }: { background?: boolean } = {}): Promise<T> {
  if (background) return work;
  const done = startLoading();
  return work.finally(done);
}

let progressSet = 0;
let progressTimer: ReturnType<typeof setTimeout> | null = null;

/** Switch to a determinate bar at `value` (0 to 1) with `text`; null for indeterminate again. */
export function setProgress(value: number | null, text: string | null = null): void {
  progressSet += 1;
  emit({ value: value === null ? null : Math.min(1, Math.max(0, value)), text });
}

/**
 * Finish a determinate bar: full at once, then, after the minimum it stays shown, indeterminate
 * again unless something set a value since, so loads still pending (or the next one) never inherit
 * it (group E review).
 */
export function endProgress(): void {
  setProgress(1, state.text);
  const mine = progressSet;
  if (progressTimer) clearTimeout(progressTimer);
  progressTimer = setTimeout(() => {
    progressTimer = null;
    if (progressSet === mine) emit({ value: null, text: null });
  }, MIN_VISIBLE);
}

/** The current state. */
export function getLoadingState(): LoadingState {
  return state;
}

/** Listen for changes; returns the unsubscribe. */
export function subscribeLoading(listener: () => void): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

/** The bar's state, re-rendering on change. */
export function useLoadingState(): LoadingState {
  return useSyncExternalStore(subscribeLoading, getLoadingState, getLoadingState);
}

/** For tests: nothing pending, nothing shown. */
export function resetLoading(): void {
  clearTimers();
  if (progressTimer) clearTimeout(progressTimer);
  progressTimer = null;
  pending = 0;
  emit({ visible: false, value: null, text: null });
}
