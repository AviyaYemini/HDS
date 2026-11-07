/**
 * Motion for the design (specs/007-ux-redesign FR-070 to FR-072, research R14, task T021).
 *
 * Three durations and two easings from the tokens, every one of them zero under reduce-motion; the
 * transitions the primitives share (enter, exit, surface, feedback); and `Presence`, which mounts,
 * slides and fades a child from the leading edge, the bottom or in place. React Native's `Animated`
 * on the native driver: transforms and opacity only, so nothing blocks input.
 */
import { useEffect, useMemo, useState } from "react";
import { Animated, Easing, Platform, type EasingFunction } from "react-native";

import { motion as tokens, component } from "../theme/theme.generated";
import { useReducedMotion } from "./reducedMotion";

export { isReducedMotion, setReducedMotionForTests, useReducedMotion } from "./reducedMotion";

/** A transition the tokens name. */
export type TransitionName = keyof typeof component.motion;

/** One easing as React Native's `Animated` wants it. */
export const easings: Record<keyof typeof tokens.easing, EasingFunction> = {
  standard: Easing.bezier(...tokens.easing.standard),
  emphasized: Easing.bezier(...tokens.easing.emphasized),
};

/** A resolved transition: milliseconds (0 under reduce-motion) and an easing. */
export interface Transition {
  duration: number;
  easing: EasingFunction;
}

/** The transition `name` stands for, honouring `reduced`. */
export function transition(name: TransitionName, reduced: boolean): Transition {
  const spec = component.motion[name];
  const [x1, y1, x2, y2] = spec.timingFunction;
  return { duration: reduced ? 0 : spec.duration, easing: Easing.bezier(x1, y1, x2, y2) };
}

/** The durations, easings and transitions for the current reduce-motion setting. */
export function useMotion() {
  const reduced = useReducedMotion();
  return useMemo(
    () => ({
      reduced,
      duration: {
        fast: reduced ? 0 : tokens.duration.fast,
        base: reduced ? 0 : tokens.duration.base,
        slow: reduced ? 0 : tokens.duration.slow,
      },
      easings,
      transition: (name: TransitionName) => transition(name, reduced),
    }),
    [reduced],
  );
}

// On the web the frames come from requestAnimationFrame and performance.now, not from Date.now:
// React Native's own timing reads Date.now, which a test's fixed clock freezes, and the browser has
// no native driver anyway. Each value runs one animation at a time; a new one replaces the last.
const running = new WeakMap<Animated.Value, number>();

function currentValue(value: Animated.Value): number {
  let current = 0;
  value.stopAnimation((v) => {
    current = v;
  });
  return current;
}

function driveOnWeb(value: Animated.Value, to: number, duration: number, easing: EasingFunction, loop: boolean): () => void {
  const token = (running.get(value) ?? 0) + 1;
  running.set(value, token);
  const from = loop ? 0 : currentValue(value);
  let start: number | null = null;
  let frame = 0;
  let onDone: (() => void) | null = null;
  const step = (now: number): void => {
    if (running.get(value) !== token) return;
    if (start === null) start = now;
    const elapsed = now - start;
    const t = Math.min(1, elapsed / duration);
    value.setValue(from + (to - from) * easing(t));
    if (t < 1) {
      frame = requestAnimationFrame(step);
    } else if (loop) {
      start = now;
      value.setValue(from);
      frame = requestAnimationFrame(step);
    } else {
      onDone?.();
    }
  };
  frame = requestAnimationFrame(step);
  const stop = (): void => {
    if (running.get(value) === token) running.delete(value);
    cancelAnimationFrame(frame);
  };
  (stop as { then?: (cb: () => void) => void }).then = (cb) => {
    onDone = cb;
  };
  return stop;
}

/** Run `value` to `to` with `spec`; resolves when done. Zero duration jumps. */
export function animateTo(value: Animated.Value, to: number, spec: Transition): Promise<void> {
  return new Promise((resolve) => {
    if (spec.duration === 0) {
      value.setValue(to);
      resolve();
      return;
    }
    if (Platform.OS === "web") {
      const stop = driveOnWeb(value, to, spec.duration, spec.easing, false);
      (stop as unknown as { then: (cb: () => void) => void }).then(resolve);
      return;
    }
    Animated.timing(value, { toValue: to, duration: spec.duration, easing: spec.easing, useNativeDriver: true }).start(
      () => resolve(),
    );
  });
}

/**
 * Sweep `value` from 0 to 1 over `duration` again and again (the loading bar, the shimmer) until
 * the returned function is called. Linear.
 */
export function startLoop(value: Animated.Value, duration: number): () => void {
  if (Platform.OS === "web") return driveOnWeb(value, 1, duration, Easing.linear, true);
  const loop = Animated.loop(Animated.timing(value, { toValue: 1, duration, easing: Easing.linear, useNativeDriver: true }));
  loop.start();
  return () => loop.stop();
}

/**
 * Keep `visible` content mounted while it enters and leaves: `shown` stays true until the exit
 * ends, and `progress` runs 0 → 1 on enter (the `enter` transition) and 1 → 0 on exit (`exit`), or
 * `surface` for large surfaces such as sheets and side panels.
 */
export function usePresence(visible: boolean, kind: "enter" | "surface" = "enter") {
  const { reduced } = useMotion();
  const [progress] = useState(() => new Animated.Value(visible ? 1 : 0));
  // `leaving` is derived while rendering (React's pattern for state that follows a prop), so the
  // content stays mounted the moment `visible` turns false and the exit can play.
  const [wasVisible, setWasVisible] = useState(visible);
  const [leaving, setLeaving] = useState(false);
  if (wasVisible !== visible) {
    setWasVisible(visible);
    if (!visible) setLeaving(true);
  }
  useEffect(() => {
    let live = true;
    if (visible) {
      void animateTo(progress, 1, transition(kind, reduced));
    } else {
      void animateTo(progress, 0, transition("exit", reduced)).then(() => {
        if (live) setLeaving(false);
      });
    }
    return () => {
      live = false;
    };
  }, [visible, kind, reduced, progress]);
  return { shown: visible || leaving, progress };
}
