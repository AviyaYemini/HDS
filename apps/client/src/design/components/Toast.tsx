/**
 * Toasts (specs/007-ux-redesign FR-047, FR-071, FR-077): short notices for news that is not
 * critical, in the past tense ("Availability submitted"). They rise from the bottom, stay four
 * seconds (six with an action), never more than three at once, and are announced politely.
 * Anything critical, or anything touching pay, belongs in an `InlineBanner` instead.
 */
import { useEffect, useSyncExternalStore } from "react";
import { useTranslation } from "react-i18next";
import { Animated, StyleSheet, View } from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";

import { Icon } from "../icons/Icon";
import { usePresence } from "../motion";
import { useTheme } from "../theme/ThemeProvider";
import { Button } from "./Button";
import { SurfaceContext, Txt } from "./Text";

/** The toast's meaning; `success` and `info` are the usual ones. */
export type ToastTone = "success" | "info" | "warning";

export interface ToastOptions {
  tone?: ToastTone;
  action?: { label: string; onPress: () => void };
  /** Milliseconds on screen; 4000, or 6000 with an action. */
  duration?: number;
  testID?: string;
}

interface ToastItem extends ToastOptions {
  id: number;
  message: string;
  /** False once it is leaving. */
  live: boolean;
}

let nextId = 1;
let toasts: ToastItem[] = [];
const listeners = new Set<() => void>();
const MAX = 3;

function emit(): void {
  for (const listener of listeners) listener();
}

/** Show a toast. Returns its id, for `dismissToast`. */
export function showToast(message: string, options: ToastOptions = {}): number {
  const id = nextId++;
  const item: ToastItem = { id, message, live: true, ...options };
  toasts = [...toasts.filter((toast) => toast.live).slice(-(MAX - 1)), item];
  emit();
  return id;
}

/** Start a toast leaving; it unmounts when its exit ends. */
export function dismissToast(id: number): void {
  toasts = toasts.map((toast) => (toast.id === id ? { ...toast, live: false } : toast));
  emit();
}

function remove(id: number): void {
  toasts = toasts.filter((toast) => toast.id !== id);
  emit();
}

/** For tests. */
export function clearToasts(): void {
  toasts = [];
  emit();
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

function ToastView({ toast }: { toast: ToastItem }) {
  const { colors } = useTheme();
  const { t } = useTranslation();
  const { shown, progress } = usePresence(toast.live, "enter");
  useEffect(() => {
    if (!toast.live) return undefined;
    const timer = setTimeout(() => dismissToast(toast.id), toast.duration ?? (toast.action ? 6000 : 4000));
    return () => clearTimeout(timer);
  }, [toast]);
  useEffect(() => {
    if (!shown && !toast.live) remove(toast.id);
  }, [shown, toast]);
  if (!shown) return null;
  const tone = toast.tone ?? "success";
  const color = tone === "warning" ? colors.status.warning : tone === "success" ? colors.status.success : colors.status.info;
  return (
    <Animated.View
      testID={toast.testID}
      role="status"
      accessibilityLiveRegion="polite"
      style={[
        styles.toast,
        { backgroundColor: colors.inverse.bg, borderColor: colors.inverse.border },
        {
          opacity: progress,
          transform: [{ translateY: progress.interpolate({ inputRange: [0, 1], outputRange: [16, 0] }) }],
        },
      ]}
    >
      <SurfaceContext.Provider value="inverse">
        <Icon name={tone === "success" ? "check" : tone === "warning" ? "warning" : "info"} size={18} color={colors.inverse.text} />
        <Txt role="body" style={styles.message}>
          {toast.message}
        </Txt>
        {toast.action ? (
          <Button
            variant="link"
            label={toast.action.label}
            onPress={() => {
              toast.action?.onPress();
              dismissToast(toast.id);
            }}
          />
        ) : null}
        <Button variant="quiet" icon="close" label="" accessibilityLabel={t("common.dismiss")} onPress={() => dismissToast(toast.id)} />
        <View style={[styles.rule, { backgroundColor: color }]} />
      </SurfaceContext.Provider>
    </Animated.View>
  );
}

/** Mount once, near the root: draws the toasts above everything, clear of the bottom inset. */
export function ToastHost() {
  const items = useSyncExternalStore(subscribe, () => toasts, () => toasts);
  const insets = useSafeAreaInsets();
  const { isDesktop } = useTheme();
  if (items.length === 0) return null;
  return (
    <View
      pointerEvents="box-none"
      style={[styles.host, { bottom: Math.max(insets.bottom, 16) + 60 }, isDesktop ? styles.hostDesktop : null]}
    >
      {items.map((toast) => (
        <ToastView key={toast.id} toast={toast} />
      ))}
    </View>
  );
}

const styles = StyleSheet.create({
  host: { position: "absolute", start: 16, end: 16, gap: 8, alignItems: "stretch" },
  hostDesktop: { end: undefined, width: 420 },
  toast: {
    flexDirection: "row",
    alignItems: "center",
    gap: 12,
    paddingStart: 16,
    paddingEnd: 4,
    paddingVertical: 8,
    borderWidth: 1,
    overflow: "hidden",
  },
  message: { flex: 1 },
  rule: { position: "absolute", start: 0, top: 0, bottom: 0, width: 3 },
});
