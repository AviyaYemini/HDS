/**
 * Leaving with unsaved changes asks first (specs/007-ux-redesign FR-045, T084). A form says while
 * it holds changes; any in-app link pressed then (a LinkPressable, the navigation included) opens
 * `confirm-leave` instead of going, and closing or reloading the browser tab asks the browser's own
 * question.
 */
import { router } from "expo-router";
import { useEffect, useSyncExternalStore } from "react";
import { useTranslation } from "react-i18next";
import { Platform, StyleSheet, View } from "react-native";

import { Button } from "./components/Button";
import { Sheet } from "./components/Sheet";
import { Txt } from "./components/Text";

const dirty = new Set<symbol>();
/** Where a held navigation was going: an address, or what to do instead (closing a panel). */
let leavingTo: string | (() => void) | null = null;
const listeners = new Set<() => void>();

function emit(): void {
  for (const listener of listeners) listener();
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

/** Whether any form on screen holds unsaved changes. */
export function hasUnsaved(): boolean {
  return dirty.size > 0;
}

/** Mark a form as holding unsaved changes while `isDirty` is true; cleared when it goes. */
export function useUnsavedChanges(isDirty: boolean): void {
  useEffect(() => {
    if (!isDirty) return undefined;
    const token = Symbol("form");
    dirty.add(token);
    emit();
    return () => {
      dirty.delete(token);
      emit();
    };
  }, [isDirty]);
  useEffect(() => {
    if (Platform.OS !== "web" || typeof window === "undefined" || !isDirty) return undefined;
    const ask = (event: BeforeUnloadEvent) => {
      event.preventDefault();
      // Older browsers need a value; every browser shows its own words.
      event.returnValue = "";
    };
    window.addEventListener("beforeunload", ask);
    return () => window.removeEventListener("beforeunload", ask);
  }, [isDirty]);
}

/**
 * Called before leaving for `href` (an address, or an action such as closing a panel): true to go
 * on, false when the question was asked instead; leaving anyway then goes or runs it.
 */
export function mayLeaveFor(href: string | (() => void)): boolean {
  if (!hasUnsaved()) return true;
  leavingTo = href;
  emit();
  return false;
}

/** For tests: nothing unsaved, no question open. */
export function resetUnsaved(): void {
  dirty.clear();
  leavingTo = null;
  emit();
}

/** The "leave without saving?" question; mounted once, over everything. */
export function ConfirmLeave() {
  const { t } = useTranslation();
  const to = useSyncExternalStore(subscribe, () => leavingTo, () => leavingTo);
  const stay = () => {
    leavingTo = null;
    emit();
  };
  const leave = () => {
    const target = leavingTo;
    dirty.clear();
    leavingTo = null;
    emit();
    if (typeof target === "function") target();
    else if (target) router.push(target as never);
  };
  return (
    <Sheet open={to !== null} onClose={stay} title={t("unsaved.title")} testID="confirm-leave">
      <View style={styles.body}>
        <Txt role="body">{t("unsaved.body")}</Txt>
        <View style={styles.actions}>
          <Button variant="primary" label={t("unsaved.stay")} onPress={stay} testID="action-stay" />
          <Button variant="secondary" label={t("unsaved.leave")} onPress={leave} testID="action-leave" />
        </View>
      </View>
    </Sheet>
  );
}

const styles = StyleSheet.create({
  body: { gap: 16 },
  actions: { flexDirection: "row", flexWrap: "wrap", gap: 8 },
});
