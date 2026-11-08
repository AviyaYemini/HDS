/**
 * Switching the language restarts the app in it (specs/007-ux-redesign FR-078, T105; the owner,
 * 2026-11-08: "as long as youre not going back to the home screen and view a loading/please wait
 * animation - yes / the user has to know the app didnt crach").
 *
 * At once, a full-screen "Switching to …" state in the new language; the choice and the screen the
 * person is on are kept; then the app restarts (the web reloads the page, a phone reloads its
 * bundle) and comes back to that screen, still signed in. A phone that cannot reload keeps the old
 * path: the language turns now and the layout direction after the next start.
 */
import AsyncStorage from "@react-native-async-storage/async-storage";
import { reloadAppAsync } from "expo";
import { useSyncExternalStore } from "react";
import { Platform } from "react-native";

import { rememberLanguage, setupI18n } from "./index";

const RESUME_KEY = "hds.resume";
/** A resume older than this is not this restart's: ignored. */
export const RESUME_FRESH_MS = 60_000;

/** Where to come back to after a restart, and when it was saved (ms since the epoch). */
export interface Resume {
  path: string;
  at: number;
}

/** A path inside this app: one leading slash, never a protocol-relative `//host`. */
export function isAppPath(path: string): boolean {
  return path.startsWith("/") && !path.startsWith("//") && !path.includes("\\");
}

/** The screen's address with its query, leaving out `lang` (the restart sets the language). */
export function resumePath(pathname: string, params: Record<string, string | string[] | undefined>): string {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (key === "lang" || value === undefined) continue;
    for (const one of Array.isArray(value) ? value : [value]) query.append(key, one);
  }
  const text = query.toString();
  return text ? `${pathname}?${text}` : pathname;
}

async function store(value: string | null): Promise<void> {
  try {
    if (Platform.OS === "web") {
      // This tab's own storage: it survives the reload and no other tab reads it.
      if (typeof window === "undefined") return;
      if (value === null) window.sessionStorage.removeItem(RESUME_KEY);
      else window.sessionStorage.setItem(RESUME_KEY, value);
    } else if (value === null) {
      await AsyncStorage.removeItem(RESUME_KEY);
    } else {
      await AsyncStorage.setItem(RESUME_KEY, value);
    }
  } catch {
    // Without storage the restart lands on the start screen's choice; nothing else breaks.
  }
}

async function read(): Promise<string | null> {
  try {
    if (Platform.OS === "web") return typeof window === "undefined" ? null : window.sessionStorage.getItem(RESUME_KEY);
    return await AsyncStorage.getItem(RESUME_KEY);
  } catch {
    return null;
  }
}

/** Keep `path` to come back to after the restart. */
export async function saveResume(path: string, now: number = Date.now()): Promise<void> {
  if (!isAppPath(path)) return;
  await store(JSON.stringify({ path, at: now } satisfies Resume));
}

/** The kept resume, taken once: removed whatever it holds, and null unless fresh and in the app. */
export async function takeResume(now: number = Date.now()): Promise<Resume | null> {
  const text = await read();
  if (text === null) return null;
  await store(null);
  try {
    const kept = JSON.parse(text) as Partial<Resume>;
    if (typeof kept.path !== "string" || typeof kept.at !== "number") return null;
    if (!isAppPath(kept.path) || now - kept.at > RESUME_FRESH_MS || kept.at > now + 5_000) return null;
    return { path: kept.path, at: kept.at };
  } catch {
    return null;
  }
}

// ── The switching state ─────────────────────────────────────────────────────

let switching: string | null = null;
const listeners = new Set<() => void>();

function setSwitching(language: string | null): void {
  switching = language;
  for (const listener of listeners) listener();
}

/** The language being switched to while the app restarts, or null. */
export function useSwitching(): string | null {
  return useSyncExternalStore(
    (listener) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    () => switching,
    () => switching,
  );
}

/** Two frames: the switching state is on screen before anything slow starts. */
function painted(): Promise<void> {
  return new Promise((resolve) => {
    if (typeof requestAnimationFrame === "undefined") {
      setTimeout(resolve, 0);
      return;
    }
    requestAnimationFrame(() => requestAnimationFrame(() => resolve()));
  });
}

export interface RestartDeps {
  /** Reload the web page (`location.reload()` by default). */
  reloadPage?: () => void;
  /** Reload a phone's bundle (expo's `reloadAppAsync()` by default). */
  reloadApp?: () => Promise<void>;
  /** Told when a phone could not reload: the old notice ("restart to finish"). */
  onRestartNeeded?: () => void;
  platform?: string;
  /** How long to wait for a phone's reload to begin (3 s by default). */
  reloadGraceMs?: number;
}

/** How long a phone's reload may take to begin before it counts as not having happened. */
const RELOAD_GRACE_MS = 3000;

/**
 * Reload a phone's bundle. Expo's reload quietly does nothing when its native side is missing, so
 * that counts as a failure here, never a switching state left on screen.
 */
async function reloadBundle(): Promise<void> {
  const native = (globalThis as { expo?: { reloadAppAsync?: unknown } }).expo;
  if (typeof native?.reloadAppAsync !== "function") throw new Error("no native reload");
  await reloadAppAsync("language");
}

/**
 * Switch to `language` by restarting in it, coming back to `path`. The choice is stored before
 * anything restarts, so the restart always starts in it.
 */
export async function restartInLanguage(language: string, path: string, deps: RestartDeps = {}): Promise<void> {
  const platform = deps.platform ?? Platform.OS;
  setSwitching(language);
  await painted();
  await rememberLanguage(language);
  await saveResume(path);
  if (platform === "web") {
    if (typeof window !== "undefined") {
      // `?lang=` would win over the stored choice after the reload.
      const url = new URL(window.location.href);
      if (url.searchParams.has("lang")) {
        url.searchParams.delete("lang");
        window.history.replaceState(window.history.state, "", url.toString());
      }
    }
    (deps.reloadPage ?? (() => window.location.reload()))();
    return;
  }
  try {
    await (deps.reloadApp ?? reloadBundle)();
    // A reload that worked never comes back here: still running means it did not happen.
    await new Promise((resolve) => setTimeout(resolve, deps.reloadGraceMs ?? RELOAD_GRACE_MS));
    throw new Error("the app did not reload");
  } catch {
    // No reload here: turn the language now and say the layout follows at the next start.
    await takeResume();
    await setupI18n(language);
    setSwitching(null);
    deps.onRestartNeeded?.();
  }
}

/** For tests: nothing switching. */
export function resetSwitching(): void {
  setSwitching(null);
}
