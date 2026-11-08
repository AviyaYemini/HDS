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

import { storeFor } from "../api/tokens";
import { safeReturnPath } from "../auth/returnPath";
import { applyDirection, isRtlLanguage, rememberLanguage, setupI18n } from "./index";

const RESUME_KEY = "hds.resume";
/** A resume older than this is not this restart's: ignored. */
export const RESUME_FRESH_MS = 60_000;
/**
 * Only a resume this fresh skips the second unlock: a restart takes seconds, so an app killed
 * mid-switch and opened again later asks as usual (group O review).
 */
export const UNLOCK_SKIP_MS = 15_000;

/**
 * Where to come back to after a restart, when it was saved (ms since the epoch), and the session
 * that switched (its `sid`, else its user), so only that session skips a second unlock.
 */
export interface Resume {
  path: string;
  at: number;
  owner: string | null;
}

/** A path inside this app, by the one rule the sign-in's return path uses. */
export function isAppPath(path: string): boolean {
  return safeReturnPath(path) === path;
}

/** The session an access token belongs to: its `sid`, else its `sub`; null when unreadable. */
export function sessionOf(access: string | null | undefined): string | null {
  const part = access?.split(".")[1];
  if (!part || typeof atob === "undefined") return null;
  try {
    const json = atob(part.replace(/-/g, "+").replace(/_/g, "/").padEnd(Math.ceil(part.length / 4) * 4, "="));
    const claims = JSON.parse(json) as { sid?: unknown; sub?: unknown };
    const owner = claims.sid ?? claims.sub;
    return typeof owner === "string" ? owner : null;
  } catch {
    return null;
  }
}

/**
 * The screen's address with its query: `params` already without the route's own segments and
 * `lang` (the sign-in redirect's `queryOnly`), so `/runs/abc` never becomes `/runs/abc?id=abc`.
 */
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

/** Keep `path` to come back to after the restart, for the session `owner`. */
export async function saveResume(path: string, owner: string | null, now: number = Date.now()): Promise<void> {
  if (!isAppPath(path)) return;
  await store(JSON.stringify({ path, at: now, owner } satisfies Resume));
}

/** Forget any kept resume (signing out: it must never outlive the switch). */
export async function clearResume(): Promise<void> {
  await store(null);
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
    return { path: kept.path, at: kept.at, owner: typeof kept.owner === "string" ? kept.owner : null };
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
  /** How long to wait for a phone's reload to begin (8 s by default). */
  reloadGraceMs?: number;
}

/** How long a phone's reload may take to begin before it counts as not having happened. */
const RELOAD_GRACE_MS = 8000;

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
  // A phone takes its layout direction from the native setting at launch: set it before the
  // reload, so the restarted app starts laid out in the new language (group O review).
  if (platform !== "web") applyDirection(isRtlLanguage(language), language);
  // Only a phone asks for biometrics at the start, so only a phone records whose switch it was
  // (the web's tab store is read-and-remove, and is not touched here).
  const kept = platform === "web" ? null : await storeFor(platform).load().catch(() => null);
  await saveResume(path, sessionOf(kept?.access));
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
  } catch {
    // No reload at all: turn the language now and say the layout follows at the next start.
    await takeResume();
    await setupI18n(language);
    setSwitching(null);
    deps.onRestartNeeded?.();
    return;
  }
  // A reload that worked never comes back here. Still running after the grace: turn the language
  // now so no one is left on the switching state, but keep the saved screen, since a slow reload
  // may still come and should land on it (group O review).
  await new Promise((resolve) => setTimeout(resolve, deps.reloadGraceMs ?? RELOAD_GRACE_MS));
  await setupI18n(language);
  setSwitching(null);
  deps.onRestartNeeded?.();
}

/** For tests: nothing switching. */
export function resetSwitching(): void {
  setSwitching(null);
}

/** Whether `resume` lets this start skip a second unlock: only the very session that switched. */
export function skipsUnlock(resume: Resume | null, keptAccess: string | null | undefined, now: number = Date.now()): boolean {
  const owner = sessionOf(keptAccess);
  if (resume === null || resume.owner === null || owner === null || resume.owner !== owner) return false;
  return now - resume.at <= UNLOCK_SKIP_MS;
}
