/**
 * Where the session's tokens are kept (research R9, ADR 019).
 *
 * On a phone they go to `expo-secure-store` (the Keychain or the Android Keystore). On the web they
 * are kept per tab (the owner's decision of 2026-11-08): in memory while the page runs, and in the
 * tab's `sessionStorage` only between a `pagehide` and the next load, so a reload or a deep link in
 * the same tab stays signed in and closing the tab ends the session. A tab duplicated, or opened
 * with a copy of the storage, while the page is open finds nothing and signs in again, so two tabs
 * never present the same refresh token (which the API would take for theft and end every session).
 */
import * as SecureStore from "expo-secure-store";

/** The access and refresh tokens of a signed-in session. */
export interface Tokens {
  access: string;
  refresh: string;
}

/** Somewhere to keep the tokens. */
export interface TokenStore {
  load(): Promise<Tokens | null>;
  save(tokens: Tokens): Promise<void>;
  clear(): Promise<void>;
}

const KEY = "hds.session";

/** Tokens kept in memory only; gone when the page or app is closed. */
export function memoryStore(): TokenStore {
  let kept: Tokens | null = null;
  return {
    load: async () => kept,
    save: async (tokens) => {
      kept = tokens;
    },
    clear: async () => {
      kept = null;
    },
  };
}

function parse(raw: string | null | undefined): Tokens | null {
  if (raw === null || raw === undefined) return null;
  try {
    const parsed = JSON.parse(raw) as Partial<Tokens>;
    return typeof parsed.access === "string" && typeof parsed.refresh === "string"
      ? { access: parsed.access, refresh: parsed.refresh }
      : null;
  } catch {
    return null;
  }
}

/** The bits of `sessionStorage` and `window` the tab store uses (both can be missing or throw). */
export interface TabStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}
export interface PageEvents {
  addEventListener(type: "pagehide" | "pageshow", listener: () => void): void;
}

/**
 * Tokens kept per browser tab (ADR 019). On creation the tab's copy is read and removed at once;
 * from then on the tokens live in memory, and are written back only when the page is hidden
 * (`pagehide`: a reload, a navigation away or closing the tab). A page restored from the
 * back-forward cache removes that copy again (`pageshow`). A crash before `pagehide` only means
 * signing in again.
 */
export function tabStore(storage: TabStorage | null, page: PageEvents | null): TokenStore {
  let kept: Tokens | null = null;
  const remove = (): void => {
    try {
      storage?.removeItem(KEY);
    } catch {
      // Blocked storage: the tokens simply stay in memory.
    }
  };
  try {
    kept = parse(storage?.getItem(KEY));
  } catch {
    kept = null;
  }
  remove();
  page?.addEventListener("pagehide", () => {
    if (kept === null) return;
    try {
      storage?.setItem(KEY, JSON.stringify(kept));
    } catch {
      // Blocked storage: the next load signs in again.
    }
  });
  page?.addEventListener("pageshow", remove);
  return {
    load: async () => kept,
    save: async (tokens) => {
      kept = tokens;
    },
    clear: async () => {
      kept = null;
      remove();
    },
  };
}

/** Tokens kept in the phone's secure store. */
export function secureStore(): TokenStore {
  return {
    load: async () => parse(await SecureStore.getItemAsync(KEY)),
    save: (tokens) => SecureStore.setItemAsync(KEY, JSON.stringify(tokens)),
    clear: () => SecureStore.deleteItemAsync(KEY),
  };
}

let webStore: TokenStore | null = null;

function browserStorage(): TabStorage | null {
  try {
    return typeof sessionStorage === "undefined" ? null : sessionStorage;
  } catch {
    return null;
  }
}

/**
 * The store for platform `os` (`Platform.OS`): the tab store on the web, the secure store
 * elsewhere. On the web there is one per page, as its load takes the tab's copy away.
 */
export function storeFor(os: string): TokenStore {
  if (os !== "web") return secureStore();
  const page = typeof window !== "undefined" && typeof window.addEventListener === "function" ? window : null;
  webStore ??= tabStore(browserStorage(), page);
  return webStore;
}

/** For tests: forget the page's web store, as a new page load would. */
export function resetWebStoreForTests(): void {
  webStore = null;
}
