/**
 * Where the session's tokens are kept (research R9).
 *
 * On a phone they go to `expo-secure-store` (the Keychain or the Android Keystore). On the web they
 * stay in memory only, so a reload signs in again: a refresh token in browser storage could be read
 * by any injected script.
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

/** Tokens kept in the phone's secure store. */
export function secureStore(): TokenStore {
  return {
    load: async () => {
      const raw = await SecureStore.getItemAsync(KEY);
      if (raw === null) return null;
      try {
        const parsed = JSON.parse(raw) as Partial<Tokens>;
        return typeof parsed.access === "string" && typeof parsed.refresh === "string"
          ? { access: parsed.access, refresh: parsed.refresh }
          : null;
      } catch {
        return null;
      }
    },
    save: (tokens) => SecureStore.setItemAsync(KEY, JSON.stringify(tokens)),
    clear: () => SecureStore.deleteItemAsync(KEY),
  };
}

/** The store for platform `os` (`Platform.OS`): memory on the web, the secure store elsewhere. */
export function storeFor(os: string): TokenStore {
  return os === "web" ? memoryStore() : secureStore();
}
