// Jest setup: English strings, a fake secure store, and a router whose calls the tests can read.
import { setupI18n } from "../../src/i18n";

jest.mock("expo-secure-store", () => {
  const items = new Map<string, string>();
  return {
    __items: items,
    getItemAsync: jest.fn(async (key: string) => items.get(key) ?? null),
    setItemAsync: jest.fn(async (key: string, value: string) => {
      items.set(key, value);
    }),
    deleteItemAsync: jest.fn(async (key: string) => {
      items.delete(key);
    }),
  };
});

jest.mock("expo-localization", () => ({ getLocales: () => [{ languageCode: "en" }] }));

jest.mock("expo-router", () => {
  const router = { replace: jest.fn(), push: jest.fn(), back: jest.fn() };
  return {
    __router: router,
    useRouter: () => router,
    Redirect: () => null,
    Link: ({ children }: { children: unknown }) => children,
    Slot: () => null,
  };
});

void setupI18n("en");
