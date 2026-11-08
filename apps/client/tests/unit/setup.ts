// Jest setup: English strings, a fake secure store, and a router whose calls the tests can read.
import { Pressable, ScrollView, StyleSheet, Text, TextInput, View } from "react-native";

import { setupI18n } from "../../src/i18n";

// React Native loads each component the first time it is read. Read the ones the screens use here,
// before any test starts, so that loading and compiling them on a cold CI runner does not count
// against the first test's timeout.
void [Pressable, ScrollView, StyleSheet, Text, TextInput, View];

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
  const React = jest.requireActual<typeof import("react")>("react");
  const { Text } = jest.requireActual<typeof import("react-native")>("react-native");
  const router = { replace: jest.fn(), push: jest.fn(), back: jest.fn() };
  const location = { pathname: "/" };
  return {
    __router: router,
    /** Where the tests say the app is; `usePathname` reads it. */
    __setPathname: (pathname: string) => {
      location.pathname = pathname;
    },
    useRouter: () => router,
    usePathname: () => location.pathname,
    useLocalSearchParams: () => ({}),
    useGlobalSearchParams: () => ({}),
    // A redirect renders where it points, so a test can read it.
    Redirect: ({ href }: { href: string }) => React.createElement(Text, { testID: "redirect" }, href),
    Link: ({ children }: { children: unknown }) => children,
    Slot: () => null,
  };
});

// The design's primitives (specs/007): fonts count as loaded, safe areas are zero, and SVG draws
// as plain views, so the tests check roles, names and states rather than pixels.
// The package's own in-memory mock; a jest.mock factory cannot use import.
jest.mock("@react-native-async-storage/async-storage", () =>
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  require("@react-native-async-storage/async-storage/jest/async-storage-mock"),
);
jest.mock("expo-font", () => ({ useFonts: () => [true, null], loadAsync: jest.fn(async () => undefined), isLoaded: () => true }));
jest.mock("react-native-safe-area-context", () => {
  const insets = { top: 0, bottom: 0, left: 0, right: 0 };
  return {
    SafeAreaProvider: ({ children }: { children: unknown }) => children,
    SafeAreaView: ({ children }: { children: unknown }) => children,
    useSafeAreaInsets: () => insets,
    useSafeAreaFrame: () => ({ x: 0, y: 0, width: 390, height: 844 }),
    initialWindowMetrics: { insets, frame: { x: 0, y: 0, width: 390, height: 844 } },
  };
});
jest.mock("react-native-svg", () => {
  /* eslint-disable @typescript-eslint/no-require-imports */
  const React = require("react");
  const { View } = require("react-native");
  /* eslint-enable @typescript-eslint/no-require-imports */
  const stub = (name: string) => {
    const Stub = (props: Record<string, unknown>) => React.createElement(View, { ...props, testID: props.testID ?? `svg-${name}` });
    Stub.displayName = name;
    return Stub;
  };
  return {
    __esModule: true,
    default: stub("Svg"),
    Svg: stub("Svg"),
    Path: stub("Path"),
    Polygon: stub("Polygon"),
    Polyline: stub("Polyline"),
    Line: stub("Line"),
    Circle: stub("Circle"),
    Rect: stub("Rect"),
  };
});

void setupI18n("en");
