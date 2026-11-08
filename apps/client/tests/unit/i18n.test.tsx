// i18n and RTL (Constitution VIII): every text a screen shows comes from the strings file, every key
// the code uses exists, and a forced right-to-left layout flips the screen with start/end styles only.
import { act, render, screen } from "@testing-library/react-native";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";
import type { TestInstance } from "test-renderer";
import { I18nManager, StyleSheet } from "react-native";


import { applyDirection, i18n, isRtlLanguage, pickLanguage, setupI18n } from "../../src/i18n";
import { SignInScreen } from "../../src/screens/SignInScreen";
import { MyShiftsScreen } from "../../src/screens/my-work/shifts/MyShiftsScreen";
import { FakeApi, json, renderWith, signedIn } from "./fakeApi";
import { Session } from "../../src/api/session";
import { memoryStore } from "../../src/api/tokens";
import { SessionProvider } from "../../src/api/SessionContext";
import en from "../../src/locales/en.json";

const ROOT = join(__dirname, "../..");

function sources(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) return sources(path);
    return /\.tsx?$/.test(name) ? [path] : [];
  });
}

function texts(): string[] {
  return screen.root!
    .queryAll((node: TestInstance) => typeof node.type === "string" && node.type === "Text")
    .flatMap((node: TestInstance) => node.children.filter((c): c is string => typeof c === "string"));
}

afterEach(async () => {
  applyDirection(false);
  await act(async () => {
    await i18n.changeLanguage("en");
  });
});

test("every key the code names exists in the English strings", () => {
  const used = new Set<string>();
  for (const file of [...sources(join(ROOT, "src")), ...sources(join(ROOT, "app"))]) {
    for (const match of readFileSync(file, "utf8").matchAll(/\bt\(\s*"([a-z_.]+)"/g)) used.add(match[1]!);
  }
  expect(used.size).toBeGreaterThan(10);
  expect([...used].filter((key) => !(key in en))).toEqual([]);
});

test("the sign-in screen shows only translated text", async () => {
  await act(async () => {
    await i18n.changeLanguage("cimode");
  });
  await render(
    <SessionProvider session={new Session("http://api.test", memoryStore(), new FakeApi().fetch)}>
      <SignInScreen />
    </SessionProvider>,
  );
  const shown = texts();
  expect(shown.length).toBeGreaterThan(0);
  expect(shown.filter((text) => !(text in en))).toEqual([]);
});

test("the shifts screen shows only translated text around the API's data", async () => {
  const api = new FakeApi().on("GET", "/me/shifts", json(200, { items: [] }));
  const session = await signedIn(api);
  await act(async () => {
    await i18n.changeLanguage("cimode");
  });
  await renderWith(session, <MyShiftsScreen />);
  await screen.findByText("shifts.none_upcoming_title");
  // The page title's eyebrow joins keys with a separator; each part must still be a key.
  expect(texts().filter((text) => !text.split(" / ").every((part) => part in en))).toEqual([]);
});

function styleKeys(): string[] {
  return screen.root!
    .queryAll((node: TestInstance) => typeof node.type === "string" && node.props.style !== undefined)
    .flatMap((node: TestInstance) => Object.keys(StyleSheet.flatten(node.props.style) ?? {}));
}

test("forced RTL sets the screen's direction, and no style names a left or right side", async () => {
  applyDirection(true);
  await render(
    <SessionProvider session={new Session("http://api.test", memoryStore(), new FakeApi().fetch)}>
      <SignInScreen />
    </SessionProvider>,
  );
  expect(StyleSheet.flatten(screen.getByTestId("screen-sign-in").props.style)).toMatchObject({ direction: "rtl" });
  const sided = styleKeys().filter((key) => /left|right/i.test(key));
  expect(sided).toEqual([]);
});

test("left to right by default", async () => {
  await render(
    <SessionProvider session={new Session("http://api.test", memoryStore(), new FakeApi().fetch)}>
      <SignInScreen />
    </SessionProvider>,
  );
  expect(StyleSheet.flatten(screen.getByTestId("screen-sign-in").props.style)).toMatchObject({ direction: "ltr" });
});

test("Hebrew is right to left and English is not", () => {
  expect(isRtlLanguage("he")).toBe(true);
  expect(isRtlLanguage("en")).toBe(false);
});

test("the language comes from the device, falling back to English", () => {
  expect(pickLanguage()).toBe("en");
});

test("setting up Hebrew turns the layout right to left", async () => {
  await act(async () => {
    await setupI18n("he");
  });
  await render(
    <SessionProvider session={new Session("http://api.test", memoryStore(), new FakeApi().fetch)}>
      <SignInScreen />
    </SessionProvider>,
  );
  expect(StyleSheet.flatten(screen.getByTestId("screen-sign-in").props.style)).toMatchObject({ direction: "rtl" });
  expect(screen.getAllByText("כניסה")).toHaveLength(2);
});

test("English stays left to right on a phone still laid out right to left from an earlier run", async () => {
  const native = jest.replaceProperty(I18nManager, "isRTL", true);
  try {
    applyDirection(false);
    await render(
      <SessionProvider session={new Session("http://api.test", memoryStore(), new FakeApi().fetch)}>
        <SignInScreen />
      </SessionProvider>,
    );
    expect(StyleSheet.flatten(screen.getByTestId("screen-sign-in").props.style)).toMatchObject({ direction: "ltr" });
  } finally {
    native.restore();
  }
});
