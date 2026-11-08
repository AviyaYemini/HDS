/**
 * The loading store, motion and theme resolution (specs/007-ux-redesign FR-061, FR-072, FR-074,
 * tasks T016, T021, T022): the bar waits 300 ms and stays 400 ms, reduce-motion zeroes every
 * duration, and the four themes follow the device unless overridden.
 */
import { getLoadingState, resetLoading, setProgress, startLoading, track } from "../../../src/design/loading/loadingStore";
import { setReducedMotionForTests, transition } from "../../../src/design/motion";
import { resolveThemeName } from "../../../src/design/theme/ThemeProvider";
import AsyncStorage from "@react-native-async-storage/async-storage";
import { Platform } from "react-native";

import {
  getThemeState,
  hydrateThemePreference,
  parsePreference,
  resetThemeStore,
  setThemePreference,
} from "../../../src/design/theme/themeStore";
import { matches, normalise, order, textDirection } from "../../../src/design/components";

describe("loading store (FR-074)", () => {
  beforeEach(() => {
    jest.useFakeTimers();
    resetLoading();
  });
  afterEach(() => jest.useRealTimers());

  it("shows nothing for a fast load", () => {
    const done = startLoading();
    jest.advanceTimersByTime(200);
    done();
    jest.advanceTimersByTime(1000);
    expect(getLoadingState().visible).toBe(false);
  });

  it("appears after 300 ms and stays at least 400 ms", async () => {
    const done = startLoading();
    jest.advanceTimersByTime(299);
    expect(getLoadingState().visible).toBe(false);
    jest.advanceTimersByTime(1);
    expect(getLoadingState().visible).toBe(true);
    done();
    jest.advanceTimersByTime(399);
    expect(getLoadingState().visible).toBe(true);
    jest.advanceTimersByTime(1);
    expect(getLoadingState().visible).toBe(false);
  });

  it("tracks a promise and carries a determinate value", async () => {
    let resolve: () => void = () => undefined;
    const work = new Promise<void>((r) => {
      resolve = r;
    });
    const tracked = track(work);
    jest.advanceTimersByTime(300);
    expect(getLoadingState().visible).toBe(true);
    setProgress(0.5, "best so far: 7 / 14");
    expect(getLoadingState()).toMatchObject({ value: 0.5, text: "best so far: 7 / 14" });
    setProgress(1.7);
    expect(getLoadingState().value).toBe(1);
    resolve();
    await tracked;
    jest.advanceTimersByTime(400);
    expect(getLoadingState()).toMatchObject({ visible: false, value: null, text: null });
  });
});

describe("motion (FR-070, FR-072)", () => {
  it("uses the token durations, and none under reduce-motion", () => {
    expect(transition("enter", false).duration).toBe(200);
    expect(transition("exit", false).duration).toBe(120);
    expect(transition("surface", false).duration).toBe(320);
    expect(transition("surface", true).duration).toBe(0);
    setReducedMotionForTests(true);
    setReducedMotionForTests(false);
  });
});

describe("themes (FR-061)", () => {
  it("follow the device unless overridden", () => {
    expect(resolveThemeName("system", false, false)).toBe("light");
    expect(resolveThemeName("system", true, false)).toBe("dark");
    expect(resolveThemeName("system", false, true)).toBe("highContrastLight");
    expect(resolveThemeName("system", true, true)).toBe("highContrastDark");
    expect(resolveThemeName("dark", false, false)).toBe("dark");
  });

  it("read a theme name from the address or storage", () => {
    expect(parsePreference("dark")).toBe("dark");
    expect(parsePreference("high-contrast-light")).toBe("highContrastLight");
    expect(parsePreference("HIGHCONTRASTDARK")).toBe("highContrastDark");
    expect(parsePreference("purple")).toBeNull();
  });

  it("refuse the names of Object's own members (PR 1 review)", () => {
    expect(parsePreference("constructor")).toBeNull();
    expect(parsePreference("toString")).toBeNull();
    expect(parsePreference("__proto__")).toBeNull();
  });
});

describe("the theme choice on phones (X12)", () => {
  beforeEach(async () => {
    await AsyncStorage.clear();
    resetThemeStore();
  });

  it("runs as a phone", () => {
    expect(Platform.OS).not.toBe("web");
  });

  it("is kept and read back on the next start", async () => {
    setThemePreference("highContrastDark");
    await Promise.resolve();
    expect(await AsyncStorage.getItem("hds.theme")).toBe("highContrastDark");
    resetThemeStore();
    await hydrateThemePreference();
    expect(getThemeState().preference).toBe("highContrastDark");
  });

  it("forgets the choice when set back to the device's", async () => {
    await AsyncStorage.setItem("hds.theme", "dark");
    setThemePreference("system");
    await Promise.resolve();
    expect(await AsyncStorage.getItem("hds.theme")).toBeNull();
  });

  it("lets a choice made before the read finishes win", async () => {
    await AsyncStorage.setItem("hds.theme", "dark");
    setThemePreference("light");
    await hydrateThemePreference();
    expect(getThemeState().preference).toBe("light");
  });

  it("ignores what it cannot read", async () => {
    await AsyncStorage.setItem("hds.theme", "constructor");
    await hydrateThemePreference();
    expect(getThemeState().preference).toBe("system");
    jest.spyOn(AsyncStorage, "getItem").mockRejectedValueOnce(new Error("storage unavailable"));
    await hydrateThemePreference();
    expect(getThemeState().preference).toBe("system");
  });
});

describe("searchable picker matching (FR-044)", () => {
  const items = [
    { id: "1", label: "Dana Levi", secondary: "Gate 3", keywords: ["דנה לוי"] },
    { id: "2", label: "Yosef Cohen", keywords: ["יוסף כהן"] },
  ];
  it("matches Hebrew and English names, without vowel points or case", () => {
    expect(normalise("דָּנָה")).toBe("דנה");
    expect(matches(items[0]!, "דנה")).toBe(true);
    expect(matches(items[0]!, "LEVI")).toBe(true);
    expect(matches(items[0]!, "gate")).toBe(true);
    expect(matches(items[1]!, "dana")).toBe(false);
  });
  it("puts recent items first", () => {
    expect(order(items, ["2"], "").map((item) => item.id)).toEqual(["2", "1"]);
    expect(order(items, ["2"], "dana").map((item) => item.id)).toEqual(["1"]);
  });
});

describe("text direction (owner, PR 4)", () => {
  it("follows the UI language, not the first letter, so a Latin start stays at the right edge in Hebrew", () => {
    expect(textDirection(undefined, true)).toBe("rtl");
    expect(textDirection(undefined, false)).toBe("ltr");
    expect(textDirection("ltr", true)).toBe("ltr");
    expect(textDirection("auto", true)).toBeUndefined();
  });
});
