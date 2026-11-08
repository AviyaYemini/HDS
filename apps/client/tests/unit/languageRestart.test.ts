/**
 * The language switch's restart (specs/007-ux-redesign FR-078, T104): the screen is kept and taken
 * back once, the choice is stored before anything restarts, and a phone that cannot reload falls
 * back to turning the language now.
 */
import AsyncStorage from "@react-native-async-storage/async-storage";

import { i18n, isRtl, setupI18n } from "../../src/i18n";
import {
  RESUME_FRESH_MS,
  UNLOCK_SKIP_MS,
  clearResume,
  isAppPath,
  resetSwitching,
  restartInLanguage,
  resumePath,
  saveResume,
  sessionOf,
  skipsUnlock,
  takeResume,
} from "../../src/i18n/restart";

/** An access token whose payload names `claims` (the signature is never read here). */
function token(claims: Record<string, string>): string {
  const part = Buffer.from(JSON.stringify(claims)).toString("base64url");
  return `header.${part}.signature`;
}

const NOW = 1_800_000_000_000;

beforeEach(async () => {
  await AsyncStorage.clear();
  resetSwitching();
  await setupI18n("en");
});

test("the screen's address keeps its query but not the language", () => {
  expect(resumePath("/my/shifts", { view: "past", lang: "he" })).toBe("/my/shifts?view=past");
  expect(resumePath("/roster", { project: "p1", week: "2026-11-08" })).toBe("/roster?project=p1&week=2026-11-08");
  expect(resumePath("/account", {})).toBe("/account");
});

test("signing out forgets a pending resume", async () => {
  await saveResume("/account", "s1", NOW);
  await clearResume();
  expect(await takeResume(NOW)).toBeNull();
});

test("only the session that switched skips the second unlock", () => {
  const resume = { path: "/account", at: NOW, owner: "s1" };
  expect(sessionOf(token({ sub: "u1", sid: "s1" }))).toBe("s1");
  expect(sessionOf(token({ sub: "u1" }))).toBe("u1");
  expect(sessionOf("not-a-token")).toBeNull();
  expect(skipsUnlock(resume, token({ sub: "u1", sid: "s1" }), NOW + 2000)).toBe(true);
  // A restart takes seconds: an app killed mid-switch and opened later asks again.
  expect(skipsUnlock(resume, token({ sub: "u1", sid: "s1" }), NOW + UNLOCK_SKIP_MS + 1)).toBe(false);
  // Another session kept on the device (signed out and in again, or someone else): asked again.
  expect(skipsUnlock(resume, token({ sub: "u1", sid: "s2" }))).toBe(false);
  expect(skipsUnlock({ ...resume, owner: null }, token({ sid: "s1" }))).toBe(false);
  expect(skipsUnlock(null, token({ sid: "s1" }))).toBe(false);
  expect(skipsUnlock(resume, null)).toBe(false);
});

test("only an address inside the app is kept", () => {
  expect(isAppPath("/my/shifts?view=past")).toBe(true);
  expect(isAppPath("//evil.example/x")).toBe(false);
  expect(isAppPath("https://evil.example")).toBe(false);
  expect(isAppPath("/\\evil.example")).toBe(false);
});

test("a saved screen is taken back once, while fresh", async () => {
  await saveResume("/my/shifts?view=past", "s1", NOW);
  expect(await takeResume(NOW + 2000)).toEqual({ path: "/my/shifts?view=past", at: NOW, owner: "s1" });
  // Taken: a second start finds nothing.
  expect(await takeResume(NOW + 3000)).toBeNull();
});

test("a stale or foreign resume is ignored and removed", async () => {
  await saveResume("/account", "s1", NOW);
  expect(await takeResume(NOW + RESUME_FRESH_MS + 1)).toBeNull();
  await AsyncStorage.setItem("hds.resume", JSON.stringify({ path: "//evil.example", at: NOW }));
  expect(await takeResume(NOW)).toBeNull();
  expect(await AsyncStorage.getItem("hds.resume")).toBeNull();
});

test("the choice and the screen are stored before the app reloads", async () => {
  const seen: (string | null)[] = [];
  await restartInLanguage("he", "/account", {
    platform: "ios",
    reloadGraceMs: 0,
    reloadApp: async () => {
      seen.push(await AsyncStorage.getItem("hds.language"));
      seen.push(await AsyncStorage.getItem("hds.resume"));
    },
  });
  expect(seen[0]).toBe("he");
  expect(JSON.parse(seen[1]!).path).toBe("/account");
});

test("a phone reload that quietly does nothing falls back as well, never stuck on the switching state", async () => {
  const told = jest.fn();
  await restartInLanguage("he", "/account", { platform: "ios", reloadGraceMs: 0, reloadApp: async () => undefined, onRestartNeeded: told });
  expect(told).toHaveBeenCalledTimes(1);
  expect(i18n.language).toBe("he");
  // A slow reload may still come: the screen stays kept for it.
  expect(JSON.parse((await AsyncStorage.getItem("hds.resume"))!).path).toBe("/account");
});

test("a phone sets the new layout direction before it reloads", async () => {
  let rtlAtReload: boolean | null = null;
  await restartInLanguage("he", "/account", {
    platform: "ios",
    reloadGraceMs: 0,
    reloadApp: async () => {
      rtlAtReload = isRtl();
    },
  });
  expect(rtlAtReload).toBe(true);
});

test("without the native reload, the default path falls back too", async () => {
  const told = jest.fn();
  await restartInLanguage("he", "/account", { platform: "ios", onRestartNeeded: told });
  expect(told).toHaveBeenCalledTimes(1);
});

test("a phone that cannot reload turns the language now and says the layout follows", async () => {
  const told = jest.fn();
  await restartInLanguage("he", "/account", {
    platform: "ios",
    reloadApp: async () => {
      throw new Error("no reload");
    },
    onRestartNeeded: told,
  });
  expect(i18n.language).toBe("he");
  expect(told).toHaveBeenCalledTimes(1);
  // Nothing is left to resume at the next start.
  expect(await AsyncStorage.getItem("hds.resume")).toBeNull();
});
