/**
 * The sign-in return path (specs/007-ux-redesign contracts/ui-routes.md "Deep links"; the reviewer's
 * ruling): only a relative address of this app is kept; anything that could leave the app, smuggle
 * a header or point at an unknown route goes home.
 */
import { encodeReturnPath, safeReturnPath } from "../../src/auth/returnPath";

describe("safeReturnPath", () => {
  it.each([
    "/account?lang=he",
    "/my/shifts?view=past",
    "/roster?project=p1&week=2026-11-01",
    "/client/projects/abc",
    "/home",
  ])("keeps %s", (path) => {
    expect(safeReturnPath(path)).toBe(path);
  });

  it("decodes once and keeps a decoded app address", () => {
    expect(safeReturnPath(encodeURIComponent("/account?lang=he"))).toBe("/account?lang=he");
    expect(safeReturnPath(["/my/pay", "/evil"])).toBe("/my/pay");
  });

  it.each([
    ["//evil.test", "a second leading slash"],
    ["/\\evil.test", "a backslash"],
    ["/%5Cevil.test", "an encoded backslash"],
    ["%2F%2Fevil.test", "encoded double slash"],
    ["https://evil.test", "a scheme"],
    ["javascript:alert(1)", "a javascript scheme"],
    ["/\u0000x", "a control character"],
    ["/%0d%0aSet-Cookie:x", "an encoded line break"],
    ["", "nothing"],
    ["/nowhere/at/all", "an unknown route"],
    ["/homework", "a prefix that only looks known"],
    ["%E0%A4%A", "malformed encoding"],
    ["/account:8080", "a colon in the path"],
  ])("refuses %s (%s)", (path) => {
    expect(safeReturnPath(path)).toBeNull();
  });

  it("refuses non-strings", () => {
    expect(safeReturnPath(undefined)).toBeNull();
    expect(safeReturnPath(null)).toBeNull();
  });
});

describe("encodeReturnPath", () => {
  it("carries the path with its query, encoded once", () => {
    expect(encodeReturnPath("/account", { lang: "he" })).toBe(encodeURIComponent("/account?lang=he"));
    expect(encodeReturnPath("/home", {})).toBe("%2Fhome");
    expect(safeReturnPath(encodeReturnPath("/roster", { project: "p 1", week: "2026-11-01" }))).toBe("/roster?project=p+1&week=2026-11-01");
  });
});
