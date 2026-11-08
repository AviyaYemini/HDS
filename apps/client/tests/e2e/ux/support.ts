/**
 * Shared helpers for feature 007's specs (specs/007-ux-redesign, tasks T002).
 *
 * Every ux spec runs in the `ux-desktop` (1280 × 800) and `ux-phone` (390 × 844) projects and, through
 * {@link LANGUAGES}, in Hebrew and English. {@link checkA11y} runs axe-core on the current screen and
 * fails on serious and critical issues (FR-064, FR-065).
 */
import { AxeBuilder } from "@axe-core/playwright";
import { expect, request, type APIRequestContext, type APIResponse, type Page, type TestInfo } from "@playwright/test";

/** The two languages every screen is checked in. */
export const LANGUAGES = ["he", "en"] as const;
export type Language = (typeof LANGUAGES)[number];

/** The seeded accounts (`hds.scripts.seed_dev`), all with the dev password. */
export const USERS = {
  admin: "admin@hds.test",
  manager: "manager@hds.test",
  employee: "employee@hds.test",
  client: "client@hds.test",
} as const;
export type Role = keyof typeof USERS;
export const PASSWORD = "Dev1234!";

/** A fixed "now" for date-dependent checks: Saturday 7 November 2026, 10:00 in Asia/Jerusalem. */
export const NOW = new Date("2026-11-07T08:00:00Z");

/** Whether this run is the phone project. */
export function isPhone(testInfo: TestInfo): boolean {
  return testInfo.project.name === "ux-phone";
}

/** Add `lang` (and any other query values) to an app path. */
export function at(path: string, language: Language, query: Record<string, string> = {}): string {
  const params = new URLSearchParams({ lang: language, ...query });
  return `${path}${path.includes("?") ? "&" : "?"}${params.toString()}`;
}

/** Sign in through the sign-in screen as a seeded role, in the given language. */
export async function signIn(page: Page, role: Role, language: Language): Promise<void> {
  await page.goto(at("/sign-in", language));
  await page.getByTestId("email").fill(USERS[role]);
  await page.getByTestId("password").fill(PASSWORD);
  await page.getByTestId("sign-in-btn").click();
  await expect(page).not.toHaveURL(/\/sign-in/);
}

/**
 * Open `path` as `role`, as a bookmark or a shared link would. A tab that is not signed in yet shows
 * sign-in first and, once signed in, returns to that address (contracts/ui-routes.md, "Deep
 * links"); a tab already signed in opens it straight away, since the web keeps the session
 * per tab (ADR 019), so a test that changes role in one tab signs out first.
 */
export async function openAs(
  page: Page,
  role: Role,
  language: Language,
  path: string,
  query: Record<string, string> = {},
): Promise<void> {
  await page.goto(at(path, language, query));
  const signInButton = page.getByTestId("sign-in-btn");
  const landed = page.locator('[data-testid^="screen-"], [data-testid^="preview-"]').first();
  // The sign-in page is a screen too, so take whichever shows first.
  await expect(signInButton.or(landed).first(), `${path} shows sign-in or its screen`).toBeVisible();
  if (await signInButton.isVisible()) {
    await page.getByTestId("email").fill(USERS[role]);
    await page.getByTestId("password").fill(PASSWORD);
    await signInButton.click();
  }
  // The guard may answer a forbidden address with the no-access page (FR-007).
  await expect(page, `${path} after sign-in`).toHaveURL(
    (url) => url.pathname === path || url.pathname === "/no-access",
  );
}

/** Run axe-core on the page (WCAG 2.2 A and AA rules) and fail on serious or critical issues. */
export async function checkA11y(page: Page, context: string): Promise<void> {
  const results = await new AxeBuilder({ page })
    .withTags(["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa"])
    .analyze();
  const blocking = results.violations.filter((v) => v.impact === "serious" || v.impact === "critical");
  // Each finding names its elements (the first five selectors), so a failure says what to fix.
  const described = blocking.map((v) => {
    const targets = v.nodes.slice(0, 5).map((n) => n.target.join(" "));
    const more = v.nodes.length > 5 ? ` and ${v.nodes.length - 5} more` : "";
    return `${v.id}: ${v.help} (${v.nodes.length} node(s): ${targets.join(" | ")}${more})`;
  });
  expect(described, `axe on ${context}`).toEqual([]);
  // At phone width, every screen axe checks is also checked for content spilling out of its box.
  if ((page.viewportSize()?.width ?? 1280) <= PHONE_WIDTH) await expectNothingOverflows(page, context);
}

/** The phone viewport's width (playwright.config.ts, ux-phone). */
export const PHONE_WIDTH = 390;

/**
 * What spills out of its box (specs/007-ux-redesign FR-079, T102): for every element with a visible
 * border or a panel background, each visible element inside it whose box crosses the element's box
 * by more than 1 px. Content inside a scrolling area is left out (it is meant to move past the
 * area's edge), and so is anything fixed to the window or kept only for screen readers.
 */
export async function overflowing(page: Page): Promise<string[]> {
  return page.evaluate(() => {
    const TOLERANCE = 1;
    const name = (el: Element) => {
      const id = el.getAttribute("data-testid");
      const text = (el.textContent ?? "").trim().replace(/\s+/g, " ").slice(0, 40);
      return `${id ? `[data-testid="${id}"]` : el.tagName.toLowerCase()}${text ? ` "${text}"` : ""}`;
    };
    const transparent = (colour: string) => colour === "transparent" || /rgba\(.*,\s*0\)$/.test(colour);
    const isPanel = (el: Element, style: CSSStyleDeclaration) => {
      if (el === document.documentElement || el === document.body) return false;
      const bordered = ["Top", "Right", "Bottom", "Left"].some(
        (side) =>
          Number.parseFloat(style.getPropertyValue(`border-${side.toLowerCase()}-width`)) > 0 &&
          style.getPropertyValue(`border-${side.toLowerCase()}-style`) !== "none" &&
          !transparent(style.getPropertyValue(`border-${side.toLowerCase()}-color`)),
      );
      return bordered || !transparent(style.backgroundColor);
    };
    const scrolls = (style: CSSStyleDeclaration) => /(auto|scroll)/.test(style.overflowX + style.overflowY);
    const visible = (el: Element, style: CSSStyleDeclaration, box: DOMRect) =>
      box.width > 1 && box.height > 1 && style.visibility !== "hidden" && style.display !== "none" && Number(style.opacity) > 0;

    const found: string[] = [];
    for (const el of Array.from(document.body.querySelectorAll("*"))) {
      const style = getComputedStyle(el);
      const box = el.getBoundingClientRect();
      if (!visible(el, style, box) || style.position === "fixed") continue;
      // Walk up: each panel above must hold this box, until a scrolling area (it may move past it).
      for (let up = el.parentElement; up && up !== document.body; up = up.parentElement) {
        const upStyle = getComputedStyle(up);
        if (upStyle.position === "fixed") break;
        if (isPanel(up, upStyle)) {
          const outer = up.getBoundingClientRect();
          const out =
            box.left < outer.left - TOLERANCE ||
            box.right > outer.right + TOLERANCE ||
            box.top < outer.top - TOLERANCE ||
            box.bottom > outer.bottom + TOLERANCE;
          if (out && visible(up, upStyle, outer)) {
            found.push(`${name(el)} crosses ${name(up)}`);
            break;
          }
        }
        if (scrolls(upStyle)) break;
      }
      if (found.length >= 10) break;
    }
    return found;
  });
}

/** Fails, naming what spills, when anything crosses the box it sits in (FR-079). */
export async function expectNothingOverflows(page: Page, context: string): Promise<void> {
  expect(await overflowing(page), `overflow on ${context}`).toEqual([]);
}

/** A colour as the browser reports it, from `#RRGGBB`. */
export function rgb(hex: string): string {
  const n = Number.parseInt(hex.slice(1), 16);
  return `rgb(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255})`;
}

/** Elements under `root` that still animate or transition (FR-072: none under reduce-motion). */
export async function movingElements(page: Page, rootTestId: string): Promise<string[]> {
  return page.getByTestId(rootTestId).evaluate((root) => {
    const moving: string[] = [];
    const nonZero = (value: string) => value.split(",").some((part) => Number.parseFloat(part) > 0);
    for (const element of [root, ...Array.from(root.querySelectorAll("*"))]) {
      const style = getComputedStyle(element);
      const animating = style.animationName !== "none" && nonZero(style.animationDuration);
      if (nonZero(style.transitionDuration) || animating) {
        moving.push(element.getAttribute("data-testid") ?? element.tagName.toLowerCase());
      }
    }
    return moving;
  });
}

/** The API the specs set data up through: the host's API from inside the Playwright image. */
export const API_URL = process.env.E2E_API_URL ?? "http://localhost:8100";

/** Whether this run's build has the previews switched on (`PREVIEWS=0` for the previews-off run). */
export const PREVIEWS_ON = process.env.PREVIEWS !== "0";

/** Sign a seeded role in through the API and return its bearer header, for setting data up. */
export async function apiHeaders(request: APIRequestContext, role: Role): Promise<Record<string, string>> {
  const response = await request.post(`${API_URL}/auth/login`, {
    data: { email: USERS[role], password: PASSWORD },
  });
  expect(response.ok(), await response.text()).toBeTruthy();
  const { access_token } = (await response.json()) as { access_token: string };
  return { Authorization: `Bearer ${access_token}` };
}

/** An API request context signed in as a seeded role, for setting data up (`API_URL`). */
export async function apiAs(role: Role): Promise<APIRequestContext> {
  const anonymous = await request.newContext({ baseURL: API_URL });
  const headers = await apiHeaders(anonymous, role);
  await anonymous.dispose();
  return request.newContext({ baseURL: API_URL, extraHTTPHeaders: headers });
}

/** The JSON body of a call that must answer `status`. */
export async function ok<T>(answer: Promise<APIResponse>, status = 200): Promise<T> {
  const response = await answer;
  const text = await response.text();
  expect(response.status(), text).toBe(status);
  return (text ? JSON.parse(text) : undefined) as T;
}

/** The local date `days` from today at the seeded site (Asia/Jerusalem), as `YYYY-MM-DD`. */
export function siteDay(days = 0): string {
  return new Date(Date.now() + days * 86_400_000).toLocaleDateString("en-CA", { timeZone: "Asia/Jerusalem" });
}

/** The Sunday on or before a `YYYY-MM-DD` date (HDS weeks run Sunday to Saturday). */
export function sundayOf(day: string): string {
  const d = new Date(`${day}T12:00:00Z`);
  d.setUTCDate(d.getUTCDate() - d.getUTCDay());
  return d.toISOString().slice(0, 10);
}

/** `day` moved by `days`, as `YYYY-MM-DD`. */
export function addDays(day: string, days: number): string {
  const d = new Date(`${day}T12:00:00Z`);
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}
