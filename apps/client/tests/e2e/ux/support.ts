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

/** Run axe-core on the page (WCAG 2.2 A and AA rules) and fail on serious or critical issues. */
export async function checkA11y(page: Page, context: string): Promise<void> {
  const results = await new AxeBuilder({ page })
    .withTags(["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa"])
    .analyze();
  const blocking = results.violations.filter((v) => v.impact === "serious" || v.impact === "critical");
  expect(
    blocking.map((v) => `${v.id}: ${v.help} (${v.nodes.length} node(s))`),
    `axe on ${context}`,
  ).toEqual([]);
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
