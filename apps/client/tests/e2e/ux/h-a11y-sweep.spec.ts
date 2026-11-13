/**
 * Group H — the accessibility sweep (specs/007-ux-redesign, task T090).
 *
 * Every screen each role can reach, in Hebrew and English, light and dark, at desktop and phone
 * sizes: axe finds no serious or critical issue, and keyboard focus is never hidden behind a sticky
 * header, tab bar or notice (WCAG 2.4.11). Covers FR-061 to FR-065. The routes are the ones in
 * `specs/007-ux-redesign/contracts/ui-routes.md`.
 */
import { expect, test, type Page } from "@playwright/test";

import { LANGUAGES, type Role, apiAs, at, checkA11y, ok, signIn } from "./support";

const SHARED = ["/account", "/help", "/accessibility", "/contact"];
const MY_WORK = ["/my/availability", "/my/shifts", "/my/shifts?view=past", "/my/pay"];
const MANAGE = ["/manage/overview", "/roster", "/roster?all=1", "/roster/submissions", "/runs", "/reports/work-log", "/reports/hours"];
const ADMIN = ["/admin/staff", "/admin/clients", "/admin/sites", "/admin/projects", "/admin/shift-templates", "/admin/staffing-needs", "/admin/pay-settings"];

/** Every route of contracts/ui-routes.md each role can reach (group H review: all of them). */
const ROUTES: Record<Role, string[]> = {
  employee: ["/home", ...MY_WORK, ...SHARED],
  manager: ["/home", ...MANAGE, ...ADMIN, ...MY_WORK, ...SHARED],
  admin: ["/home", ...MANAGE, ...ADMIN, "/admin/special-days", ...SHARED],
  client: ["/client", "/client/contact", ...SHARED],
};

/** A record's own page for the role, when one exists: a run, a shift (dynamic routes). */
async function recordRoutes(role: Role): Promise<string[]> {
  const api = await apiAs(role);
  const found: string[] = [];
  try {
    if (role === "manager" || role === "admin") {
      const runs = await ok<{ items: { id: string }[] }>(api.get("/api/v1/schedule-jobs"));
      if (runs.items[0]) found.push(`/runs/${runs.items[0].id}`);
    }
    if (role !== "client") {
      const shifts = await ok<{ items: { id: string }[] }>(api.get("/me/shifts"));
      if (shifts.items[0]) found.push(`/my/shifts/${shifts.items[0].id}`);
    }
  } finally {
    await api.dispose();
  }
  return found;
}

/** Tab through the first focusable elements and fail if one is covered where it is drawn. */
async function focusNeverHidden(page: Page, steps = 25): Promise<void> {
  for (let i = 0; i < steps; i += 1) {
    await page.keyboard.press("Tab");
    const hidden = await page.evaluate(() => {
      const el = document.activeElement as HTMLElement | null;
      if (!el || el === document.body) return null;
      const box = el.getBoundingClientRect();
      if (box.width === 0 || box.height === 0) return null;
      const x = Math.min(Math.max(box.left + box.width / 2, 0), window.innerWidth - 1);
      const y = Math.min(Math.max(box.top + box.height / 2, 0), window.innerHeight - 1);
      const top = document.elementFromPoint(x, y);
      return top && (top === el || el.contains(top) || top.contains(el)) ? null : el.getAttribute("data-testid") ?? el.tagName;
    });
    expect(hidden, "focused element covered by another").toBeNull();
  }
}

for (const language of LANGUAGES) {
  // All four themes, the two high-contrast ones included (FR-061).
  for (const theme of ["light", "dark", "high-contrast-light", "high-contrast-dark"] as const) {
    for (const role of Object.keys(ROUTES) as Role[]) {
      test(`${role}, ${language}, ${theme}: every screen passes axe and keeps focus visible`, async ({ page }) => {
        test.setTimeout(240_000);
        await signIn(page, role, language);
        for (const path of [...ROUTES[role], ...(await recordRoutes(role))]) {
          const [pathname, query] = path.split("?");
          await page.goto(at(pathname!, language, { theme, ...Object.fromEntries(new URLSearchParams(query ?? "")) }));
          // The screen itself, never the no-access or sign-in page in its place (group H review).
          await expect(page, `${path} stays where it was asked`).toHaveURL((url) => url.pathname === pathname);
          await expect(page.locator('[data-testid^="screen-"]').first()).toBeVisible();
          await expect(page.getByTestId("screen-no-access")).toHaveCount(0);
          await checkA11y(page, `${role} ${path} (${language}, ${theme})`);
          await focusNeverHidden(page);
        }
      });
    }
  }
}
