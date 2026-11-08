/**
 * Group H — the accessibility sweep (specs/007-ux-redesign, task T090).
 *
 * Every screen each role can reach, in Hebrew and English, light and dark, at desktop and phone
 * sizes: axe finds no serious or critical issue, and keyboard focus is never hidden behind a sticky
 * header, tab bar or notice (WCAG 2.4.11). Covers FR-061 to FR-065. The routes are the ones in
 * `specs/007-ux-redesign/contracts/ui-routes.md`.
 */
import { expect, test, type Page } from "@playwright/test";

import { LANGUAGES, type Role, at, checkA11y, signIn } from "./support";

const ROUTES: Record<Role, string[]> = {
  employee: ["/home", "/my/availability", "/my/shifts", "/my/shifts?view=past", "/my/pay", "/account", "/help", "/accessibility"],
  manager: ["/home", "/roster", "/roster/submissions", "/runs", "/reports/work-log", "/reports/hours", "/admin/staff", "/admin/clients", "/admin/sites", "/admin/projects", "/admin/shift-templates", "/admin/staffing-needs", "/admin/pay-settings", "/account"],
  admin: ["/home", "/admin/special-days", "/admin/pay-settings", "/account"],
  client: ["/client", "/client/contact", "/account", "/help", "/accessibility"],
};

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
  for (const theme of ["light", "dark"] as const) {
    for (const role of Object.keys(ROUTES) as Role[]) {
      test(`${role}, ${language}, ${theme}: every screen passes axe and keeps focus visible`, async ({ page }) => {
        await signIn(page, role, language);
        for (const path of ROUTES[role]) {
          await page.goto(at(path, language, { theme }));
          await expect(page.locator("main, [role='main']").first()).toBeVisible();
          await checkA11y(page, `${role} ${path} (${language}, ${theme})`);
          await focusNeverHidden(page);
        }
      });
    }
  }
}
