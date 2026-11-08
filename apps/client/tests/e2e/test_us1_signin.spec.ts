/**
 * US1 — Sign In: signing out revokes the session on the server, and the locked-account message in
 * English and Hebrew.
 *
 * Each role's destinations and the client portal moved to `ux/b-roles.spec.ts` (007 T033), which
 * checks them for all four roles, both languages and both screen sizes.
 */
import { expect, request, test } from "@playwright/test";

import { API } from "./support";

const BASE = process.env.APP_URL ?? "http://localhost:3000";

// Lock locked@hds.test (no such account: the lock is the same either way, FR-221) with five
// failed sign-ins through the API, so the specs below see the lock on their own attempt.
test.beforeAll(async () => {
  const api = await request.newContext({ baseURL: API });
  for (let i = 0; i < 5; i++) {
    await api.post("/auth/login", { data: { email: "locked@hds.test", password: "wrong" } });
  }
  await api.dispose();
});

test("Sign out invalidates session", async ({ page }) => {
  await page.goto(`${BASE}/sign-in`);
  await page.fill('[data-testid="email"]', "employee@hds.test");
  await page.fill('[data-testid="password"]', "Dev1234!");
  await page.click('[data-testid="sign-in-btn"]');
  const logout = page.waitForRequest((r) => r.url().endsWith("/auth/logout"));
  // The sign-out every screen carries (007 contracts/ui-routes.md).
  await page.getByTestId("action-sign-out").first().click();
  await expect(page).toHaveURL(/\/sign-in/);
  // The refresh token the page sent is revoked on the server, not just forgotten in the page.
  const { refresh_token } = (await logout).postDataJSON() as { refresh_token: string };
  const api = await request.newContext({ baseURL: API });
  const refreshed = await api.post("/auth/refresh", { data: { refresh_token } });
  expect(refreshed.status()).toBe(401);
  await api.dispose();
  // A signed-in address now sends the visitor back to sign-in.
  await page.goto(`${BASE}/account`);
  await expect(page).toHaveURL(/\/sign-in/);
});

test("Locked account shows locked message (LTR)", async ({ page }) => {
  await page.goto(`${BASE}/sign-in`);
  await page.fill('[data-testid="email"]', "locked@hds.test");
  await page.fill('[data-testid="password"]', "wrong");
  await page.click('[data-testid="sign-in-btn"]');
  await expect(page.locator('[data-testid="error-message"]')).toContainText(
    "locked"
  );
});

test("Locked account shows locked message (RTL — Hebrew)", async ({ page }) => {
  await page.goto(`${BASE}/sign-in?lang=he`);
  await page.fill('[data-testid="email"]', "locked@hds.test");
  await page.fill('[data-testid="password"]', "wrong");
  await page.click('[data-testid="sign-in-btn"]');
  await expect(page.locator('[data-testid="error-message"]')).toContainText(
    "ננעל"
  );
});
