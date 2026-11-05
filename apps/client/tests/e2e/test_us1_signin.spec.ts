/**
 * US1 — Sign In and Role-Based Access: Playwright smoke tests.
 *
 * Runnable since 003 Part 2: signing out, and the locked-account message in English and Hebrew.
 * Still skipped, each with its reason: the role dashboards and the client portal, which no screen
 * shows yet (they must pass before the screens that make them runnable merge, Constitution IV).
 */
import { expect, request, test } from "@playwright/test";

import { API } from "./support";

const BASE = process.env.APP_URL ?? "http://localhost:3000";
const PENDING_DASHBOARD = "pending the role dashboards, which no screen shows yet";

// Lock locked@hds.test (no such account: the lock is the same either way, FR-221) with five
// failed sign-ins through the API, so the specs below see the lock on their own attempt.
test.beforeAll(async () => {
  const api = await request.newContext({ baseURL: API });
  for (let i = 0; i < 5; i++) {
    await api.post("/auth/login", { data: { email: "locked@hds.test", password: "wrong" } });
  }
  await api.dispose();
});

test("Employee signs in and sees employee dashboard", async ({ page }) => {
  test.skip(true, PENDING_DASHBOARD);
  await page.goto(`${BASE}/sign-in`);
  await page.fill('[data-testid="email"]', "employee@hds.test");
  await page.fill('[data-testid="password"]', "Dev1234!");
  await page.click('[data-testid="sign-in-btn"]');
  await expect(page).toHaveURL(/\/dashboard/);
  await expect(page.locator('[data-testid="nav-employees"]')).not.toBeVisible();
});

test("Manager signs in and sees management nav", async ({ page }) => {
  test.skip(true, PENDING_DASHBOARD);
  await page.goto(`${BASE}/sign-in`);
  await page.fill('[data-testid="email"]', "manager@hds.test");
  await page.fill('[data-testid="password"]', "Dev1234!");
  await page.click('[data-testid="sign-in-btn"]');
  await expect(page).toHaveURL(/\/dashboard/);
  await expect(page.locator('[data-testid="nav-employees"]')).toBeVisible();
});

test("Admin signs in and sees admin nav", async ({ page }) => {
  test.skip(true, PENDING_DASHBOARD);
  await page.goto(`${BASE}/sign-in`);
  await page.fill('[data-testid="email"]', "admin@hds.test");
  await page.fill('[data-testid="password"]', "Dev1234!");
  await page.click('[data-testid="sign-in-btn"]');
  await expect(page).toHaveURL(/\/dashboard/);
  await expect(page.locator('[data-testid="nav-admin"]')).toBeVisible();
});

test("Client signs in and sees only own projects", async ({ page }) => {
  test.skip(true, PENDING_DASHBOARD);
  await page.goto(`${BASE}/sign-in`);
  await page.fill('[data-testid="email"]', "client@hds.test");
  await page.fill('[data-testid="password"]', "Dev1234!");
  await page.click('[data-testid="sign-in-btn"]');
  await expect(page).toHaveURL(/\/projects/);
  await expect(page.locator('[data-testid="nav-employees"]')).not.toBeVisible();
});

test("Sign out invalidates session", async ({ page }) => {
  await page.goto(`${BASE}/sign-in`);
  await page.fill('[data-testid="email"]', "employee@hds.test");
  await page.fill('[data-testid="password"]', "Dev1234!");
  await page.click('[data-testid="sign-in-btn"]');
  const logout = page.waitForRequest((r) => r.url().endsWith("/auth/logout"));
  await page.click('[data-testid="sign-out-btn"]');
  await expect(page).toHaveURL(/\/sign-in/);
  // The refresh token the page sent is revoked on the server, not just forgotten in the page.
  const { refresh_token } = (await logout).postDataJSON() as { refresh_token: string };
  const api = await request.newContext({ baseURL: API });
  const refreshed = await api.post("/auth/refresh", { data: { refresh_token } });
  expect(refreshed.status()).toBe(401);
  await api.dispose();
  // Navigating to protected route should redirect back to sign-in
  await page.goto(`${BASE}/me`);
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
