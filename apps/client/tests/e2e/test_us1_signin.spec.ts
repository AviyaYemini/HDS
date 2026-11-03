/**
 * US1 — Sign In and Role-Based Access: Playwright smoke tests.
 *
 * NOTE: These tests are SKIPPED pending apps/client implementation.
 * Remove test.skip and implement selectors once the frontend is built.
 * These MUST pass before the apps/client feature branch merges (Constitution IV).
 */
import { test, expect } from "@playwright/test";

const BASE = process.env.APP_URL ?? "http://localhost:3000";

test.skip("Employee signs in and sees employee dashboard", async ({ page }) => {
  await page.goto(`${BASE}/sign-in`);
  await page.fill('[data-testid="email"]', "employee@hds.test");
  await page.fill('[data-testid="password"]', "Dev1234!");
  await page.click('[data-testid="sign-in-btn"]');
  await expect(page).toHaveURL(/\/dashboard/);
  await expect(page.locator('[data-testid="nav-employees"]')).not.toBeVisible();
});

test.skip("Manager signs in and sees management nav", async ({ page }) => {
  await page.goto(`${BASE}/sign-in`);
  await page.fill('[data-testid="email"]', "manager@hds.test");
  await page.fill('[data-testid="password"]', "Dev1234!");
  await page.click('[data-testid="sign-in-btn"]');
  await expect(page).toHaveURL(/\/dashboard/);
  await expect(page.locator('[data-testid="nav-employees"]')).toBeVisible();
});

test.skip("Admin signs in and sees admin nav", async ({ page }) => {
  await page.goto(`${BASE}/sign-in`);
  await page.fill('[data-testid="email"]', "admin@hds.test");
  await page.fill('[data-testid="password"]', "Dev1234!");
  await page.click('[data-testid="sign-in-btn"]');
  await expect(page).toHaveURL(/\/dashboard/);
  await expect(page.locator('[data-testid="nav-admin"]')).toBeVisible();
});

test.skip("Client signs in and sees only own projects", async ({ page }) => {
  await page.goto(`${BASE}/sign-in`);
  await page.fill('[data-testid="email"]', "client@hds.test");
  await page.fill('[data-testid="password"]', "Dev1234!");
  await page.click('[data-testid="sign-in-btn"]');
  await expect(page).toHaveURL(/\/projects/);
  await expect(page.locator('[data-testid="nav-employees"]')).not.toBeVisible();
});

test.skip("Sign out invalidates session", async ({ page }) => {
  await page.goto(`${BASE}/sign-in`);
  await page.fill('[data-testid="email"]', "employee@hds.test");
  await page.fill('[data-testid="password"]', "Dev1234!");
  await page.click('[data-testid="sign-in-btn"]');
  await page.click('[data-testid="sign-out-btn"]');
  await expect(page).toHaveURL(/\/sign-in/);
  // Navigating to protected route should redirect back to sign-in
  await page.goto(`${BASE}/me`);
  await expect(page).toHaveURL(/\/sign-in/);
});

test.skip("Locked account shows locked message (LTR)", async ({ page }) => {
  // Requires a locked address: seed a login_throttle row (kind address) with locked_until in the future
  await page.goto(`${BASE}/sign-in`);
  await page.fill('[data-testid="email"]', "locked@hds.test");
  await page.fill('[data-testid="password"]', "wrong");
  await page.click('[data-testid="sign-in-btn"]');
  await expect(page.locator('[data-testid="error-message"]')).toContainText(
    "locked"
  );
});

test.skip("Locked account shows locked message (RTL — Hebrew)", async ({ page }) => {
  await page.goto(`${BASE}/sign-in?lang=he`);
  await page.fill('[data-testid="email"]', "locked@hds.test");
  await page.fill('[data-testid="password"]', "wrong");
  await page.click('[data-testid="sign-in-btn"]');
  await expect(page.locator('[data-testid="error-message"]')).toContainText(
    "ננעל"
  );
});
