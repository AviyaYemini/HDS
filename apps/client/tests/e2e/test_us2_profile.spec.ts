/**
 * US2 — Employee Self-Service Profile: Playwright smoke tests.
 *
 * NOTE: SKIPPED pending apps/client implementation.
 */
import { test, expect } from "@playwright/test";

const BASE = process.env.APP_URL ?? "http://localhost:3000";

async function signIn(page: any, email: string, password = "Dev1234!") {
  await page.goto(`${BASE}/sign-in`);
  await page.fill('[data-testid="email"]', email);
  await page.fill('[data-testid="password"]', password);
  await page.click('[data-testid="sign-in-btn"]');
}

test.skip("Employee can open My Profile and edit phone", async ({ page }) => {
  await signIn(page, "employee@hds.test");
  await page.click('[data-testid="nav-my-profile"]');
  await expect(page).toHaveURL(/\/me/);
  await page.fill('[data-testid="phone-input"]', "+972509999999");
  await page.click('[data-testid="save-btn"]');
  await expect(page.locator('[data-testid="phone-input"]')).toHaveValue("+972509999999");
  // Reload to confirm persisted
  await page.reload();
  await expect(page.locator('[data-testid="phone-input"]')).toHaveValue("+972509999999");
});

test.skip("Employee cannot navigate to another employee profile", async ({ page }) => {
  await signIn(page, "employee@hds.test");
  await page.goto(`${BASE}/employees/00000000-0000-0000-0000-000000000001`);
  // Should redirect or show 403 page
  await expect(page).not.toHaveURL(/\/employees\//);
});
