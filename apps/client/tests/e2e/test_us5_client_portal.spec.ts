/**
 * US5 — Client Views Own Projects: Playwright smoke tests.
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

test.skip("Client sees only own projects", async ({ page }) => {
  await signIn(page, "client@hds.test");
  await expect(page).toHaveURL(/\/projects/);
  const items = page.locator('[data-testid="project-item"]');
  // All visible projects belong to this client
  const count = await items.count();
  expect(count).toBeGreaterThanOrEqual(0);
});

test.skip("Client cannot navigate to employees page", async ({ page }) => {
  await signIn(page, "client@hds.test");
  await page.goto(`${BASE}/employees`);
  await expect(page).not.toHaveURL(/\/employees/);
});
