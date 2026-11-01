/**
 * US3 — Employee and Group Management: Playwright smoke tests.
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

test.skip("Manager can create an employee", async ({ page }) => {
  await signIn(page, "manager@hds.test");
  await page.click('[data-testid="nav-employees"]');
  await page.click('[data-testid="create-employee-btn"]');
  await page.fill('[data-testid="email"]', "teststaff@hds.test");
  await page.fill('[data-testid="full_name"]', "Test Staff");
  await page.fill('[data-testid="password"]', "Dev1234!");
  await page.click('[data-testid="submit-btn"]');
  await expect(page.locator('[data-testid="employee-list"]')).toContainText("teststaff@hds.test");
});

test.skip("Manager can deactivate an employee and they can no longer sign in", async ({ page }) => {
  await signIn(page, "manager@hds.test");
  await page.click('[data-testid="nav-employees"]');
  await page.click('[data-testid="employee-employee@hds.test"] [data-testid="deactivate-btn"]');
  await page.click('[data-testid="confirm-btn"]');

  // Now try to sign in as the deactivated employee
  await page.goto(`${BASE}/sign-in`);
  await page.fill('[data-testid="email"]', "employee@hds.test");
  await page.fill('[data-testid="password"]', "Dev1234!");
  await page.click('[data-testid="sign-in-btn"]');
  await expect(page.locator('[data-testid="error-message"]')).toBeVisible();
});

test.skip("Admin can delete a non-last employee", async ({ page }) => {
  await signIn(page, "admin@hds.test");
  await page.click('[data-testid="nav-employees"]');
  await page.click('[data-testid="employee-employee@hds.test"] [data-testid="delete-btn"]');
  await page.click('[data-testid="confirm-delete-btn"]');
  await expect(page.locator('[data-testid="employee-list"]')).not.toContainText("employee@hds.test");
});
