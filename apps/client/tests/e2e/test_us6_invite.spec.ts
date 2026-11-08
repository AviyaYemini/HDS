/**
 * US6 — Password Reset and Invitation: Playwright smoke tests.
 *
 * NOTE: SKIPPED: invitations and the password reset are not part of 007's screens (staff are added
 * with a first password in the admin staff panel); the API is covered by the API tests.
 */
import { test, expect } from "@playwright/test";

const BASE = process.env.APP_URL ?? "http://localhost:3000";

test.skip("Manager can invite a new employee", async ({ page }) => {
  await page.goto(`${BASE}/sign-in`);
  await page.fill('[data-testid="email"]', "manager@hds.test");
  await page.fill('[data-testid="password"]', "Dev1234!");
  await page.click('[data-testid="sign-in-btn"]');
  await page.click('[data-testid="nav-employees"]');
  await page.click('[data-testid="invite-btn"]');
  await page.fill('[data-testid="invite-email"]', "newperson@hds.test");
  await page.click('[data-testid="send-invite-btn"]');
  await expect(page.locator('[data-testid="invite-success"]')).toBeVisible();
});

test.skip("User can reset their password", async ({ page }) => {
  await page.goto(`${BASE}/sign-in`);
  await page.click('[data-testid="forgot-password-link"]');
  await page.fill('[data-testid="reset-email"]', "employee@hds.test");
  await page.click('[data-testid="send-reset-btn"]');
  await expect(page.locator('[data-testid="reset-sent-msg"]')).toBeVisible();
});
