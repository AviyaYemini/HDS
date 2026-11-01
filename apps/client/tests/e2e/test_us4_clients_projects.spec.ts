/**
 * US4 — Clients, Sites, Projects: Playwright smoke tests.
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

test.skip("Manager can create a client, site, and project", async ({ page }) => {
  await signIn(page, "manager@hds.test");
  // Create client
  await page.click('[data-testid="nav-clients"]');
  await page.click('[data-testid="create-client-btn"]');
  await page.fill('[data-testid="client-name"]', "Test Client");
  await page.fill('[data-testid="contract-start"]', "2026-01-01");
  await page.click('[data-testid="submit-btn"]');
  // Create project
  await page.click('[data-testid="nav-projects"]');
  await page.click('[data-testid="create-project-btn"]');
  await page.fill('[data-testid="billing-rate"]', "75");
  await page.click('[data-testid="submit-btn"]');
  await expect(page.locator('[data-testid="project-list"]')).toContainText("Night Watch");
});

test.skip("Zero billing rate is rejected", async ({ page }) => {
  await signIn(page, "manager@hds.test");
  await page.click('[data-testid="nav-projects"]');
  await page.click('[data-testid="create-project-btn"]');
  await page.fill('[data-testid="billing-rate"]', "0");
  await page.click('[data-testid="submit-btn"]');
  await expect(page.locator('[data-testid="error-billing-rate"]')).toBeVisible();
});

test.skip("Project status can be toggled active/ended", async ({ page }) => {
  await signIn(page, "manager@hds.test");
  await page.click('[data-testid="nav-projects"]');
  await page.click('[data-testid="project-row-0"] [data-testid="end-project-btn"]');
  await expect(page.locator('[data-testid="project-row-0"] [data-testid="status"]')).toHaveText("Ended");
  await page.click('[data-testid="project-row-0"] [data-testid="reactivate-project-btn"]');
  await expect(page.locator('[data-testid="project-row-0"] [data-testid="status"]')).toHaveText("Active");
});
