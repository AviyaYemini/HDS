/**
 * US4 — Clients, sites and projects, on 007's admin screens (T082, T083): a Manager adds a client
 * and a project for it, a billing rate of zero is refused at the field, and a project is ended and
 * made active again.
 */
import { expect, test, type Page } from "@playwright/test";

import { signIn } from "./support";

async function openNew(page: Page, path: string): Promise<void> {
  await page.goto(`${path}?open=new`);
  await expect(page.getByTestId("record-form")).toBeVisible();
}

test("a Manager adds a client and a project for it", async ({ page }) => {
  const stamp = Date.now();
  await signIn(page, "manager");
  await expect(page.getByTestId("nav-tabs").or(page.getByTestId("nav-side")).first()).toBeVisible();
  await openNew(page, "/admin/clients");
  await page.getByTestId("field-name").fill(`Test Client ${stamp}`);
  await page.getByTestId("field-contract_start").fill("1.1.2026");
  await page.getByTestId("field-contract_start").blur();
  await page.getByTestId("action-save").click();
  await expect(page.getByTestId("table-row").filter({ hasText: `Test Client ${stamp}` })).toBeVisible();

  await openNew(page, "/admin/projects");
  await page.getByTestId("field-name").fill(`Test Project ${stamp}`);
  await page.getByTestId("field-start_date").fill("1.1.2026");
  await page.getByTestId("field-start_date").blur();
  await page.getByTestId("field-billing_rate").fill("75");
  await page.getByTestId("action-save").click();
  await expect(page.getByTestId("table-row").filter({ hasText: `Test Project ${stamp}` })).toBeVisible();
});

test("a billing rate of zero is refused at the field", async ({ page }) => {
  await signIn(page, "manager");
  await expect(page.getByTestId("nav-tabs").or(page.getByTestId("nav-side")).first()).toBeVisible();
  await openNew(page, "/admin/projects");
  await page.getByTestId("field-billing_rate").fill("0");
  await page.getByTestId("field-billing_rate").blur();
  await expect(page.getByTestId("field-error-billing_rate")).toBeVisible();
});

test("a project is ended and made active again", async ({ page }) => {
  await signIn(page, "admin");
  await expect(page.getByTestId("nav-tabs").or(page.getByTestId("nav-side")).first()).toBeVisible();
  await page.goto("/admin/projects?q=Day%20Patrol");
  await page.getByTestId("table-row").first().click();
  for (const [status, label] of [["ended", "Ended"], ["active", "Active"]] as const) {
    await page.getByTestId(`field-status-${status}`).click();
    await page.getByTestId("action-save").click();
    await expect(page.getByTestId("table-row").first()).toContainText(label);
  }
});
