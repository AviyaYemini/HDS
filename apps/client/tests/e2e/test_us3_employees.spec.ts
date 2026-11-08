/**
 * US3 — Staff on 007's admin screens (T082, T083): a Manager adds a staff member, and an Admin
 * deletes one. Deactivating from the staff panel is not part of 007's screens yet, so that journey
 * waits for it (the API is covered by the API tests).
 */
import { expect, test } from "@playwright/test";

import { signIn } from "./support";

test("a Manager adds a staff member, who is listed", async ({ page }) => {
  const email = `new.${Date.now()}@hds.test`;
  await signIn(page, "manager");
  await expect(page.getByTestId("nav-tabs").or(page.getByTestId("nav-side")).first()).toBeVisible();
  await page.goto("/admin/staff?open=new");
  await page.getByTestId("field-full_name").fill("New Person");
  await page.getByTestId("field-email").fill(email);
  await page.getByTestId("field-initial_password").fill("Welcome1234!");
  await page.getByTestId("action-save").click();
  await page.getByTestId("table-search").fill(email);
  await expect(page.getByTestId("table-row").filter({ hasText: email })).toBeVisible();
});

test("an Admin deletes a staff member after confirming", async ({ page }) => {
  const email = `leaving.${Date.now()}@hds.test`;
  await signIn(page, "admin");
  await expect(page.getByTestId("nav-tabs").or(page.getByTestId("nav-side")).first()).toBeVisible();
  await page.goto("/admin/staff?open=new");
  await page.getByTestId("field-full_name").fill("Leaving Person");
  await page.getByTestId("field-email").fill(email);
  await page.getByTestId("field-initial_password").fill("Welcome1234!");
  await page.getByTestId("action-save").click();
  await expect(page.getByTestId("action-delete")).toBeVisible();
  await page.getByTestId("action-delete").click();
  await page.getByTestId("action-confirm-delete").click();
  await page.getByTestId("table-search").fill(email);
  await expect(page.getByTestId("table-row").filter({ hasText: email })).toHaveCount(0);
});

test.skip("a Manager deactivates a staff member, who can no longer sign in", async () => {
  // Waits for a deactivate action in the staff panel (not in 007's screens).
});
