/**
 * US2 — My profile: an employee changes their own phone on the Account screen and it is kept
 * (001 FR-017; 007 T039, contracts/ui-routes.md "Account").
 *
 * Reaching another employee's profile is refused by the API and drawn as the no-access page, which
 * `ux/b-roles.spec.ts` checks for every role (007 T033).
 */
import { expect, test } from "@playwright/test";

const BASE = process.env.APP_URL ?? "http://localhost:3000";

test("an employee changes their phone on the Account screen and it is kept", async ({ page }) => {
  await page.goto(`${BASE}/sign-in`);
  await page.fill('[data-testid="email"]', "employee@hds.test");
  await page.fill('[data-testid="password"]', "Dev1234!");
  await page.click('[data-testid="sign-in-btn"]');
  await page.goto(`${BASE}/account`);
  await expect(page.getByTestId("screen-account")).toBeVisible();
  // A new number each run, so the check never passes on what an earlier run left.
  const phone = `+97250${String(Date.now()).slice(-7)}`;
  await page.getByTestId("profile-phone").fill(phone);
  await page.getByTestId("action-save-profile").click();
  await expect(page.getByTestId("profile-phone")).toHaveValue(phone);
  await page.reload();
  await expect(page.getByTestId("profile-phone")).toHaveValue(phone);
});
