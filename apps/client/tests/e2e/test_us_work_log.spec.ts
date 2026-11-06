/** 005 US4 — Who worked at a project: a Manager picks Night Watch and sees the employee's shift. */
import { expect, test } from "@playwright/test";

import { workedShift } from "./pay_support";
import { signIn } from "./support";

test.beforeAll(async () => {
  await workedShift();
});

test("a Manager sees who worked at a project since it started", async ({ page }) => {
  await signIn(page, "manager");
  await page.getByTestId("nav-work-log").click();
  await expect(page).toHaveURL(/\/reports\/work-log$/);
  await page.getByText("Night Watch").first().click();
  await expect(page.getByTestId("log-range")).toBeVisible();
  await expect(page.locator('[data-testid^="log-row-"]').first()).toContainText("Night Watch");
});

test("an employee has no reports in the bar", async ({ page }) => {
  await signIn(page, "employee");
  await expect(page.getByTestId("nav-pay")).toBeVisible();
  await expect(page.getByTestId("nav-work-log")).toHaveCount(0);
});
