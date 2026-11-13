/** 005 US4 — Who worked at a project: a Manager picks Night Watch and sees the employee's shift (007's work log). */
import { expect, test } from "@playwright/test";

import { inEarlierMonth, workedShift } from "./pay_support";
import { signIn } from "./support";

let startsAt: Date;

test.beforeAll(async () => {
  ({ startsAt } = await workedShift());
});

test("a Manager sees who worked at a project", async ({ page }) => {
  await signIn(page, "manager");
  await page.getByTestId("nav-work-log").click();
  await expect(page).toHaveURL(/\/reports\/work-log$/);
  // The log opens on this month; the shift may have been in the last one.
  if (inEarlierMonth(startsAt)) await page.getByTestId("preset-last-month").click();
  await page.getByTestId("scope-project").click();
  await page.getByTestId("report-scope-picker").click();
  await page.getByTestId("report-scope-picker-sheet").getByText("Night Watch").first().click();
  await expect(page.getByTestId("figure-shifts")).toBeVisible();
  // Who worked there, in the table under the figures (the owner, 2026-11-13).
  await expect(page.getByTestId("wl-table")).toContainText("Night Watch");
});

test("an employee has no reports in the bar", async ({ page }) => {
  await signIn(page, "employee");
  await expect(page.getByTestId("nav-pay")).toBeVisible();
  await expect(page.getByTestId("nav-work-log")).toHaveCount(0);
});
