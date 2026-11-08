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
  // The report opens on this pay period; the shift may have been in the last one.
  if (inEarlierMonth(startsAt)) await page.getByTestId("preset-last-pay-period").click();
  await page.getByTestId("scope-project").click();
  await page.getByTestId("report-scope-picker").click();
  await page.getByTestId("report-scope-picker-sheet").getByText("Night Watch").first().click();
  await expect(page.getByTestId("figure-shifts")).toBeVisible();
  // Each project is a bar that opens into who worked there.
  const bar = page.getByTestId("report-bar").filter({ hasText: "Night Watch" }).first();
  await bar.click();
  await expect(page.getByTestId("report-rows").first().getByTestId("report-row").first()).toBeVisible();
});

test("an employee has no reports in the bar", async ({ page }) => {
  await signIn(page, "employee");
  await expect(page.getByTestId("nav-pay")).toBeVisible();
  await expect(page.getByTestId("nav-work-log")).toHaveCount(0);
});
