/** 005 US5 — Hours report: an Admin sees hours and pay for the month, by employee and by project. */
import { expect, test } from "@playwright/test";

import { inEarlierMonth, workedShift } from "./pay_support";
import { signIn } from "./support";

let startsAt: Date;

test.beforeAll(async () => {
  ({ startsAt } = await workedShift());
});

test("an Admin sees the month's hours with pay, both ways", async ({ page }) => {
  await signIn(page, "admin");
  await page.getByTestId("nav-hours").click();
  await expect(page).toHaveURL(/\/reports\/hours$/);
  // The site files a shift under its local start date; the screen opens at the device month.
  if (inEarlierMonth(startsAt)) await page.getByTestId("hours-prev").click();
  await expect(page.getByTestId("hours-total")).toContainText(/Total \d+\.\d{2}/);
  await page.getByTestId("group-by-project").click();
  await expect(page.getByText("Night Watch").first()).toBeVisible();
});
