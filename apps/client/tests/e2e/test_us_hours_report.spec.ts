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
  // The site files a shift under its local start date; the report opens on this pay period.
  if (inEarlierMonth(startsAt)) await page.getByTestId("preset-last-pay-period").click();
  await expect(page.getByTestId("figure-hours")).toHaveAttribute("data-value", /^\d+(\.\d+)?$/);
  await expect(page.getByTestId("figure-pay")).toBeVisible();
  // demo/ceo: hours and costs opens by project, with a table instead of bars that open.
  await page.getByTestId("group-project").click();
  await expect(page.getByTestId("hc-table")).toContainText("Night Watch");
});
