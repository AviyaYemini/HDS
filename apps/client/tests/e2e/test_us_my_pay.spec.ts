/**
 * 005 US1 — My pay on the web build: an employee who worked a shift sees its hours, its pay and the
 * month's total, in at most three steps from opening the app (SC-001): sign in, My pay.
 */
import { expect, test } from "@playwright/test";

import { inEarlierMonth, workedShift } from "./pay_support";
import { signIn } from "./support";

let shiftId: string;
let startsAt: Date;

test.beforeAll(async () => {
  ({ shiftId, startsAt } = await workedShift());
});

test("an employee sees the pay for a shift they worked", async ({ page }) => {
  await signIn(page, "employee");
  await page.getByTestId("nav-pay").click();
  await expect(page).toHaveURL(/\/pay$/);
  // The site files a shift under its local start date; the screen opens at the device month.
  if (inEarlierMonth(startsAt)) await page.getByTestId("pay-prev").click();
  const line = page.getByTestId(`pay-line-${shiftId}`);
  await expect(line).toContainText("Night Watch");
  await expect(line).toContainText("8.00 h");
  await expect(line).toContainText(/Pay \d+\.\d{2}/);
  await expect(page.getByTestId("pay-total")).toContainText(/Total \d+\.\d{2}/);
});

test("in Hebrew the pay screen is right to left", async ({ page }) => {
  await signIn(page, "employee", "?lang=he");
  await page.getByTestId("nav-pay").click();
  await expect(page.locator("html")).toHaveAttribute("dir", "rtl");
  await expect(page.getByText("השכר שלי").first()).toBeVisible();
});
