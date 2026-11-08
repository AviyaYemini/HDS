/**
 * 005 US2 and US3 — An Admin sets a default rate, a project's pay settings and a special day.
 *
 * SKIPPED: 007's admin screens edit the organisation's pay settings and special days, not a
 * project's own yet; remove the skip when those are built. The API is covered by the API tests.
 */
import { expect, test } from "@playwright/test";

import { signIn } from "./support";

test.skip(true, "a project's own pay settings and special days are not in 007's admin screens (the organisation's are)");

test("an Admin opens a project's pay to its Managers and adds a special day", async ({ page }) => {
  await signIn(page, "admin");
  await page.getByTestId("nav-pay-settings").click();
  await page.getByTestId("project-pay-managers-see-pay").click();
  await page.getByTestId("special-day-date").fill("2030-09-11");
  await page.getByTestId("special-day-name").fill("Yom Kippur");
  await page.getByTestId("special-day-add").click();
  await expect(page.getByTestId("special-days")).toContainText("Yom Kippur");
});
