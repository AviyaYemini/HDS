/**
 * 004 US1 — Staffing needs: a Manager sets how many people a project needs per shift type and
 * weekday, and a type the project does not use is refused with the reason.
 *
 * SKIPPED pending the staff screens: written against the planned screens (Constitution IV, one
 * smoke test per journey); remove the skip when they are built.
 */
import { expect, test } from "@playwright/test";

import { signIn } from "./support";

test.skip(true, "pending the staff screens");

test("a Manager sets a project's staffing needs", async ({ page }) => {
  await signIn(page, "manager");
  await page.getByTestId("nav-projects").click();
  await page.getByText("Night Watch").click();
  await page.getByTestId("project-requirements").click();
  await page.getByTestId("need-night-all-week").fill("2");
  await page.getByTestId("save-requirements").click();
  await expect(page.getByTestId("project-requirements")).toContainText("2");
});
