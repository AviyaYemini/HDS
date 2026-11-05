/**
 * 005 US6 — An Admin assigns a Manager to a project.
 *
 * SKIPPED pending the admin screens: written against the planned screens (Constitution IV, one smoke
 * test per journey); remove the skip when they are built. The API is covered by the API tests.
 */
import { expect, test } from "@playwright/test";

import { signIn } from "./support";

test.skip(true, "pending the admin screens");

test("an Admin assigns a Manager to a project", async ({ page }) => {
  await signIn(page, "admin");
  await page.getByTestId("nav-project-managers").click();
  await page.getByTestId("manager-option").first().click();
  await page.getByTestId("managers-save").click();
  await expect(page.getByTestId("project-managers")).not.toBeEmpty();
});
