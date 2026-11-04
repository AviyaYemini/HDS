/**
 * 003 US6 — The roster: an Admin creates a shift, assigns an employee and sees the rule warnings
 * without being stopped; a Manager publishes the drafts.
 *
 * SKIPPED pending the staff screens: written against the planned screens (Constitution IV, one
 * smoke test per journey); remove the skip when they are built.
 */
import { expect, test } from "@playwright/test";

import { signIn } from "./support";

test.skip(true, "pending the staff screens");

test("an Admin builds a shift and sees warnings but is not stopped", async ({ page }) => {
  await signIn(page, "admin");
  await page.getByTestId("nav-roster").click();
  await page.getByTestId("add-shift").click();
  await page.getByTestId("shift-project").selectOption({ label: "Night Watch" });
  await page.getByTestId("shift-type-night").click();
  await page.getByTestId("shift-date").fill("2030-08-05");
  await page.getByTestId("save-shift").click();
  await page.getByTestId("assign-employee").selectOption({ label: "Employee User" });
  await page.getByTestId("save-assignment").click();
  await expect(page.getByTestId("assignment-warnings")).toBeVisible();
  await expect(page.getByTestId("assignments")).toContainText("Employee User");
});

test("a Manager publishes the drafts of a week", async ({ page }) => {
  await signIn(page, "manager");
  await page.getByTestId("nav-roster").click();
  await page.getByTestId("publish-week").click();
  await expect(page.getByTestId("published-count")).toBeVisible();
});
