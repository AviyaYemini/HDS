/**
 * 003 US8 — Shift types: a Manager adds a type with local start and end times, retires it, and
 * chooses the types a project uses.
 *
 * SKIPPED pending the staff screens: written against the planned screens (Constitution IV, one
 * smoke test per journey); remove the skip when they are built.
 */
import { expect, test } from "@playwright/test";

import { signIn } from "./support";

test.skip(true, "pending the staff screens");

test("a Manager adds and retires a shift type", async ({ page }) => {
  await signIn(page, "manager");
  await page.getByTestId("nav-shift-types").click();
  await page.getByTestId("add-shift-type").click();
  await page.getByTestId("shift-type-name").fill("evening");
  await page.getByTestId("shift-type-start").fill("18:00");
  await page.getByTestId("shift-type-end").fill("02:00");
  await page.getByTestId("save-shift-type").click();
  await expect(page.getByTestId("shift-types")).toContainText("evening");
  await page.getByTestId("retire-evening").click();
  await expect(page.getByTestId("shift-types")).toContainText("evening");
});

test("a Manager chooses the types a project uses", async ({ page }) => {
  await signIn(page, "manager");
  await page.getByTestId("nav-projects").click();
  await page.getByText("Night Watch").click();
  await page.getByTestId("project-shift-types").click();
  await page.getByTestId("use-type-morning").check();
  await page.getByTestId("save-project-types").click();
  await expect(page.getByTestId("project-shift-types")).toContainText("morning");
});
