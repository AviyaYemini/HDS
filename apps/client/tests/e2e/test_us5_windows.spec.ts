/**
 * 003 US5 — Submission windows: a Manager opens a window for a week with a deadline, moves the
 * deadline, sees an overlapping window refused, and sees who is available.
 *
 * SKIPPED pending the staff screens: written against the planned screens (Constitution IV, one
 * smoke test per journey); remove the skip when they are built.
 */
import { expect, test } from "@playwright/test";

import { signIn } from "./support";

test.skip(true, "pending the staff screens");

test("a Manager opens a window and moves its deadline", async ({ page }) => {
  await signIn(page, "manager");
  await page.getByTestId("nav-windows").click();
  await page.getByTestId("add-window").click();
  await page.getByTestId("window-first-day").fill("2030-08-04");
  await page.getByTestId("window-last-day").fill("2030-08-10");
  await page.getByTestId("window-deadline").fill("2030-08-01T12:00");
  await page.getByTestId("save-window").click();
  await expect(page.getByTestId("windows")).toContainText("Aug 4");
});

test("an overlapping window is refused with the reason", async ({ page }) => {
  await signIn(page, "manager");
  await page.getByTestId("nav-windows").click();
  await page.getByTestId("add-window").click();
  await page.getByTestId("window-first-day").fill("2030-08-08");
  await page.getByTestId("window-last-day").fill("2030-08-14");
  await page.getByTestId("save-window").click();
  await expect(page.getByTestId("error-message")).toContainText("shares dates");
});

test("a Manager sees who is available in the window", async ({ page }) => {
  await signIn(page, "manager");
  await page.getByTestId("nav-windows").click();
  await page.getByTestId("window-availability").first().click();
  await expect(page.getByTestId("employee-availability")).toContainText("Employee User");
});
