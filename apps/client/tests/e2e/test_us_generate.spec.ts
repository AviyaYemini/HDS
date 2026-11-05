/**
 * 004 US2 to US4 — Generating a roster: a Manager asks for next week, sees the proposal with its
 * empty places and their reasons, and accepts it as drafts.
 *
 * SKIPPED pending the staff screens: written against the planned screens (Constitution IV, one
 * smoke test per journey); remove the skip when they are built.
 */
import { expect, test } from "@playwright/test";

import { signIn } from "./support";

test.skip(true, "pending the staff screens");

test("a Manager generates a week and accepts it as drafts", async ({ page }) => {
  await signIn(page, "manager");
  await page.getByTestId("nav-generate").click();
  await page.getByTestId("generate-first-day").fill("2030-08-04");
  await page.getByTestId("generate-last-day").fill("2030-08-10");
  await page.getByTestId("generate").click();
  await expect(page.getByTestId("proposal")).toBeVisible({ timeout: 60_000 });
  await expect(page.getByTestId("proposal-optimal")).toBeVisible();
  await page.getByTestId("accept-proposal").click();
  await expect(page.getByTestId("proposal-state")).toContainText("Accepted");
});

test("an empty place shows why it could not be filled", async ({ page }) => {
  await signIn(page, "manager");
  await page.getByTestId("nav-generate").click();
  await page.getByTestId("generate").click();
  await expect(page.getByTestId("unfilled").first()).toContainText(/blocked|rest|weekly|shift/i);
});
