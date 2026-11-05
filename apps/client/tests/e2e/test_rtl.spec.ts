/**
 * RTL smoke test (Constitution VIII): in Hebrew the document is right to left and the screen mirrors,
 * so the sign-in button starts at the right edge; in English it starts at the left.
 */
import { expect, test, type Page } from "@playwright/test";

async function buttonSide(page: Page): Promise<"left" | "right"> {
  const root = await page.getByTestId("screen-root").boundingBox();
  const button = await page.getByTestId("sign-in-btn").boundingBox();
  expect(root && button).toBeTruthy();
  const middle = root!.x + root!.width / 2;
  return button!.x + button!.width / 2 > middle ? "right" : "left";
}

test("Hebrew lays the sign-in screen out right to left", async ({ page }) => {
  await page.goto("/sign-in?lang=he");
  await expect(page.locator("html")).toHaveAttribute("dir", "rtl");
  await expect(page.getByTestId("sign-in-btn")).toContainText("כניסה");
  const direction = await page.getByTestId("screen-root").evaluate((node) => getComputedStyle(node).direction);
  expect(direction).toBe("rtl");
  expect(await buttonSide(page)).toBe("right");
});

test("English lays it out left to right", async ({ page }) => {
  await page.goto("/sign-in");
  await expect(page.locator("html")).toHaveAttribute("dir", "ltr");
  expect(await buttonSide(page)).toBe("left");
});
