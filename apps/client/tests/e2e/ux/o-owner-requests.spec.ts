/**
 * Group O — the owner's requests of 2026-11-08 (specs/007-ux-redesign FR-078 to FR-081, T104).
 *
 * The language switch restarts the app in the new language: at once a "Switching to …" state in
 * that language, then the same screen, address and query, laid out in its direction, still signed
 * in; never the home screen. From the top bar, from Account and before signing in.
 */
import { expect, test, type Page } from "@playwright/test";

import { type Language, openAs, at } from "./support";

const SEEN = "e2e.switching-seen";

/**
 * Record the switching state's text in this tab's storage the moment it shows: the page reloads a
 * few frames later, faster than an assertion could look, and the record survives the reload.
 */
async function watchSwitching(page: Page): Promise<void> {
  await page.addInitScript((key) => {
    const look = () => {
      const node = document.querySelector('[data-testid="language-switching"]');
      if (node && !sessionStorage.getItem(key)) {
        sessionStorage.setItem(key, `${node.getAttribute("lang")}|${node.textContent ?? ""}`);
      }
    };
    new MutationObserver(look).observe(document, { childList: true, subtree: true });
  }, SEEN);
}

async function seenSwitching(page: Page): Promise<string | null> {
  return page.evaluate((key) => sessionStorage.getItem(key), SEEN);
}

const OTHER: Record<Language, Language> = { he: "en", en: "he" };
const SWITCHING: Record<Language, RegExp> = { en: /Switching to English/, he: /עוברים לעברית/ };

async function landedIn(page: Page, language: Language, path: string, query: Record<string, string>): Promise<void> {
  await expect(page.locator("html")).toHaveAttribute("dir", language === "he" ? "rtl" : "ltr");
  await expect(page.locator("html")).toHaveAttribute("lang", language);
  const url = new URL(page.url());
  expect(url.pathname).toBe(path);
  for (const [key, value] of Object.entries(query)) expect(url.searchParams.get(key)).toBe(value);
  expect(url.searchParams.has("lang")).toBe(false);
  await expect(page.getByTestId("language-switching")).toHaveCount(0);
}

for (const from of ["he", "en"] as const) {
  const to = OTHER[from];

  test(`${from} to ${to} from the top bar, on a deep address, back on it signed in`, async ({ page }) => {
    await watchSwitching(page);
    await openAs(page, "employee", from, "/my/shifts", { view: "past" });
    await expect(page.getByTestId("screen-my-shifts")).toBeVisible();
    await page.getByTestId("action-switch-language").first().click();

    await expect(page.getByTestId("screen-my-shifts")).toBeVisible();
    await landedIn(page, to, "/my/shifts", { view: "past" });
    const seen = await seenSwitching(page);
    expect(seen, "the switching state showed").not.toBeNull();
    expect(seen!).toMatch(new RegExp(`^${to}\\|`));
    expect(seen!).toMatch(SWITCHING[to]);
  });

  test(`${from} to ${to} from Account, back on Account signed in`, async ({ page }) => {
    await watchSwitching(page);
    await openAs(page, "manager", from, "/account");
    await expect(page.getByTestId("screen-account")).toBeVisible();
    await page.getByTestId(`language-${to}`).click();

    await expect(page.getByTestId("screen-account")).toBeVisible();
    await landedIn(page, to, "/account", {});
    expect(await seenSwitching(page)).toMatch(SWITCHING[to]);
  });

  test(`${from} to ${to} before signing in, still on sign-in`, async ({ page }) => {
    await watchSwitching(page);
    await page.goto(at("/sign-in", from));
    await expect(page.getByTestId("screen-sign-in")).toBeVisible();
    await page.getByTestId("action-switch-language").first().click();

    await expect(page.getByTestId("screen-sign-in")).toBeVisible();
    await landedIn(page, to, "/sign-in", {});
    expect(await seenSwitching(page)).toMatch(SWITCHING[to]);
  });
}
