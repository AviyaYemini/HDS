/**
 * Group A — the design foundation, on the development gallery (specs/007-ux-redesign, task T006).
 *
 * Covers FR-051, FR-052, FR-055, FR-056, FR-057, FR-061, FR-064, FR-067, FR-072 and FR-074, in Hebrew
 * and English, at desktop and phone sizes. Written before the gallery exists, so it fails until the
 * [UI] tasks T014 to T023 build it.
 *
 * The contract with the gallery (`/dev/gallery`, previews on; `?theme=light|dark` picks a theme):
 *
 * | test id                         | what it is                                                       |
 * | ------------------------------- | ---------------------------------------------------------------- |
 * | `gallery-root`                  | the page; its background is the theme's canvas                   |
 * | `gallery-title`                 | a PageTitle with its eyebrow breadcrumb                          |
 * | `gallery-cta`                   | the screen's one primary action (`cta.bg`)                       |
 * | `gallery-link`                  | a text link (`accent`)                                           |
 * | `gallery-panel`                 | a FeaturePanel (`inverse.bg`)                                    |
 * | `gallery-row-leading` / `-trailing` | the first and last item of one Row                           |
 * | `gallery-arrow-next`            | a "next" arrow; `data-mirrored="true"` in Hebrew, else `"false"`  |
 * | `gallery-clock-icon`            | the clock icon; `data-mirrored="false"` always (O13)             |
 * | `gallery-time`                  | the time range 06:00–14:00, wrapped in LRI … PDI (D8)            |
 * | `gallery-open-sheet`, `gallery-sheet` | a button that opens a Sheet, and the Sheet                 |
 * | `gallery-load`                  | a button that loads `GET <api>/health` through the loading store |
 * | `loading-bar`                   | the LoadingBar, `role="progressbar"`, present only while shown   |
 * | `date-field`                    | a DateField's text input                                         |
 * | `date-field-open`               | its calendar button; the calendar is a `role="dialog"`           |
 * | `date-range`                    | a DateRangeField with its calendar shown inline                  |
 * | `date-range-summary`            | the dates the current choice stands for, as dd/MM/yyyy – dd/MM/yyyy |
 * | `preset-<key>`                  | a preset, `role="radio"` with `aria-checked`; keys: today, yesterday, this-week, last-week, last-7-days, last-30-days, this-month, last-month, custom |
 *
 * Calendar day cells (in both pickers) are `role="gridcell"` with `data-date="YYYY-MM-DD"`; a cell
 * inside the chosen range has `data-in-range="true"`. The chosen date is announced in a
 * `role="status"` region.
 */
import { expect, test, type Page } from "@playwright/test";

import { LANGUAGES, NOW, at, checkA11y, isPhone, movingElements, rgb } from "./support";

const GALLERY = "/dev/gallery";

async function openGallery(page: Page, language: (typeof LANGUAGES)[number], query: Record<string, string> = {}) {
  await page.clock.setFixedTime(NOW);
  await page.goto(at(GALLERY, language, query));
  await expect(page.getByTestId("gallery-root")).toBeVisible();
}

async function css(page: Page, testId: string, property: string): Promise<string> {
  return page.getByTestId(testId).evaluate((node, name) => getComputedStyle(node).getPropertyValue(name), property);
}

async function focusedDate(page: Page): Promise<string | null> {
  return page.evaluate(() => document.activeElement?.getAttribute("data-date") ?? null);
}

for (const language of LANGUAGES) {
  test.describe(`gallery in ${language}`, () => {
    test("draws Option B's colours and type in the light theme", async ({ page }) => {
      await openGallery(page, language, { theme: "light" });
      expect(await css(page, "gallery-root", "background-color")).toBe(rgb("#DDE1DD"));
      expect(await css(page, "gallery-cta", "background-color")).toBe(rgb("#D7A57E"));
      expect(await css(page, "gallery-link", "color")).toBe(rgb("#834B30"));
      expect(await css(page, "gallery-panel", "background-color")).toBe(rgb("#101413"));
      const family = await css(page, "gallery-title", "font-family");
      expect(family).toContain(language === "he" ? "IBM Plex Sans Hebrew" : "IBM Plex Sans");
    });

    test("draws the dark theme from the inverse set", async ({ page }) => {
      await openGallery(page, language, { theme: "dark" });
      expect(await css(page, "gallery-root", "background-color")).toBe(rgb("#101413"));
    });

    test("mirrors layout and arrows but not times or the clock", async ({ page }) => {
      await openGallery(page, language);
      await expect(page.locator("html")).toHaveAttribute("dir", language === "he" ? "rtl" : "ltr");
      const leading = await page.getByTestId("gallery-row-leading").boundingBox();
      const trailing = await page.getByTestId("gallery-row-trailing").boundingBox();
      expect(leading && trailing).toBeTruthy();
      if (language === "he") expect(leading!.x).toBeGreaterThan(trailing!.x);
      else expect(leading!.x).toBeLessThan(trailing!.x);
      await expect(page.getByTestId("gallery-arrow-next")).toHaveAttribute(
        "data-mirrored",
        language === "he" ? "true" : "false",
      );
      await expect(page.getByTestId("gallery-clock-icon")).toHaveAttribute("data-mirrored", "false");
      const time = await page.getByTestId("gallery-time").textContent();
      expect(time).toContain("⁦06:00");
      expect(time).toContain("14:00⁩");
    });

    test("reads a typed date with any separator as day, month, year", async ({ page }) => {
      await openGallery(page, language);
      const field = page.getByTestId("date-field");
      for (const typed of ["7/11/2026", "7.11.2026", "7-11-2026"]) {
        await field.fill(typed);
        await field.blur();
        await expect(field).toHaveValue("07/11/2026");
      }
    });

    test("follows the W3C date-picker keyboard pattern", async ({ page }) => {
      await openGallery(page, language);
      const field = page.getByTestId("date-field");
      await field.fill("07/11/2026");
      await field.blur();
      await page.getByTestId("date-field-open").click();
      await expect(page.getByRole("dialog")).toBeVisible();
      expect(await focusedDate(page)).toBe("2026-11-07");
      // The day after is to the right in English and to the left in Hebrew.
      await page.keyboard.press(language === "he" ? "ArrowLeft" : "ArrowRight");
      expect(await focusedDate(page)).toBe("2026-11-08");
      await page.keyboard.press("ArrowDown");
      expect(await focusedDate(page)).toBe("2026-11-15");
      await page.keyboard.press("PageDown");
      expect(await focusedDate(page)).toBe("2026-12-15");
      await page.keyboard.press("Enter");
      await expect(field).toHaveValue("15/12/2026");
      await expect(page.getByRole("status").filter({ hasText: "15/12/2026" })).toHaveCount(1);
      await page.getByTestId("date-field-open").click();
      await page.keyboard.press("Escape");
      await expect(page.getByRole("dialog")).toBeHidden();
      await expect(field).toBeFocused();
    });

    test("keeps a preset in sync with the highlighted range", async ({ page }) => {
      await openGallery(page, language);
      const range = page.getByTestId("date-range");
      await range.getByTestId("preset-last-week").click();
      await expect(range.getByTestId("preset-last-week")).toHaveAttribute("aria-checked", "true");
      // Weeks start on Sunday: the week before Sunday 1 November 2026.
      await expect(range.getByTestId("date-range-summary")).toContainText("25/10/2026");
      await expect(range.getByTestId("date-range-summary")).toContainText("31/10/2026");
      const inRange = range.locator('[role="gridcell"][data-in-range="true"]');
      await expect(inRange.first()).toHaveAttribute("data-date", "2026-10-25");
      await expect(inRange.last()).toHaveAttribute("data-date", "2026-10-31");
      await expect(inRange).toHaveCount(7);

      await range.getByTestId("preset-last-month").click();
      await expect(range.getByTestId("date-range-summary")).toContainText("01/10/2026");
      await expect(range.getByTestId("date-range-summary")).toContainText("31/10/2026");

      await range.locator('[role="gridcell"][data-date="2026-10-20"]').click();
      await expect(range.getByTestId("preset-custom")).toHaveAttribute("aria-checked", "true");
      await expect(range.getByTestId("preset-last-month")).toHaveAttribute("aria-checked", "false");
    });

    test("drives the searchable picker by keyboard", async ({ page }) => {
      await openGallery(page, language);
      await page.getByTestId("gallery-picker").click();
      const search = page.getByTestId("gallery-picker-search");
      await expect(search).toBeFocused();
      await search.fill("dana");
      await page.keyboard.press("ArrowDown");
      await page.keyboard.press("Enter");
      await expect(page.getByTestId("gallery-picker")).toContainText("דנה לוי");
      await expect(page.getByTestId("gallery-picker-sheet")).toHaveCount(0);
    });

    test("shows the loading bar for a slow load and not for a fast one", async ({ page }) => {
      await openGallery(page, language);
      // setFixedTime only fixes the date; the page's timers (the bar's 300 ms delay) run for real.
      let delay = 1500;
      await page.route("**/health", async (route) => {
        await new Promise((resolve) => setTimeout(resolve, delay));
        await route.fulfill({ status: 200, contentType: "application/json", body: '{"status":"ok"}' });
      });

      delay = 100;
      await page.getByTestId("gallery-load").click();
      for (let i = 0; i < 8; i += 1) {
        await expect(page.getByTestId("loading-bar")).toHaveCount(0);
        await page.waitForTimeout(100);
      }

      delay = 1500;
      await page.getByTestId("gallery-load").click();
      const bar = page.getByTestId("loading-bar");
      await expect(bar).toBeVisible({ timeout: 1200 });
      await expect(bar).toHaveAttribute("role", "progressbar");
      await expect(bar).toHaveCount(0, { timeout: 3000 });
    });

    test("moves nothing under reduce-motion", async ({ page }) => {
      await page.emulateMedia({ reducedMotion: "reduce" });
      await openGallery(page, language);
      await page.getByTestId("gallery-open-sheet").click();
      await expect(page.getByTestId("gallery-sheet")).toBeVisible();
      expect(await movingElements(page, "gallery-root")).toEqual([]);
    });

    test("meets target sizes and passes axe", async ({ page }, testInfo) => {
      await openGallery(page, language);
      if (isPhone(testInfo)) {
        const small = await page.getByTestId("gallery-root").evaluate((root) =>
          Array.from(root.querySelectorAll('button, [role="button"], a[href], [role="radio"]'))
            .map((node) => ({ id: node.getAttribute("data-testid"), box: node.getBoundingClientRect() }))
            .filter(({ box }) => box.width > 0 && (box.width < 44 || box.height < 44))
            .map(({ id, box }) => `${id ?? "?"} ${Math.round(box.width)}×${Math.round(box.height)}`),
        );
        expect(small, "targets under 44 × 44 on a phone").toEqual([]);
      }
      await checkA11y(page, `gallery (${language})`);
    });
  });
}
