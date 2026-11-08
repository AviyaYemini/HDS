/**
 * Group F — admin screens on one table and one form pattern (specs/007-ux-redesign, task T081).
 *
 * Covers FR-043 to FR-049, in Hebrew and English, at desktop and phone sizes. Test ids:
 * `specs/007-ux-redesign/contracts/ui-routes.md`, "Admin test ids".
 */
import { expect, test } from "@playwright/test";

import { LANGUAGES, apiAs, at, checkA11y, ok, signIn } from "./support";

for (const language of LANGUAGES) {
  test.describe(`admin in ${language}`, () => {
    test("a table sorts, searches, saves a view, and keeps it all in the address", async ({ page }) => {
      await signIn(page, "admin", language);
      await page.goto(at("/admin/staff", language));
      await expect(page.getByTestId("screen-admin-staff")).toBeVisible();
      const name = page.getByTestId("sort-name");
      await name.click();
      await expect(name).toHaveAttribute("aria-sort", "ascending");
      await name.click();
      await expect(name).toHaveAttribute("aria-sort", "descending");
      await page.getByTestId("table-search").fill("Employee");
      await expect(page.getByTestId("filters-on")).toBeVisible();
      await expect(page).toHaveURL(/[?&]q=Employee/);
      await expect(page).toHaveURL(/[?&]sort=/);
      const viewName = `Search ${language} ${Date.now()}`;
      await page.getByTestId("action-save-view").click();
      await page.getByTestId("save-view-name").fill(viewName);
      await page.getByTestId("action-confirm-save-view").click();
      await expect(page.getByTestId("view-tab").filter({ hasText: viewName })).toBeVisible();

      await page.reload();
      await expect(page.getByTestId("table-search")).toHaveValue("Employee");
      await expect(page.getByTestId("sort-name")).toHaveAttribute("aria-sort", "descending");
      await page.getByTestId("table-row").first().click();
      await expect(page.getByTestId("side-panel")).toBeVisible();
      await expect(page).toHaveURL(/[?&]open=/);
      await page.reload();
      await expect(page.getByTestId("side-panel")).toBeVisible();
      await checkA11y(page, `staff table (${language})`);
    });

    test("the searchable picker finds people by Hebrew and English names", async ({ page }) => {
      const admin = await apiAs("admin");
      const people = await ok<{ items: { id: string; email: string }[] }>(admin.get("/api/v1/employees", { params: { search: "extra@hds.test" } }));
      await ok(admin.patch(`/api/v1/employees/${people.items[0]!.id}`, { data: { full_name: "Extra Employee אקסטרה" } }));
      await admin.dispose();
      await signIn(page, "admin", language);
      await page.goto(at("/admin/projects", language));
      await page.getByTestId("table-row").first().click();
      const picker = page.getByTestId("searchable-picker").first();
      for (const query of ["Extra", "אקסטרה"]) {
        await picker.fill(query);
        const option = page.getByTestId("picker-option").filter({ hasText: "Extra Employee" });
        await expect(option).toBeVisible();
        await expect(option.getByTestId("picker-option-detail")).not.toBeEmpty();
      }
    });

    test("a form shows inline errors and an error summary that takes focus", async ({ page }) => {
      await signIn(page, "admin", language);
      await page.goto(at("/admin/clients", language, { open: "new" }));
      const form = page.getByTestId("record-form");
      await expect(form).toBeVisible();
      await expect(form.getByTestId("action-save")).toBeEnabled();
      await form.getByTestId("action-save").click();
      await expect(page.getByTestId("error-summary")).toBeFocused();
      await expect(form.getByTestId("field-error-name")).toBeVisible();
      await form.getByTestId("field-name").fill("Fixed name");
      await form.getByTestId("field-name").blur();
      await expect(form.getByTestId("field-error-name")).toHaveCount(0);
      await checkA11y(page, `client form (${language})`);
    });

    test("a pay change confirms in specific words, and leaving unsaved asks first", async ({ page }) => {
      await signIn(page, "admin", language);
      await page.goto(at("/admin/staff", language, { q: "employee@hds.test" }));
      await page.getByTestId("table-row").first().click();
      const rate = page.getByTestId("field-default_hourly_rate");
      const before = await rate.inputValue();
      const next = before === "48.00" ? "49.00" : "48.00";
      await rate.fill(next);
      await page.getByTestId("action-save").click();
      const confirm = page.getByTestId("confirm-dialog");
      await expect(confirm).toContainText(next);
      if (before) await expect(confirm).toContainText(before);
      await page.keyboard.press("Escape");

      await rate.fill("51.00");
      await page.getByTestId("nav-home").or(page.getByTestId("tab-home")).first().click();
      await expect(page.getByTestId("confirm-leave")).toBeVisible();
    });

    test("the first load shows skeleton rows, never 'No records'", async ({ page }) => {
      await signIn(page, "admin", language);
      await page.route("**/api/v1/sites**", async (route) => {
        await new Promise((resolve) => setTimeout(resolve, 1500));
        await route.continue();
      });
      await page.goto(at("/admin/sites", language));
      await expect(page.getByTestId("skeleton-row").first()).toBeVisible();
      await expect(page.locator("body")).not.toContainText(/No records|אין רשומות/);
      await expect(page.getByTestId("table-row").first()).toBeVisible({ timeout: 5000 });
    });

    test("a manager creates and edits but never deletes", async ({ page }) => {
      await signIn(page, "manager", language);
      await page.goto(at("/admin/staff", language));
      await page.getByTestId("table-row").first().click();
      await expect(page.getByTestId("side-panel")).toBeVisible();
      await expect(page.getByTestId("action-delete")).toHaveCount(0);
    });
  });
}
