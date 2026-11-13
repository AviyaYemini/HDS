/**
 * Group G — the work log and the hours report (specs/007-ux-redesign, task T087).
 *
 * Covers FR-025 (the name), FR-050 and D4, in Hebrew and English, at desktop and phone sizes. Test
 * ids: `specs/007-ux-redesign/contracts/ui-routes.md`, "Report test ids". The key figures must be
 * exactly what the API answers: the client never adds them up.
 */
import { expect, test } from "@playwright/test";

import { LANGUAGES, apiAs, at, checkA11y, ok, signIn, siteDay } from "./support";

function thisMonth(): { from: string; to: string } {
  const today = siteDay();
  const first = `${today.slice(0, 8)}01`;
  return { from: first, to: today };
}

for (const language of LANGUAGES) {
  test(`${language}: the work log is named as decided and its figures match the API`, async ({ page }) => {
    const manager = await apiAs("manager");
    const range = thisMonth();
    // The log opens on the whole company (the owner, PR 5).
    const api = await ok<{ people: number; shifts: number; hours: string }>(manager.get("/api/v1/reports/work-log", { params: range }));
    await manager.dispose();

    await signIn(page, "manager", language);
    await page.goto(at("/reports/work-log", language));
    await expect(page.getByTestId("screen-work-log")).toBeVisible();
    await expect(page.getByTestId("page-title")).toHaveText(language === "he" ? "יומן עבודה" : "Work log");
    await expect(page.locator("body")).not.toContainText("מי עבד");
    await expect(page.getByTestId("preset-this-pay-period")).toHaveAttribute("aria-checked", "true");
    await expect(page.getByTestId("figure-people")).toHaveAttribute("data-value", String(api.people));
    await expect(page.getByTestId("figure-shifts")).toHaveAttribute("data-value", String(api.shifts));
    await expect(page.getByTestId("figure-hours")).toHaveAttribute("data-value", api.hours);
    // Reports cannot reach into the future.
    const tomorrow = siteDay(1);
    await expect(page.getByTestId("date-range").locator(`[role="gridcell"][data-date="${tomorrow}"]`)).toHaveAttribute("aria-disabled", "true");
    await checkA11y(page, `work log (${language})`);
  });

  // demo/ceo: hours and costs in the deck's style (numbered figures, bars, a table by project).
  test(`${language}: hours and costs shows the API's figures and its table`, async ({ page }) => {
    const manager = await apiAs("manager");
    const api = await ok<{ hours: string; shifts: number; groups: unknown[] }>(manager.get("/api/v1/reports/hours", { params: { ...thisMonth(), group_by: "project" } }));
    await manager.dispose();
    await signIn(page, "manager", language);
    await page.goto(at("/reports/hours", language));
    await expect(page.getByTestId("screen-hours-costs")).toBeVisible();
    await expect(page.getByTestId("figure-hours")).toHaveAttribute("data-value", api.hours);
    if (api.groups.length > 0) await expect(page.getByTestId("hc-table")).toBeVisible();
    await checkA11y(page, `hours report (${language})`);
  });
}

// The owner's PR 5 additions: the whole company is the work log's default, and hours go by client.
test("the work log opens on the whole company, and the hours report groups by client", async ({ page }) => {
  const manager = await apiAs("manager");
  const range = thisMonth();
  const byClient = await ok<{ hours: string; shifts: number }>(manager.get("/api/v1/reports/hours", { params: { ...range, group_by: "client" } }));
  await manager.dispose();
  await signIn(page, "manager", "en");
  await page.goto(at("/reports/work-log", "en"));
  await expect(page.getByTestId("scope-company")).toHaveAttribute("aria-checked", "true");
  await expect(page.getByTestId("report-scope-picker")).toHaveCount(0);
  await page.goto(at("/reports/hours", "en"));
  await page.getByTestId("group-client").click();
  await expect(page.getByTestId("figure-hours")).toHaveAttribute("data-value", byClient.hours);
});
