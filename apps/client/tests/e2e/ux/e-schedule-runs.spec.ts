/**
 * Group E — schedule runs as they are today, in the new design (specs/007-ux-redesign, task T077).
 *
 * Covers FR-042, FR-074 (the determinate loading bar) and US7's run scenarios, in Hebrew and
 * English, at desktop and phone sizes. Test ids: `specs/007-ux-redesign/contracts/ui-routes.md`,
 * "Schedule run test ids". The solver streams no interim result, so the bar shows elapsed time
 * against the time limit; "best so far" comes with feature 008.
 */
import { expect, test, type APIRequestContext } from "@playwright/test";

import { LANGUAGES, addDays, apiAs, at, checkA11y, ok, signIn, siteDay, sundayOf } from "./support";

async function needsSomething(admin: APIRequestContext): Promise<string> {
  const projects = await ok<{ items: { id: string; name: string }[] }>(admin.get("/api/v1/projects"));
  const project = projects.items.find((p) => p.name === "Night Watch")!;
  const types = await ok<{ items: { id: string; name: string }[] }>(admin.get("/api/v1/shift-types"));
  await ok(admin.put(`/api/v1/projects/${project.id}/shift-types`, { data: { ids: types.items.map((t) => t.id) } }));
  const morning = types.items.find((t) => t.name === "morning")!.id;
  await ok(admin.put(`/api/v1/projects/${project.id}/requirements`, {
    data: { items: [0, 1, 2, 3, 4].map((weekday) => ({ shift_type_id: morning, weekday, headcount: 1 })) },
  }));
  return project.id;
}

// A run searches for up to 5 s here (the e2e time limit) after the worker claims it.
test.setTimeout(120_000);

for (const language of LANGUAGES) {
  test(`${language}: generate with a preset range, watch it run, read the honesty label, discard`, async ({ page }) => {
    const admin = await apiAs("admin");
    await needsSomething(admin);
    await admin.dispose();

    await signIn(page, "manager", language);
    await page.goto(at("/runs", language));
    await expect(page.getByTestId("screen-runs")).toBeVisible();
    await checkA11y(page, `runs (${language})`);
    await page.getByTestId("action-new-run").click();
    const range = page.getByTestId("date-range");
    await range.getByTestId("preset-custom").click();
    const next = addDays(sundayOf(siteDay()), 7);
    // A run cannot start in the past: yesterday is disabled in the calendar.
    await expect(range.locator(`[role="gridcell"][data-date="${siteDay(-1)}"]`)).toHaveAttribute("aria-disabled", "true");
    await range.locator(`[role="gridcell"][data-date="${next}"]`).click();
    await range.locator(`[role="gridcell"][data-date="${addDays(next, 4)}"]`).click();
    await page.getByTestId("action-generate").click();

    await expect(page.getByTestId("screen-run")).toBeVisible();
    const bar = page.getByTestId("loading-bar");
    const values: number[] = [];
    for (let i = 0; i < 6 && (await bar.count()) > 0; i += 1) {
      const now = await bar.getAttribute("aria-valuenow");
      if (now !== null) values.push(Number(now));
      await page.waitForTimeout(500);
    }
    for (let i = 1; i < values.length; i += 1) expect(values[i]).toBeGreaterThanOrEqual(values[i - 1]!);

    const label = page.getByTestId("run-honesty-label");
    await expect(label).toBeVisible({ timeout: 90_000 });
    await expect(label).toHaveText(language === "he" ? /אופטימלי|הטוב ביותר שנמצא/ : /Optimal|Best found in \d+ s/);
    await expect(page.getByTestId("run-filled")).toContainText("/");
    await checkA11y(page, `run (${language})`);
    await page.getByTestId("action-discard").click();
    await expect(page.getByTestId("action-accept")).toHaveCount(0);
  });
}
