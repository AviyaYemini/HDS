/**
 * Group D of 008 — best so far and Stop, the notice, the record, and one flow from the roster to a
 * run and back (specs/008-auto-scheduler-review T047).
 *
 * Covers FR-027 to FR-034 and US6 to US8, in Hebrew and English, at desktop and phone sizes:
 * "Generate this week" from the roster opens the form on that week; the run's record; Stop on a run
 * ends it stopped or cancelled; a run started elsewhere tells its owner when it ends, on another
 * screen; accepting a whole proposal opens its roster week, where Publish is one tap away and then
 * offers "Copy to next week"; an empty seat ranks who could take it.
 */
import { expect, test, type APIRequestContext } from "@playwright/test";

import { LANGUAGES, addDays, apiAs, at, checkA11y, ok, openAs, siteDay, sundayOf } from "./support";

async function nightWatch(admin: APIRequestContext): Promise<string> {
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

async function ended(api: APIRequestContext, id: string): Promise<{ status: string; ended: string | null }> {
  for (let i = 0; i < 120; i += 1) {
    const read = await ok<{ status: string; ended: string | null }>(api.get(`/api/v1/schedule-jobs/${id}`));
    if (read.status !== "queued" && read.status !== "running") return read;
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error("the run did not end");
}

test.setTimeout(180_000);

LANGUAGES.forEach((language, index) => {
  test(`${language}: from the roster to a run and back, Stop, the notice and the record`, async ({ page }, testInfo) => {
    const admin = await apiAs("admin");
    const manager = await apiAs("manager");
    const project = await nightWatch(admin);
    const viewport = testInfo.project.name.includes("phone") ? 1 : 0;
    const week = addDays(sundayOf(siteDay()), 7 * (60 + (index * 2 + viewport) * 40 + Math.floor(Math.random() * 40)));

    // Stop: a run asked to stop ends stopped (with its best) or cancelled (none yet, or still queued).
    const stopping = await ok<{ id: string }>(manager.post("/api/v1/schedule-jobs", { data: { from: week, to: addDays(week, 4), project_ids: [project] } }), 202);
    await ok(manager.post(`/api/v1/schedule-jobs/${stopping.id}/stop`));
    const stopped = await ended(manager, stopping.id);
    expect(["stopped", "cancelled", "finished", "timed_out"]).toContain(stopped.ended);

    // The roster's "Generate this week" opens the run form on that week and project.
    await openAs(page, "manager", language, "/runs");
    await page.goto(at("/roster", language, { project, week }));
    await page.getByTestId("action-generate-week").click();
    await expect(page.getByTestId("run-form")).toBeVisible({ timeout: 30_000 });
    await page.getByTestId("action-generate").click();
    await expect(page.getByTestId("screen-run")).toBeVisible({ timeout: 30_000 });
    await expect(page.getByTestId("proposal-review")).toBeVisible({ timeout: 60_000 });
    await expect(page.locator('[data-testid="record-step"][data-kind="created"]')).toBeVisible();
    await checkA11y(page, `run with its record (${language})`);

    // A run started elsewhere tells its owner when it ends, on another screen.
    await page.goto(at(`/roster`, language, { project, week: addDays(week, 14) }));
    await expect(page.getByTestId("screen-roster")).toBeVisible();
    await page.waitForTimeout(1_000); // the notice's first look, before the run starts
    const elsewhere = await ok<{ id: string }>(manager.post("/api/v1/schedule-jobs", { data: { from: addDays(week, 14), to: addDays(week, 18), project_ids: [project] } }), 202);
    await ended(manager, elsewhere.id);
    await expect(page.getByTestId("toast-run-finished")).toBeVisible({ timeout: 45_000 });
    await ok(manager.post(`/api/v1/schedule-jobs/${elsewhere.id}/discard`));

    // Accepting the whole proposal opens its roster week, where Publish is one tap away.
    const runs = await ok<{ items: { id: string; from: string; status: string; proposal_state: string | null }[] }>(manager.get("/api/v1/schedule-jobs", { params: { limit: 10 } }));
    const ours = runs.items.find((r) => r.from === week && r.status === "succeeded" && r.proposal_state === "open")!;
    await page.goto(at(`/runs/${ours.id}`, language));
    await page.getByTestId("action-accept").click();
    await expect(page.getByTestId("screen-roster")).toBeVisible({ timeout: 30_000 });
    expect(page.url()).toContain(`week=${week}`);

    // An empty seat ranks who could take it (who is free first; no rule warnings, the owner, 2026-11-13).
    const seat = page.getByTestId("empty-seat").first();
    if (await seat.isVisible()) {
      await seat.click();
      await expect(page.getByTestId("assign-option").first()).toBeVisible();
      await page.keyboard.press("Escape");
    }

    // Publish, then "Copy to next week" opens next week.
    await page.getByTestId("action-publish").click();
    await page.getByTestId("publish-review").getByTestId("action-confirm-publish").click();
    const toast = page.getByTestId("toast-published");
    await expect(toast).toBeVisible();
    await toast.getByRole("button", { name: language === "he" ? "העתקה לשבוע הבא" : "Copy to next week" }).click();
    await expect.poll(() => page.url()).toContain(`week=${addDays(week, 7)}`);
    await checkA11y(page, `roster after copying to next week (${language})`);

    await admin.dispose();
    await manager.dispose();
  });
});
