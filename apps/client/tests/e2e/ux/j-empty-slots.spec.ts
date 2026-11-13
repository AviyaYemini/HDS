/**
 * Group B of 008 — why a slot is empty and who else could cover it (specs/008-auto-scheduler-
 * review T023).
 *
 * Covers FR-013 to FR-016 and US3, in Hebrew and English, at desktop and phone sizes: the empty
 * place's reasons name a guard kept off by rest, with the numbers; "Who else could cover" lists the
 * free first and the guard after; picking a free person places them; picking the
 * guard is simply stored, with no reason asked.
 */
import { expect, test, type APIRequestContext } from "@playwright/test";

import { LANGUAGES, addDays, apiAs, checkA11y, ok, openAs, siteDay, sundayOf } from "./support";

type Project = { id: string; name: string };
type Review = { slots: { key: string; date: string }[] };

/** The seeded projects and shift types, every type used by both projects. */
async function places(admin: APIRequestContext): Promise<{ watch: Project; patrol: Project; types: Record<string, string> }> {
  const projects = await ok<{ items: Project[] }>(admin.get("/api/v1/projects"));
  const watch = projects.items.find((p) => p.name === "Night Watch")!;
  const patrol = projects.items.find((p) => p.name === "Day Patrol")!;
  const listed = await ok<{ items: { id: string; name: string }[] }>(admin.get("/api/v1/shift-types"));
  for (const project of [watch, patrol]) {
    await ok(admin.put(`/api/v1/projects/${project.id}/shift-types`, { data: { ids: listed.items.map((t) => t.id) } }));
  }
  return { watch, patrol, types: Object.fromEntries(listed.items.map((t) => [t.name, t.id])) };
}

test.setTimeout(150_000);

LANGUAGES.forEach((language, index) => {
  test(`${language}: an empty place names who rest keeps off, and who else could cover fills it`, async ({ page }, testInfo) => {
    const admin = await apiAs("admin");
    const manager = await apiAs("manager");
    const { watch, patrol, types } = await places(admin);
    // A week of its own, far ahead and drawn at random (the test database keeps earlier runs).
    const viewport = testInfo.project.name.includes("phone") ? 1 : 0;
    const sunday = addDays(sundayOf(siteDay()), 7 * (30 + (index * 2 + viewport) * 40 + Math.floor(Math.random() * 40)));
    const monday = addDays(sunday, 1);

    // More places on Monday morning than the staff can fill, so some stay empty.
    await ok(admin.put(`/api/v1/projects/${watch.id}/requirements`, { data: { items: [{ shift_type_id: types.morning, weekday: 1, headcount: 60 }] } }));
    // A guard who works Day Patrol's night into Monday 07:00: rest keeps them off Monday's morning.
    const stamp = `${Date.now()}${Math.floor(Math.random() * 1000)}`;
    const tired = await ok<{ id: string }>(
      admin.post("/api/v1/employees", { data: { email: `tired.${stamp}@hds.test`, full_name: `Tired Guard ${stamp.slice(-4)}`, initial_password: "a-long-password-1" } }),
      201,
    );
    const night = await ok<{ shift: { id: string } }>(admin.post("/api/v1/shifts", { data: { project_id: patrol.id, shift_type_id: types.night, date: sunday, headcount: 1 } }), 201);
    await ok(admin.post(`/api/v1/shifts/${night.shift.id}/assignments`, { data: { user_id: tired.id } }), 201);

    const job = await ok<{ id: string }>(manager.post("/api/v1/schedule-jobs", { data: { from: monday, to: monday, project_ids: [watch.id] } }), 202);
    for (let i = 0; ; i += 1) {
      const read = await ok<{ status: string }>(manager.get(`/api/v1/schedule-jobs/${job.id}`));
      if (read.status === "succeeded") break;
      expect(read.status).not.toBe("failed");
      expect(i, "the run finishes").toBeLessThan(120);
      await new Promise((resolve) => setTimeout(resolve, 500));
    }

    await openAs(page, "manager", language, `/runs/${job.id}`);
    await expect(page.getByTestId("proposal-review")).toBeVisible({ timeout: 30_000 });
    // The run's reasons: rest names the guard with the numbers (FR-013).
    const named = page.locator(`[data-testid="unfilled-person"][data-user-id="${tired.id}"]`);
    await expect(named).toBeVisible();
    await expect(named).toContainText(language === "he" ? "0 ש׳ מנוחה" : "0 h rest");
    await checkA11y(page, `empty places (${language})`);

    const read = await ok<Review>(manager.get(`/api/v1/schedule-jobs/${job.id}/proposal`));
    const key = read.slots.find((s) => s.date === monday)!.key;
    const block = page.locator(`[data-testid="proposal-block"][data-slot-key="${key}"]`);

    // The empty slots come first, in red; one opens who else could cover: the free first, then
    // the guard, with the rule (FR-014, T042b).
    await expect(page.getByTestId("review-empty")).toBeVisible();
    await page.locator(`[data-testid="review-empty-slot"][data-slot-key="${key}"]`).click();
    const sheet = page.getByTestId("review-assign-sheet");
    await expect(sheet.getByTestId("review-why-empty")).toBeVisible();
    const options = sheet.getByTestId("review-cover-option");
    await expect(options.first()).toBeVisible();
    const free = await options.evaluateAll((all) => all.map((o) => o.getAttribute("data-free")));
    expect(free, "free people come first").toEqual([...free].sort().reverse());
    await expect(sheet.locator(`[data-testid="review-cover-option"][data-user-id="${tired.id}"]`)).toHaveAttribute("data-free", "false");
    await checkA11y(page, `who else could cover (${language})`);

    // Picking a free person places them (FR-015).
    const first = sheet.locator('[data-testid="review-assign-option"][data-free="true"]').first();
    const picked = await first.getAttribute("data-user-id");
    await first.click();
    await expect(block.locator(`[data-testid="proposal-person"][data-user-id="${picked}"]`)).toBeVisible();

    // Picking the guard breaks the rest rule, and the edit is simply stored: no reason asked, no
    // warning (the owner, 2026-11-13).
    await block.getByTestId("action-review-assign").click();
    await sheet.locator(`[data-testid="review-assign-option"][data-user-id="${tired.id}"]`).click();
    await expect(block.locator(`[data-testid="proposal-person"][data-user-id="${tired.id}"]`)).toBeVisible();
    await expect(page.getByTestId("review-override-sheet")).toHaveCount(0);

    await ok(manager.post(`/api/v1/schedule-jobs/${job.id}/discard`));
    await admin.dispose();
    await manager.dispose();
  });
});
