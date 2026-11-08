/**
 * Group A of 008 — review a proposal as a change, accept it in parts, edit it (specs/008-auto-
 * scheduler-review T010).
 *
 * Covers FR-001 to FR-012 and US1, US2, in Hebrew and English, at desktop and phone sizes: the
 * marks and their counts in words, accepting one day, an edit and its undo, a rule break that asks
 * for a reason before it is kept, and discarding the rest.
 */
import { expect, test, type APIRequestContext } from "@playwright/test";

import { LANGUAGES, addDays, apiAs, at, checkA11y, ok, openAs, siteDay, sundayOf } from "./support";

type Review = {
  version: number;
  slots: { key: string; date: string; places: number; people: { user_id: string }[] }[];
};

/** Night Watch needs three in the morning on weekdays; the seeded Manager manages it. */
async function nightWatchNeeds(admin: APIRequestContext): Promise<string> {
  const projects = await ok<{ items: { id: string; name: string }[] }>(admin.get("/api/v1/projects"));
  const project = projects.items.find((p) => p.name === "Night Watch")!;
  const types = await ok<{ items: { id: string; name: string }[] }>(admin.get("/api/v1/shift-types"));
  await ok(admin.put(`/api/v1/projects/${project.id}/shift-types`, { data: { ids: types.items.map((t) => t.id) } }));
  const morning = types.items.find((t) => t.name === "morning")!.id;
  await ok(admin.put(`/api/v1/projects/${project.id}/requirements`, {
    data: { items: [0, 1, 2, 3, 4].map((weekday) => ({ shift_type_id: morning, weekday, headcount: 3 })) },
  }));
  return project.id;
}

/** The week after next, as the Manager, solved. */
async function solvedRun(manager: APIRequestContext, project: string, weeks: number): Promise<string> {
  const first = addDays(sundayOf(siteDay()), 7 * weeks);
  const job = await ok<{ id: string }>(manager.post("/api/v1/schedule-jobs", { data: { from: first, to: addDays(first, 4), project_ids: [project] } }), 202);
  for (let i = 0; i < 120; i += 1) {
    const read = await ok<{ status: string }>(manager.get(`/api/v1/schedule-jobs/${job.id}`));
    if (read.status === "succeeded") return job.id;
    expect(read.status).not.toBe("failed");
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error("the run did not finish");
}

test.setTimeout(150_000);

LANGUAGES.forEach((language, index) => {
  test(`${language}: marks, accept one day, edit and undo, a reason for a rule break, discard the rest`, async ({ page }, testInfo) => {
    const admin = await apiAs("admin");
    const manager = await apiAs("manager");
    const project = await nightWatchNeeds(admin);
    // A week of its own, far ahead and drawn at random, so neither another test's drafts nor an
    // earlier run's (the test database keeps them) are already there.
    const viewport = testInfo.project.name.includes("phone") ? 1 : 0;
    const weeks = 20 + ((index * 2 + viewport) * 40 + Math.floor(Math.random() * 40));
    const job = await solvedRun(manager, project, weeks);

    await openAs(page, "manager", language, `/runs/${job}`);
    const review = page.getByTestId("proposal-review");
    await expect(review).toBeVisible({ timeout: 30_000 });
    await expect(page.getByTestId("review-counts")).toContainText(language === "he" ? "נוספו" : "added");
    await expect(page.locator('[data-testid="proposal-person"][data-change="added"]').first()).toBeVisible();
    await checkA11y(page, `proposal review (${language})`);

    // Accept the first day only: its marks read accepted, the rest stay open.
    const firstDay = page.getByTestId("action-accept-day").first();
    const day = await firstDay.getAttribute("data-date");
    await firstDay.click();
    const accepted = page.locator(`[data-testid="proposal-day"][data-date="${day}"] [data-testid="mark-accepted"]`);
    await expect(accepted.first()).toBeVisible();
    await expect(page.getByTestId("action-accept")).toBeVisible();

    // Take someone off a later day, then undo it.
    const read = await ok<Review>(manager.get(`/api/v1/schedule-jobs/${job}/proposal`));
    const later = read.slots.find((s) => s.date !== day && s.people.length > 0)!;
    const block = page.locator(`[data-testid="proposal-block"][data-slot-key="${later.key}"]`);
    const person = later.people[0]!.user_id;
    const before = await review.getAttribute("data-version");
    await block.getByTestId("review-person").first().click();
    await page.getByTestId("action-review-unassign").click();
    await expect(review).not.toHaveAttribute("data-version", before!);
    await expect(block.locator(`[data-testid="proposal-person"][data-user-id="${person}"]:not([data-change="removed"])`)).toHaveCount(0);
    await page.getByTestId("action-undo").click();
    await expect(block.locator(`[data-testid="proposal-person"][data-user-id="${person}"]:not([data-change="removed"])`)).toHaveCount(1);

    // One more person than a full shift needs breaks a rule: a reason is asked before it is kept.
    const listed = await ok<{ items: { id: string; role: string; is_active: boolean }[] }>(admin.get("/api/v1/employees"));
    // Staff only (Managers and Admins too): the list holds client accounts, never placed.
    const people = { items: listed.items.filter((p) => p.role !== "client" && p.is_active) };
    const now = await ok<Review>(manager.get(`/api/v1/schedule-jobs/${job}/proposal`));
    const full = now.slots.find((s) => s.date !== day && s.key !== later.key)!;
    const onIt = new Set(full.people.map((p) => p.user_id));
    // Fill its empty places through the API first, so the person the screen adds is one too many.
    const free = people.items.map((p) => p.id).filter((id) => !onIt.has(id));
    expect(free.length, "staff enough to fill the shift and add one more").toBeGreaterThan(full.places);
    for (let i = 0; i < full.places; i += 1) {
      const id = free.shift()!;
      onIt.add(id);
      await ok(manager.post(`/api/v1/schedule-jobs/${job}/edits`, { data: { op: "assign", key: full.key, user_id: id, override_reason: "filling for the test" } }));
    }
    // Back with the language: the app drops `?lang=` from the address once loaded.
    await page.goto(at(`/runs/${job}`, language));
    await expect(review).toBeVisible({ timeout: 30_000 });
    const extra = free.shift()!;
    const target = page.locator(`[data-testid="proposal-block"][data-slot-key="${full.key}"]`);
    await target.getByTestId("action-review-assign").click();
    await page.locator(`[data-testid="review-assign-option"][data-user-id="${extra}"]`).click();
    const sheet = page.getByTestId("review-override-sheet");
    await expect(sheet.getByTestId("review-break").first()).toBeVisible();
    await expect(page.getByTestId("action-keep-anyway")).toBeDisabled();
    await page.getByTestId("review-override-reason").fill(language === "he" ? "צריך עוד אדם" : "One more is needed");
    await page.getByTestId("action-keep-anyway").click();
    await expect(target.locator(`[data-testid="proposal-person"][data-user-id="${extra}"]`).getByTestId("mark-override")).toBeVisible();
    await checkA11y(page, `proposal edited (${language})`);

    await page.getByTestId("action-discard").click();
    await expect(page.getByTestId("run-proposal-state")).toContainText(language === "he" ? "אושר בחלקו" : "Partly accepted");
    await expect(page.getByTestId("action-accept")).toHaveCount(0);
    await admin.dispose();
    await manager.dispose();
  });
});
