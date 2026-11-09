/**
 * Group C of 008 — steering a run, its trade-offs and fairness (specs/008-auto-scheduler-review
 * T034).
 *
 * Covers FR-017 to FR-026 and US4, US5, in Hebrew and English, at desktop and phone sizes: the run
 * form's two controls and a chip built in words; a run's guarantee line, summary in words, chips'
 * states and fairness panel; a lock set in the proposal, marked by more than colour; and "Run again",
 * which opens the form with the run's chips and keeps its locks.
 */
import { expect, test, type APIRequestContext } from "@playwright/test";

import { LANGUAGES, addDays, apiAs, checkA11y, ok, openAs, siteDay, sundayOf } from "./support";

type Review = { version: number; slots: { key: string; date: string; people: { user_id: string }[] }[] };

async function nightWatch(admin: APIRequestContext): Promise<string> {
  const projects = await ok<{ items: { id: string; name: string }[] }>(admin.get("/api/v1/projects"));
  const project = projects.items.find((p) => p.name === "Night Watch")!;
  const types = await ok<{ items: { id: string; name: string }[] }>(admin.get("/api/v1/shift-types"));
  await ok(admin.put(`/api/v1/projects/${project.id}/shift-types`, { data: { ids: types.items.map((t) => t.id) } }));
  const morning = types.items.find((t) => t.name === "morning")!.id;
  await ok(admin.put(`/api/v1/projects/${project.id}/requirements`, {
    data: { items: [0, 1, 2, 3, 4].map((weekday) => ({ shift_type_id: morning, weekday, headcount: 2 })) },
  }));
  return project.id;
}

async function finished(manager: APIRequestContext, id: string): Promise<void> {
  for (let i = 0; i < 120; i += 1) {
    const read = await ok<{ status: string }>(manager.get(`/api/v1/schedule-jobs/${id}`));
    if (read.status === "succeeded") return;
    expect(read.status).not.toBe("failed");
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error("the run did not finish");
}

test.setTimeout(150_000);

LANGUAGES.forEach((language, index) => {
  test(`${language}: controls and chips, the guarantee and fairness, a lock, and run again`, async ({ page }, testInfo) => {
    const admin = await apiAs("admin");
    const manager = await apiAs("manager");
    const project = await nightWatch(admin);
    const viewport = testInfo.project.name.includes("phone") ? 1 : 0;
    const first = addDays(sundayOf(siteDay()), 7 * (40 + (index * 2 + viewport) * 40 + Math.floor(Math.random() * 40)));
    const listed = await ok<{ items: { id: string; role: string; is_active: boolean }[] }>(admin.get("/api/v1/employees"));
    const someone = listed.items.find((p) => p.role === "employee" && p.is_active)!.id;

    // The form: the two controls in words, and a chip built from a kind, a person and a count.
    await openAs(page, "manager", language, "/runs");
    await page.getByTestId("action-new-run").click();
    await expect(page.getByTestId("run-steering")).toBeVisible();
    await page.getByTestId("control-changes").getByRole("radio").nth(1).click();
    await page.getByTestId("action-chip-new").click();
    await page.getByTestId("chip-kind").click();
    await page.getByTestId("chip-kind-option-max_nights").click();
    await page.getByTestId("chip-person").click();
    await page.getByTestId(`chip-person-option-${someone}`).click();
    await page.getByTestId("chip-count").fill("1");
    await page.getByTestId("action-chip-save").click();
    await expect(page.getByTestId("run-chip")).toHaveCount(1);
    await checkA11y(page, `run form with a chip (${language})`);

    // A run with that chip, as the form sends it.
    const job = await ok<{ id: string }>(
      manager.post("/api/v1/schedule-jobs", {
        data: { from: first, to: addDays(first, 4), project_ids: [project], controls: { keep_changes_small: true, wishes_strictly: true }, chips: [{ kind: "max_nights", user_id: someone, count: 1 }] },
      }),
      202,
    );
    await finished(manager, job.id);

    await openAs(page, "manager", language, `/runs/${job.id}`);
    await expect(page.getByTestId("fairness-panel")).toBeVisible({ timeout: 30_000 });
    await expect(page.getByTestId("review-guarantee")).toBeVisible();
    await expect(page.getByTestId("review-summary")).not.toBeEmpty();
    await expect(page.getByTestId("review-chip")).toHaveCount(1);
    await expect(page.getByTestId("fairness-row").first()).toBeVisible();
    await checkA11y(page, `the guarantee, the summary and fairness (${language})`);

    // Lock someone in the proposal: marked with words, and counted as a lock kept.
    const read = await ok<Review>(manager.get(`/api/v1/schedule-jobs/${job.id}/proposal`));
    const slot = read.slots.find((s) => s.people.length > 0)!;
    const person = slot.people[0]!.user_id;
    const block = page.locator(`[data-testid="proposal-block"][data-slot-key="${slot.key}"]`);
    await block.locator(`[data-testid="proposal-person"][data-user-id="${person}"]`).getByTestId("review-person").click();
    await page.getByTestId("action-review-lock").click();
    await expect(block.locator(`[data-testid="proposal-person"][data-user-id="${person}"]`).getByTestId("mark-locked")).toBeVisible();
    await expect(page.getByTestId("review-locks")).toBeVisible();

    // Run again: the form with the run's chip, and the lock kept by the rerun.
    await page.getByTestId("action-run-again").click();
    await expect(page.getByTestId("run-again-note")).toBeVisible({ timeout: 30_000 });
    await expect(page.getByTestId("run-chip")).toHaveCount(1);
    const rerun = await ok<{ id: string }>(manager.post("/api/v1/schedule-jobs", { data: { from: first, to: addDays(first, 4), project_ids: [project], based_on: job.id } }), 202);
    await finished(manager, rerun.id);
    const again = await ok<{ proposal: { slots: { key: string; kept: string[] }[] } }>(manager.get(`/api/v1/schedule-jobs/${rerun.id}`));
    expect(again.proposal.slots.find((s) => s.key === slot.key)!.kept).toContain(person);

    await ok(manager.post(`/api/v1/schedule-jobs/${job.id}/discard`));
    await ok(manager.post(`/api/v1/schedule-jobs/${rerun.id}/discard`));
    await admin.dispose();
    await manager.dispose();
  });
});
