/**
 * 004 US2 to US4 — Generating a roster: a Manager asks for next week, sees the proposal with its
 * empty places and their reasons, and accepts it as drafts. Drawn in 007's design (T078, T079):
 * the run list, the generate form and the run's page; since 008, accepting opens the roster week.
 */
import { expect, test, type APIRequestContext } from "@playwright/test";

import { apiAs, ok, signIn } from "./support";

/** Night Watch needs more morning people on Sundays than anyone can give, so places stay empty. */
async function needsTooMany(admin: APIRequestContext): Promise<void> {
  const projects = await ok<{ items: { id: string; name: string }[] }>(admin.get("/api/v1/projects"));
  const project = projects.items.find((p) => p.name === "Night Watch")!;
  const types = await ok<{ items: { id: string; name: string }[] }>(admin.get("/api/v1/shift-types"));
  await ok(admin.put(`/api/v1/projects/${project.id}/shift-types`, { data: { ids: types.items.map((t) => t.id) } }));
  const morning = types.items.find((t) => t.name === "morning")!.id;
  await ok(admin.put(`/api/v1/projects/${project.id}/requirements`, {
    data: { items: [{ shift_type_id: morning, weekday: 0, headcount: 40 }] },
  }));
}

test.setTimeout(120_000);

test("a Manager generates next week, reads why places stay empty, and accepts it as drafts", async ({ page }) => {
  const admin = await apiAs("admin");
  await needsTooMany(admin);
  await admin.dispose();

  await signIn(page, "manager");
  await page.getByTestId("nav-runs").click();
  await expect(page).toHaveURL(/\/runs$/);
  await page.getByTestId("action-new-run").click();
  await page.getByTestId("date-range").getByTestId("preset-next-week").click();
  await page.getByTestId("action-generate").click();

  await expect(page.getByTestId("run-honesty-label")).toBeVisible({ timeout: 90_000 });
  await expect(page.getByTestId("unfilled").first()).toContainText(/Not available|another shift|Rest|Weekly|elsewhere|No active staff/);
  const run = page.url();
  await page.getByTestId("action-accept").click();
  // A whole accept opens its roster week, Publish one tap away (008 FR-033); the run says so.
  await expect(page.getByTestId("screen-roster")).toBeVisible({ timeout: 30_000 });
  await page.goto(run);
  await expect(page.getByTestId("run-proposal-state")).toContainText("Accepted");
  await expect(page.getByTestId("action-accept")).toHaveCount(0);
});
