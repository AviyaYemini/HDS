/**
 * Group D — the roster, the manager's main screen (specs/007-ux-redesign, task T062).
 *
 * Covers FR-025 to FR-041 and the owner's note (a weekly roster of shift blocks naming who filled
 * each one, flexible enough to show a half shift or two extra hours), in Hebrew and English, at
 * desktop and phone sizes. Test ids: `specs/007-ux-redesign/contracts/ui-routes.md`, "Roster test
 * ids". Each test makes its own project through the API, so runs never see each other's data.
 */
import { expect, test, type APIRequestContext, type Page } from "@playwright/test";

import { LANGUAGES, PREVIEWS_ON, addDays, apiAs, at, checkA11y, isPhone, ok, signIn, siteDay, sundayOf } from "./support";

type Project = { id: string; types: Record<string, string> };
type Shift = { id: string; starts_at: string; ends_at: string; etag: string };
type Me = { id: string };

const WEEK = addDays(sundayOf(siteDay()), 21); // three weeks ahead: only drafts and our own data

async function project(admin: APIRequestContext, name: string): Promise<Project> {
  const suffix = `${name} ${Date.now()}`;
  const owner = await ok<{ id: string }>(admin.post("/api/v1/clients", { data: { name: `${suffix} Ltd`, contract_start: siteDay(-30) } }), 201);
  const site = await ok<{ id: string }>(
    admin.post("/api/v1/sites", { data: { name: `${suffix} site`, address: "1 HQ St", timezone: "Asia/Jerusalem" } }),
    201,
  );
  const made = await ok<{ id: string }>(
    admin.post("/api/v1/projects", {
      data: { client_id: owner.id, site_id: site.id, name: suffix, type: "security", billing_rate: "75.0000", start_date: siteDay(-30) },
    }),
    201,
  );
  const types = await ok<{ items: { id: string; name: string }[] }>(admin.get("/api/v1/shift-types"));
  await ok(admin.put(`/api/v1/projects/${made.id}/shift-types`, { data: { ids: types.items.map((t) => t.id) } }));
  return { id: made.id, types: Object.fromEntries(types.items.map((t) => [t.name, t.id])) };
}

async function shift(admin: APIRequestContext, p: Project, day: string, kind: string, headcount = 1): Promise<Shift> {
  const made = await ok<{ shift: Shift }>(
    admin.post("/api/v1/shifts", { data: { project_id: p.id, shift_type_id: p.types[kind], date: day, headcount } }),
    201,
  );
  return made.shift;
}

async function assign(admin: APIRequestContext, s: Shift, who: Me, hours?: [number, number]): Promise<void> {
  const data: Record<string, string> = { user_id: who.id };
  if (hours) {
    const start = new Date(s.starts_at).getTime();
    data.starts_at = new Date(start + hours[0] * 3_600_000).toISOString();
    data.ends_at = new Date(start + hours[1] * 3_600_000).toISOString();
  }
  await ok(admin.post(`/api/v1/shifts/${s.id}/assignments`, { data }), 201);
}

function block(page: Page, id: string) {
  return page.locator(`[data-testid="roster-block"][data-shift-id="${id}"]`).first();
}

async function openRoster(page: Page, language: (typeof LANGUAGES)[number], p: Project, week = WEEK) {
  await page.goto(at("/roster", language, { project: p.id, week }));
  await expect(page.getByTestId("screen-roster")).toBeVisible();
}

for (const language of LANGUAGES) {
  test.describe(`roster in ${language}`, () => {
    let admin: APIRequestContext;
    let employee: Me;
    let extra: Me;
    let manager: Me;

    test.beforeAll(async () => {
      admin = await apiAs("admin");
      const people = await ok<{ items: { id: string; email: string }[] }>(admin.get("/api/v1/employees", { params: { search: "hds.test" } }));
      const find = (email: string) => ({ id: people.items.find((u) => u.email === email)!.id });
      employee = find("employee@hds.test");
      extra = find("extra@hds.test");
      manager = find("manager@hds.test");
    });

    test.afterAll(async () => {
      await admin.dispose();
    });

    test("draws blocks with their people, a half shift, extra hours, an overnight shift and coverage", async ({ page }, testInfo) => {
      const p = await project(admin, `Roster draw ${language}`);
      await ok(admin.put(`/api/v1/projects/${p.id}/managers`, { data: { user_ids: [manager.id] } }));
      const monday = addDays(WEEK, 1);
      const morning = await shift(admin, p, monday, "morning", 3);
      await assign(admin, morning, employee, [0, 4]); // 06:00 to 10:00
      await assign(admin, morning, extra, [0, 10]); // until 16:00, two extra hours
      const night = await shift(admin, p, addDays(WEEK, 2), "night");
      await assign(admin, night, employee);

      await signIn(page, "manager", language);
      await openRoster(page, language, p);
      if (!isPhone(testInfo)) await expect(page.getByTestId("roster-day")).toHaveCount(7);

      const b = block(page, morning.id);
      await expect(b.getByTestId("block-coverage")).toHaveText("2/3");
      await expect(b.getByTestId("empty-seat")).toHaveCount(1);
      const blockBox = (await b.boundingBox())!;
      const half = (await b.locator(`[data-testid="block-person"][data-user-id="${employee.id}"] [data-testid="person-bar"]`).boundingBox())!;
      const long = (await b.locator(`[data-testid="block-person"][data-user-id="${extra.id}"] [data-testid="person-bar"]`).boundingBox())!;
      if (!isPhone(testInfo)) {
        expect(half.width).toBeLessThan(blockBox.width * 0.7); // 4 of 8 hours
        expect(long.width).toBeGreaterThan(blockBox.width); // 10 of 8 hours: past the edge
      }
      await expect(b.locator(`[data-testid="block-person"][data-user-id="${employee.id}"]`)).toHaveAttribute("data-own-times", "true");

      const n = block(page, night.id);
      await expect(n).toContainText("+1");
      if (!isPhone(testInfo)) {
        await expect(page.locator(`[data-testid="roster-continuation"][data-shift-id="${night.id}"]`)).toBeVisible();
      }
      await expect(b).toHaveAttribute("data-status", "draft");
      if (PREVIEWS_ON) await expect(page.getByTestId("preview-cost-strip")).toBeVisible();
      await checkA11y(page, `roster (${language})`);
    });

    test("assigns by tapping in three taps or fewer, warns with the rule and numbers, and takes an override", async ({ page }) => {
      const p = await project(admin, `Roster assign ${language}`);
      await ok(admin.put(`/api/v1/projects/${p.id}/managers`, { data: { user_ids: [manager.id] } }));
      const night = await shift(admin, p, addDays(WEEK, 1), "night");
      await assign(admin, night, employee); // Monday 22:00 to Tuesday 06:00
      const morning = await shift(admin, p, addDays(WEEK, 2), "morning"); // Tuesday 06:00: no rest

      await signIn(page, "manager", language);
      await openRoster(page, language, p);
      let taps = 0;
      await block(page, morning.id).getByTestId("empty-seat").first().click();
      taps += 1;
      const sheet = page.getByTestId("assign-sheet");
      await expect(sheet).toBeVisible();
      await sheet.locator(`[data-testid="assign-option"][data-user-id="${employee.id}"]`).click();
      taps += 1;
      expect(taps).toBeLessThanOrEqual(3);
      const b = block(page, morning.id);
      await expect(b.locator(`[data-testid="block-person"][data-user-id="${employee.id}"]`)).toBeVisible();

      const badge = b.getByTestId("block-warning-badge");
      await expect(badge).toBeVisible();
      await badge.click();
      const detail = page.getByTestId("warning-detail");
      await expect(detail).toContainText("11"); // the rule's hours
      await expect(detail).toContainText(/0|00:00/); // the rest actually scheduled
      await page.getByTestId("action-override").first().click();
      await page.getByTestId("override-reason").fill(language === "he" ? "החלפה מוסכמת" : "Agreed handover");
      await page.getByTestId("action-confirm-override").click();
      await expect(detail).not.toBeVisible();
      await checkA11y(page, `roster after assigning (${language})`);
    });

    test("assigning someone who missed the availability deadline shows the not-submitted warning", async ({ page }) => {
      const p = await project(admin, `Roster missed ${language}`);
      await ok(admin.put(`/api/v1/projects/${p.id}/managers`, { data: { user_ids: [manager.id] } }));
      const first = addDays(WEEK, language === "he" ? 7 : 14);
      const windows = await ok<{ items: { id: string }[] }>(admin.get("/api/v1/availability-windows", { params: { from: first, to: addDays(first, 6) } }));
      for (const w of windows.items) await ok(admin.delete(`/api/v1/availability-windows/${w.id}`), 204);
      await ok(admin.post("/api/v1/availability-windows", {
        data: { starts_on: first, ends_on: addDays(first, 6), deadline: new Date(Date.now() - 3_600_000).toISOString() },
      }), 201);
      const s = await shift(admin, p, addDays(first, 1), "morning");

      await signIn(page, "manager", language);
      await openRoster(page, language, p, first);
      await block(page, s.id).getByTestId("empty-seat").first().click();
      await page.getByTestId("assign-sheet").locator(`[data-testid="assign-option"][data-user-id="${extra.id}"]`).click();
      await block(page, s.id).getByTestId("block-warning-badge").click();
      await expect(page.getByTestId("warning-detail")).toContainText(
        language === "he" ? "לא הוגשה" : "did not submit",
      );
    });

    test("publishes through a review whose counts match, and undo steps back", async ({ page }) => {
      const p = await project(admin, `Roster publish ${language}`);
      await ok(admin.put(`/api/v1/projects/${p.id}/managers`, { data: { user_ids: [manager.id] } }));
      const a = await shift(admin, p, addDays(WEEK, 1), "morning", 2);
      await assign(admin, a, employee);
      const b = await shift(admin, p, addDays(WEEK, 3), "afternoon");
      await assign(admin, b, extra);

      await signIn(page, "manager", language);
      await openRoster(page, language, p);
      const before = await page.locator('[data-testid="block-person"]').count();
      await block(page, b.id).getByTestId("block-person").first().click();
      await page.getByTestId("action-unassign").click();
      await expect(page.locator('[data-testid="block-person"]')).toHaveCount(before - 1);
      await page.getByTestId("action-undo-roster").click();
      await expect(page.locator('[data-testid="block-person"]')).toHaveCount(before);

      await page.getByTestId("action-publish").click();
      const review = page.getByTestId("publish-review");
      await expect(review).toBeVisible();
      await expect(review.getByTestId("review-shifts")).toContainText("2");
      await expect(review.getByTestId("review-people")).toContainText("2");
      await expect(review.getByTestId("review-unfilled")).toContainText("1");
      await review.getByTestId("publish-silently").click();
      await review.getByTestId("action-confirm-publish").click();
      await expect(block(page, a.id)).toHaveAttribute("data-status", "published");
      await checkA11y(page, `roster published (${language})`);
    });

    test("a manager changes only their own projects, and a stale save is told", async ({ page }) => {
      const mine = await project(admin, `Roster mine ${language}`);
      await ok(admin.put(`/api/v1/projects/${mine.id}/managers`, { data: { user_ids: [manager.id] } }));
      const theirs = await project(admin, `Roster theirs ${language}`);
      const other = await shift(admin, theirs, addDays(WEEK, 1), "morning");
      const own = await shift(admin, mine, addDays(WEEK, 1), "morning", 2);

      await signIn(page, "manager", language);
      await openRoster(page, language, theirs);
      await expect(block(page, other.id)).toHaveAttribute("data-editable", "false");
      await expect(block(page, other.id).getByTestId("empty-seat")).toHaveCount(1);
      await block(page, other.id).click();
      await expect(page.getByTestId("assign-sheet")).toHaveCount(0);

      await openRoster(page, language, mine);
      await expect(block(page, own.id)).toHaveAttribute("data-editable", "true");
      // Someone else changes the shift after the page read it.
      await ok(admin.patch(`/api/v1/shifts/${own.id}`, { data: { headcount: 3 } }));
      await block(page, own.id).getByTestId("empty-seat").first().click();
      await page.getByTestId("assign-sheet").locator(`[data-testid="assign-option"][data-user-id="${employee.id}"]`).click();
      await expect(page.getByTestId("banner-roster-changed")).toBeVisible();
      await expect(block(page, own.id).getByTestId("block-coverage")).toHaveText(/\/3$/);
    });

    test("the submission tracker lists who has and hasn't submitted", async ({ page }) => {
      const first = addDays(WEEK, language === "he" ? 28 : 35);
      const windows = await ok<{ items: { id: string }[] }>(admin.get("/api/v1/availability-windows", { params: { from: first, to: addDays(first, 6) } }));
      for (const w of windows.items) await ok(admin.delete(`/api/v1/availability-windows/${w.id}`), 204);
      const window = await ok<{ id: string }>(admin.post("/api/v1/availability-windows", {
        data: { starts_on: first, ends_on: addDays(first, 6), deadline: new Date(Date.now() + 86_400_000).toISOString() },
      }), 201);
      const asEmployee = await apiAs("employee");
      await ok(asEmployee.post("/me/availability-submissions", { data: { window_id: window.id } }));
      await asEmployee.dispose();

      await signIn(page, "manager", language);
      await page.goto(at("/roster/submissions", language, { window: window.id }));
      await expect(page.getByTestId("screen-submissions")).toBeVisible();
      await expect(page.locator(`[data-testid="submission-row"][data-user-id="${employee.id}"]`)).toHaveAttribute("data-status", "submitted");
      await expect(page.locator(`[data-testid="submission-row"][data-user-id="${extra.id}"]`)).toHaveAttribute("data-status", "not_submitted");
      await checkA11y(page, `submissions (${language})`);
    });

    test("on a phone, a day agenda assigns and publishes", async ({ page }, testInfo) => {
      test.skip(!isPhone(testInfo), "the phone agenda");
      const p = await project(admin, `Roster agenda ${language}`);
      await ok(admin.put(`/api/v1/projects/${p.id}/managers`, { data: { user_ids: [manager.id] } }));
      const day = addDays(WEEK, 4);
      const s = await shift(admin, p, day, "afternoon");
      await signIn(page, "manager", language);
      await page.goto(at("/roster", language, { project: p.id, week: WEEK, day }));
      await expect(page.getByTestId("roster-agenda")).toBeVisible();
      await block(page, s.id).getByTestId("empty-seat").first().click();
      await page.getByTestId("assign-sheet").locator(`[data-testid="assign-option"][data-user-id="${employee.id}"]`).click();
      await page.getByTestId("action-publish").click();
      await page.getByTestId("publish-review").getByTestId("action-confirm-publish").click();
      await expect(block(page, s.id)).toHaveAttribute("data-status", "published");
    });
  });
}
