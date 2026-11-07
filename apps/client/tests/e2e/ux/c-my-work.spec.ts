/**
 * Group C — employee: My work (specs/007-ux-redesign, task T047).
 *
 * Covers FR-013 to FR-024 and the owner's notes (an interactive availability week with an explicit
 * Submit; My shifts split into upcoming and past), in Hebrew and English, at desktop and phone
 * sizes. Test ids: `specs/007-ux-redesign/contracts/ui-routes.md`, "My work test ids". Data is set
 * up through the API; the e2e database persists between runs, so each test clears what it uses.
 */
import { expect, test, type APIRequestContext, type Page } from "@playwright/test";

import {
  LANGUAGES,
  PREVIEWS_ON,
  type Language,
  addDays,
  apiAs,
  at,
  checkA11y,
  ok,
  signIn,
  siteDay,
  sundayOf,
} from "./support";

type Window = { id: string; starts_on: string; ends_on: string };
type Me = { id: string };

/** A week `weeksAhead` weeks after this one, per language, so the two never share a window. */
function weekFor(language: Language, offset: number): { first: string; last: string } {
  const base = language === "he" ? 2 : 5;
  const first = addDays(sundayOf(siteDay()), 7 * (base + offset));
  return { first, last: addDays(first, 6) };
}

async function clearAvailability(admin: APIRequestContext, employee: Me, first: string, last: string): Promise<void> {
  const { items } = await ok<{ items: { id: string }[] }>(
    admin.get(`/api/v1/employees/${employee.id}/constraints`, { params: { from: first, to: last } }),
  );
  for (const entry of items) await ok(admin.delete(`/api/v1/constraints/${entry.id}`), 204);
  const windows = await ok<{ items: Window[] }>(admin.get("/api/v1/availability-windows", { params: { from: first, to: last } }));
  for (const window of windows.items) await ok(admin.delete(`/api/v1/availability-windows/${window.id}`), 204);
}

async function openWindow(manager: APIRequestContext, first: string, last: string, hoursToDeadline: number): Promise<Window> {
  const deadline = new Date(Date.now() + hoursToDeadline * 3600_000).toISOString();
  return ok<Window>(manager.post("/api/v1/availability-windows", { data: { starts_on: first, ends_on: last, deadline } }), 201);
}

async function banner(page: Page) {
  const found = page.getByTestId("availability-banner");
  await expect(found).toBeVisible();
  return found;
}

function cell(page: Page, day: string) {
  return page.locator(`[data-testid="availability-cell"][data-date="${day}"]:not([data-shift-type="all-day"])`).first();
}

for (const language of LANGUAGES) {
  test.describe(`My work in ${language}`, () => {
    let admin: APIRequestContext;
    let manager: APIRequestContext;
    let me: Me;

    test.beforeAll(async () => {
      admin = await apiAs("admin");
      manager = await apiAs("manager");
      const employee = await apiAs("employee");
      me = await ok<Me>(employee.get("/me"));
      await employee.dispose();
    });

    test.afterAll(async () => {
      await admin.dispose();
      await manager.dispose();
    });

    test("the availability week: cycle, undo, review, submit, change, lock", async ({ page }) => {
      const { first, last } = weekFor(language, 0);
      await clearAvailability(admin, me, addDays(first, -7), last);
      const window = await openWindow(manager, first, last, 24);
      // Last week's mark, for Copy last week (an Admin may enter one outside any window).
      const monday = addDays(first, 1);
      await ok(admin.post(`/api/v1/employees/${me.id}/constraints`, {
        data: { kind: "open", starts_on: addDays(monday, -7), ends_on: addDays(monday, -7) },
      }), 201);

      await signIn(page, "employee", language);
      await page.goto(at("/my/availability", language, { window: window.id }));
      await expect(page.getByTestId("screen-availability")).toBeVisible();
      await expect(page.getByTestId("availability-rules")).toBeVisible();
      await expect(await banner(page)).toHaveAttribute("data-status", "not_submitted");
      await expect(page.getByTestId("availability-not-submitted-warning")).toBeVisible();
      await checkA11y(page, `availability (${language})`);

      const tuesday = cell(page, addDays(first, 2));
      await expect(tuesday).toHaveAttribute("data-state", "unmarked");
      await tuesday.click();
      await expect(tuesday).toHaveAttribute("data-state", "open");
      await expect(await banner(page)).toHaveAttribute("data-status", "draft");
      await tuesday.click();
      await expect(tuesday).toHaveAttribute("data-state", "close");
      await page.getByTestId("action-undo").click();
      await expect(tuesday).toHaveAttribute("data-state", "open");
      await tuesday.click();
      await tuesday.click();
      await expect(tuesday).toHaveAttribute("data-state", "blocked");
      await tuesday.click();
      await expect(tuesday).toHaveAttribute("data-state", "unmarked");
      await tuesday.click();

      await page.getByTestId("action-copy-last-week").click();
      await expect(cell(page, monday)).toHaveAttribute("data-state", "open");

      await page.getByTestId("action-review").click();
      const review = page.getByTestId("availability-review");
      await expect(review).toBeVisible();
      await expect(review.getByTestId("review-row")).toHaveCount(2);
      await review.getByTestId("action-submit").click();
      await expect(await banner(page)).toHaveAttribute("data-status", "submitted");
      const employee = await apiAs("employee");
      const stored = await ok<{ status: string }>(
        employee.get("/me/availability-submissions", { params: { window_id: window.id } }),
      );
      await employee.dispose();
      expect(stored.status).toBe("submitted");

      await tuesday.click();
      await expect(await banner(page)).toHaveAttribute("data-status", "draft");

      await ok(manager.patch(`/api/v1/availability-windows/${window.id}`, {
        data: { deadline: new Date(Date.now() - 60_000).toISOString() },
      }));
      await page.reload();
      const locked = await banner(page);
      await expect(locked).toHaveAttribute("data-status", "locked");
      await expect(locked).toHaveAttribute("data-counted-unavailable", "false");
      await expect(tuesday).toBeDisabled();
    });

    test("a missed deadline shows as counted unavailable", async ({ page }) => {
      const { first, last } = weekFor(language, 1);
      await clearAvailability(admin, me, first, last);
      const window = await openWindow(manager, first, last, -1);
      await signIn(page, "employee", language);
      await page.goto(at("/my/availability", language, { window: window.id }));
      const locked = await banner(page);
      await expect(locked).toHaveAttribute("data-status", "locked");
      await expect(locked).toHaveAttribute("data-counted-unavailable", "true");
      await checkA11y(page, `availability locked (${language})`);
    });

    test("My shifts: upcoming and past apart, the next shift first, overnight with +1", async ({ page }) => {
      const projects = await ok<{ items: { id: string; name: string }[] }>(admin.get("/api/v1/projects"));
      const project = projects.items.find((p) => p.name === "Night Watch")!;
      const types = await ok<{ items: { id: string; name: string }[] }>(admin.get("/api/v1/shift-types"));
      await ok(admin.put(`/api/v1/projects/${project.id}/shift-types`, { data: { ids: types.items.map((t) => t.id) } }));
      const typeId = (name: string) => types.items.find((t) => t.name === name)!.id;

      async function published(day: string, type: string): Promise<string> {
        const created = await ok<{ shift: { id: string } }>(
          admin.post("/api/v1/shifts", { data: { project_id: project.id, shift_type_id: typeId(type), date: day, headcount: 1 } }),
          201,
        );
        await ok(admin.post(`/api/v1/shifts/${created.shift.id}/assignments`, { data: { user_id: me.id } }), 201);
        await ok(admin.post(`/api/v1/projects/${project.id}/publish`, { data: { from: day, to: day } }));
        return created.shift.id;
      }
      const soon = await published(siteDay(language === "he" ? 2 : 3), "morning");
      const overnight = await published(siteDay(language === "he" ? 4 : 5), "night");
      const before = await published(siteDay(language === "he" ? -3 : -4), "morning");

      await signIn(page, "employee", language);
      await page.goto(at("/my/shifts", language));
      await expect(page.getByTestId("screen-my-shifts")).toBeVisible();
      await expect(page.getByTestId("view-upcoming")).toHaveAttribute("aria-checked", "true");
      const card = page.getByTestId("next-shift-card");
      await expect(card).toBeVisible();
      const firstGroup = page.getByTestId("week-group").first();
      expect((await card.boundingBox())!.y).toBeLessThan((await firstGroup.boundingBox())!.y);
      await expect(page.locator(`[data-testid="shift-row"][data-shift-id="${soon}"]`)).toBeVisible();
      await expect(page.locator(`[data-testid="shift-row"][data-shift-id="${before}"]`)).toHaveCount(0);
      const night = page.locator(`[data-testid="shift-row"][data-shift-id="${overnight}"]`);
      await expect(night.getByTestId("shift-time")).toContainText("+1");
      if (PREVIEWS_ON) await expect(page.getByTestId("preview-report-hours").first()).toBeVisible();
      await checkA11y(page, `my shifts upcoming (${language})`);

      await page.getByTestId("view-past").click();
      await expect(page.locator(`[data-testid="period-group"] [data-testid="shift-row"][data-shift-id="${before}"]`)).toBeVisible();
      await expect(page.locator(`[data-testid="shift-row"][data-shift-id="${soon}"]`)).toHaveCount(0);
      await checkA11y(page, `my shifts past (${language})`);

      await page.getByTestId("view-upcoming").click();
      await page.locator(`[data-testid="shift-row"][data-shift-id="${soon}"]`).click();
      const detail = page.getByTestId("screen-shift-detail");
      await expect(detail).toBeVisible();
      await expect(detail).toContainText("Rothschild");
      await expect(page.getByTestId("action-add-to-calendar")).toBeVisible();
      await checkA11y(page, `shift detail (${language})`);
    });

    test("My pay: the breakdown by rate adds up exactly to the total, labelled an estimate", async ({ page }) => {
      await ok(admin.patch(`/api/v1/employees/${me.id}`, { data: { default_hourly_rate: "50.0000" } }));
      await signIn(page, "employee", language);
      await page.goto(at("/my/pay", language));
      await expect(page.getByTestId("screen-my-pay")).toBeVisible();
      await page.getByTestId("view-pay").click();
      await expect(page.getByTestId("pay-estimate-label")).toBeVisible();
      const total = await page.getByTestId("pay-total").getAttribute("data-amount");
      const parts = await page.getByTestId("pay-breakdown-row").evaluateAll((rows) =>
        rows.map((r) => r.getAttribute("data-amount") ?? "0"),
      );
      // Exact decimal sums: scale to ten-thousandths as integers, never floating point.
      const units = (amount: string): bigint => {
        const [whole = "0", fraction = ""] = amount.split(".");
        return BigInt(whole) * 10_000n + BigInt((fraction + "0000").slice(0, 4));
      };
      expect(parts.reduce((sum, part) => sum + units(part), 0n)).toBe(units(total ?? "0"));
      await checkA11y(page, `my pay (${language})`);

      await page.getByTestId("view-hours").click();
      if (PREVIEWS_ON) await expect(page.getByTestId("preview-approved-hours")).toBeVisible();
    });
  });
}
