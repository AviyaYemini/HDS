/**
 * Group O — the owner's requests of 2026-11-08 (specs/007-ux-redesign FR-078 to FR-081, T104).
 *
 * The language switch restarts the app in the new language: at once a "Switching to …" state in
 * that language, then the same screen, address and query, laid out in its direction, still signed
 * in; never the home screen. From the top bar, from Account and before signing in.
 */
import { expect, test, type APIRequestContext, type Page } from "@playwright/test";

import { addDays, apiAs, at, checkA11y, ok, openAs, siteDay, sundayOf, type Language } from "./support";

const SEEN = "e2e.switching-seen";

/**
 * Record the switching state's text in this tab's storage the moment it shows: the page reloads a
 * few frames later, faster than an assertion could look, and the record survives the reload.
 */
async function watchSwitching(page: Page): Promise<void> {
  await page.addInitScript((key) => {
    const look = () => {
      const node = document.querySelector('[data-testid="language-switching"]');
      if (node && !sessionStorage.getItem(key)) {
        sessionStorage.setItem(key, `${node.getAttribute("lang")}|${node.textContent ?? ""}`);
      }
    };
    new MutationObserver(look).observe(document, { childList: true, subtree: true });
  }, SEEN);
}

async function seenSwitching(page: Page): Promise<string | null> {
  return page.evaluate((key) => sessionStorage.getItem(key), SEEN);
}

const OTHER: Record<Language, Language> = { he: "en", en: "he" };
const SWITCHING: Record<Language, RegExp> = { en: /Switching to English/, he: /עוברים לעברית/ };

async function landedIn(page: Page, language: Language, path: string, query: Record<string, string>): Promise<void> {
  await expect(page.locator("html")).toHaveAttribute("dir", language === "he" ? "rtl" : "ltr");
  await expect(page.locator("html")).toHaveAttribute("lang", language);
  const url = new URL(page.url());
  expect(url.pathname).toBe(path);
  for (const [key, value] of Object.entries(query)) expect(url.searchParams.get(key)).toBe(value);
  expect(url.searchParams.has("lang")).toBe(false);
  await expect(page.getByTestId("language-switching")).toHaveCount(0);
}

for (const from of ["he", "en"] as const) {
  const to = OTHER[from];

  test(`${from} to ${to} from the top bar, on a deep address, back on it signed in`, async ({ page }) => {
    await watchSwitching(page);
    await openAs(page, "employee", from, "/my/shifts", { view: "past" });
    await expect(page.getByTestId("screen-my-shifts")).toBeVisible();
    await page.getByTestId("action-switch-language").first().click();

    await expect(page.getByTestId("screen-my-shifts")).toBeVisible();
    await landedIn(page, to, "/my/shifts", { view: "past" });
    const seen = await seenSwitching(page);
    expect(seen, "the switching state showed").not.toBeNull();
    expect(seen!).toMatch(new RegExp(`^${to}\\|`));
    expect(seen!).toMatch(SWITCHING[to]);
  });

  test(`${from} to ${to} from Account, back on Account signed in`, async ({ page }) => {
    await watchSwitching(page);
    await openAs(page, "manager", from, "/account");
    await expect(page.getByTestId("screen-account")).toBeVisible();
    await page.getByTestId(`language-${to}`).click();

    await expect(page.getByTestId("screen-account")).toBeVisible();
    await landedIn(page, to, "/account", {});
    expect(await seenSwitching(page)).toMatch(SWITCHING[to]);
  });

  test(`${from} to ${to} before signing in, still on sign-in`, async ({ page }) => {
    await watchSwitching(page);
    await page.goto(at("/sign-in", from));
    await expect(page.getByTestId("screen-sign-in")).toBeVisible();
    await page.getByTestId("action-switch-language").first().click();

    await expect(page.getByTestId("screen-sign-in")).toBeVisible();
    await landedIn(page, to, "/sign-in", {});
    expect(await seenSwitching(page)).toMatch(SWITCHING[to]);
  });
}

// ── A run's output (FR-080, T106) ───────────────────────────────────────────

type Job = {
  id: string;
  status: string;
  proposal_state: string | null;
  proposal: {
    slots: { key: string; project_id: string; date: string; assigned: string[] }[];
    unfilled: { key: string; places: number }[];
    no_availability: string[];
  } | null;
};

/** Night Watch, managed by the seeded Manager, needs more morning people than it has. */
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

/** Ask for next week as the Manager and wait for the proposal. */
async function solvedRun(manager: APIRequestContext, project: string): Promise<Job> {
  const next = addDays(sundayOf(siteDay()), 7);
  const job = await ok<Job>(manager.post("/api/v1/schedule-jobs", { data: { from: next, to: addDays(next, 6), project_ids: [project], solver: "cpsat" } }), 202);
  for (let i = 0; i < 120; i += 1) {
    const read = await ok<Job>(manager.get(`/api/v1/schedule-jobs/${job.id}`));
    if (read.status === "succeeded") return read;
    expect(read.status).not.toBe("failed");
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error("the run did not finish");
}

test.describe("a run's output", () => {
  test.setTimeout(120_000);

  for (const language of ["he", "en"] as const) {
    test(`${language}: an accepted run shows who works which shift, by name, and opens in the roster`, async ({ page }) => {
      const admin = await apiAs("admin");
      const project = await nightWatchNeeds(admin);
      const manager = await apiAs("manager");
      const job = await solvedRun(manager, project);
      const slot = job.proposal!.slots.find((s) => s.assigned.length > 0)!;
      const people = await ok<{ items: { id: string; full_name: string }[] }>(admin.get("/api/v1/employees"));
      const named = people.items.find((p) => p.id === slot.assigned[0])!.full_name;
      await ok(manager.post(`/api/v1/schedule-jobs/${job.id}/accept`));

      await openAs(page, "manager", language, `/runs/${job.id}`);
      await expect(page.getByTestId("screen-run")).toBeVisible();
      await expect(page.getByTestId("proposal-week").first()).toBeVisible();
      const block = page.locator(`[data-testid="proposal-block"][data-slot-key="${slot.key}"]`);
      await expect(block).toContainText(named);
      if (job.proposal!.unfilled.length > 0) await expect(page.getByTestId("unfilled").first()).toBeVisible();
      for (const id of job.proposal!.no_availability.slice(0, 3)) {
        const name = people.items.find((p) => p.id === id)!.full_name;
        await expect(page.getByTestId("no-availability")).toContainText(name);
      }
      const open = page.getByTestId("action-open-in-roster");
      // The run's first week: it starts on next Sunday.
      await expect(open).toHaveAttribute("href", `/roster?project=${project}&week=${addDays(sundayOf(siteDay()), 7)}`);
      await checkA11y(page, `accepted run (${language})`);
      await admin.dispose();
      await manager.dispose();
    });
  }

  test("a discarded run shows the same view, labelled discarded", async ({ page }) => {
    const admin = await apiAs("admin");
    const project = await nightWatchNeeds(admin);
    const manager = await apiAs("manager");
    const job = await solvedRun(manager, project);
    await ok(manager.post(`/api/v1/schedule-jobs/${job.id}/discard`));

    await openAs(page, "manager", "en", `/runs/${job.id}`);
    await expect(page.getByTestId("run-proposal-state")).toContainText("Discarded");
    await expect(page.getByTestId("proposal-week").first()).toBeVisible();
    await expect(page.getByTestId("action-open-in-roster")).toHaveCount(0);
    await admin.dispose();
    await manager.dispose();
  });
});

// ── Runs for chosen projects (FR-081, T108) ─────────────────────────────────

test.describe("runs for chosen projects", () => {
  test.setTimeout(120_000);

  test("a manager is offered only the projects they manage, each with its type", async ({ page }) => {
    // Other specs give the seeded Manager more projects: what they manage is what /me says.
    const manager = await apiAs("manager");
    const me = await ok<{ managed_project_ids: string[] }>(manager.get("/me"));
    const projects = await ok<{ items: { id: string; status: string }[] }>(manager.get("/api/v1/projects"));
    const offered = projects.items.filter((p) => p.status === "active" && me.managed_project_ids.includes(p.id));
    await manager.dispose();

    await openAs(page, "manager", "en", "/runs");
    await page.getByTestId("action-new-run").click();
    await page.getByTestId("run-projects").click();
    const sheet = page.getByTestId("run-projects-sheet");
    await expect(sheet.getByRole("checkbox")).toHaveCount(offered.length);
    for (const project of offered) await expect(sheet.getByTestId(`run-projects-option-${project.id}`)).toBeVisible();
    await expect(sheet.getByRole("checkbox").first()).toContainText(/Security|Training/);
  });

  test("an admin generates for one project, and the proposal holds only its slots", async ({ page }) => {
    const admin = await apiAs("admin");
    const projects = await ok<{ items: { id: string; name: string }[] }>(admin.get("/api/v1/projects"));
    const night = projects.items.find((p) => p.name === "Night Watch")!.id;

    await openAs(page, "admin", "en", "/runs");
    await page.getByTestId("action-new-run").click();
    await page.getByTestId("run-projects").click();
    const sheet = page.getByTestId("run-projects-sheet");
    // Leave only Night Watch chosen, whatever the default (one type: all; both: none).
    for (const project of projects.items) {
      const option = sheet.getByTestId(`run-projects-option-${project.id}`);
      if ((await option.count()) === 0) continue;
      const checked = (await option.getAttribute("aria-checked")) === "true";
      if (checked !== (project.id === night)) await option.click();
    }
    await sheet.getByTestId("run-projects-done").click();
    await page.getByTestId("date-range").getByTestId("preset-next-week").click();
    await page.getByTestId("action-generate").click();

    await expect(page.getByTestId("screen-run")).toBeVisible();
    await expect(page.getByTestId("run-projects-covered")).toContainText("Night Watch");
    const id = new URL(page.url()).pathname.split("/").pop()!;
    let job: { status: string; project_ids: string[]; proposal: { slots: { project_id: string }[] } | null } | null = null;
    for (let i = 0; i < 120; i += 1) {
      job = await ok(admin.get(`/api/v1/schedule-jobs/${id}`));
      if (job!.status === "succeeded") break;
      await new Promise((resolve) => setTimeout(resolve, 500));
    }
    expect(job!.project_ids).toEqual([night]);
    expect(new Set(job!.proposal!.slots.map((s) => s.project_id))).toEqual(new Set([night]));
    await admin.dispose();
  });
});
