/**
 * US7 — My shifts on the web build: an Admin publishes a night shift for the employee through the
 * API; the employee sees it with times at the site, ending the next day.
 */
import { expect, test } from "@playwright/test";

import { apiAs, ok, signIn } from "./support";

const DATE = "2030-07-01";
let shiftId: string;

test.beforeAll(async () => {
  const admin = await apiAs("admin");
  const employee = await apiAs("employee");
  const me = await ok<{ id: string }>(employee.get("/me"));
  await employee.dispose();

  const projects = await ok<{ items: { id: string; name: string }[] }>(admin.get("/api/v1/projects"));
  const project = projects.items.find((p) => p.name === "Night Watch");
  expect(project, "the seeded project").toBeDefined();
  const types = await ok<{ items: { id: string; name: string }[] }>(admin.get("/api/v1/shift-types"));
  await ok(admin.put(`/api/v1/projects/${project!.id}/shift-types`, { data: { ids: types.items.map((t) => t.id) } }));
  const night = types.items.find((t) => t.name === "night")!;

  const created = await ok<{ shift: { id: string } }>(
    admin.post("/api/v1/shifts", {
      data: { project_id: project!.id, shift_type_id: night.id, date: DATE, headcount: 1 },
    }),
    201,
  );
  shiftId = created.shift.id;
  await ok(admin.post(`/api/v1/shifts/${shiftId}/assignments`, { data: { user_id: me.id } }), 201);
  await ok(admin.post(`/api/v1/projects/${project!.id}/publish`, { data: { from: DATE, to: DATE } }));
  await admin.dispose();
});

test("an employee sees their published night shift at the site's times", async ({ page }) => {
  await signIn(page, "employee");
  // Sign-in lands on the role's home; My shifts is under My work (007 contracts/ui-routes.md).
  await expect(page).toHaveURL(/\/home$/);
  await page.getByTestId("nav-my-shifts").click();
  await expect(page).toHaveURL(/\/my\/shifts/);
  const card = page.getByTestId(`shift-${shiftId}`);
  await expect(card).toContainText("Mon, Jul 1");
  await expect(card).toContainText("23:00 – 07:00 (Tue, Jul 2)");
  await expect(card).toContainText("Night Watch");
  await expect(card).toContainText("Asia/Jerusalem");
});
