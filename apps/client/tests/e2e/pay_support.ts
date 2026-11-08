/** Data for the 005 journeys: a published shift that ended a couple of hours ago, worked by the employee. */
import { type APIRequestContext } from "@playwright/test";

import { apiAs, ok } from "./support";

const ZONE = "Asia/Jerusalem";

/** The calendar date of `instant` at the seeded site. */
export function siteDate(instant: Date): string {
  return instant.toLocaleDateString("en-CA", { timeZone: ZONE });
}

/** Night Watch's id, with every shift type in use. */
async function nightWatch(admin: APIRequestContext): Promise<{ id: string; type: string }> {
  const projects = await ok<{ items: { id: string; name: string }[] }>(admin.get("/api/v1/projects"));
  const project = projects.items.find((p) => p.name === "Night Watch")!;
  const types = await ok<{ items: { id: string; name: string }[] }>(admin.get("/api/v1/shift-types"));
  await ok(admin.put(`/api/v1/projects/${project.id}/shift-types`, { data: { ids: types.items.map((t) => t.id) } }));
  return { id: project.id, type: types.items.find((t) => t.name === "morning")!.id };
}

/** A worked shift for the employee, ending two hours ago; its id and start. */
export async function workedShift(): Promise<{ shiftId: string; startsAt: Date }> {
  const admin = await apiAs("admin");
  const employee = await apiAs("employee");
  const me = await ok<{ id: string }>(employee.get("/me"));
  await employee.dispose();
  await ok(admin.patch(`/api/v1/employees/${me.id}`, { data: { default_hourly_rate: "50.0000" } }));
  const project = await nightWatch(admin);
  const end = new Date(Date.now() - 2 * 3600 * 1000);
  const start = new Date(end.getTime() - 8 * 3600 * 1000);
  const day = siteDate(start);
  const created = await ok<{ shift: { id: string } }>(
    admin.post("/api/v1/shifts", {
      data: {
        project_id: project.id,
        shift_type_id: project.type,
        date: day,
        starts_at: start.toISOString(),
        ends_at: end.toISOString(),
        headcount: 1,
      },
    }),
    201,
  );
  await ok(admin.post(`/api/v1/shifts/${created.shift.id}/assignments`, { data: { user_id: me.id } }), 201);
  await ok(admin.post(`/api/v1/projects/${project.id}/publish`, { data: { from: day, to: day } }));
  await admin.dispose();
  return { shiftId: created.shift.id, startsAt: start };
}

/** Whether a shift starting at `startsAt` belongs to an earlier month than the device's current one. */
export function inEarlierMonth(startsAt: Date): boolean {
  // Today as the report reads it, in the site's zone, not the runner's (group G review).
  return siteDate(startsAt).slice(0, 7) < siteDate(new Date()).slice(0, 7);
}
