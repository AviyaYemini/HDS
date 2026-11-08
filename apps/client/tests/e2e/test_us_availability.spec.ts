/**
 * US4 — My availability on the web build: an employee adds, changes and removes an entry inside the
 * open window, and sees the API's refusal for dates outside it. The window is opened through the API.
 */
import { expect, test, type APIRequestContext } from "@playwright/test";

import { apiAs, ok, onlyWindow, signIn } from "./support";

test.describe.configure({ mode: "serial" });

const FIRST = "2030-06-02"; // a Sunday
const LAST = "2030-06-08";
const DAY = "2030-06-03";

let employee: APIRequestContext;
let entriesPath: string;
let windowId: string;

interface Entry {
  id: string;
  kind: string;
}

async function entriesOn(day: string): Promise<Entry[]> {
  const { items } = await ok<{ items: Entry[] }>(employee.get(`${entriesPath}?from=${day}&to=${day}`));
  return items;
}

test.beforeAll(async () => {
  const manager = await apiAs("manager");
  windowId = (await onlyWindow(manager, FIRST, LAST)).id;
  await manager.dispose();
  employee = await apiAs("employee");
  const me = await ok<{ id: string }>(employee.get("/me"));
  entriesPath = `/api/v1/employees/${me.id}/constraints`;
  const { items } = await ok<{ items: Entry[] }>(employee.get(`${entriesPath}?from=${FIRST}&to=${LAST}`));
  for (const entry of items) await ok(employee.delete(`/api/v1/constraints/${entry.id}`), 204);
});

test.afterAll(async () => {
  await employee.dispose();
});

test("an employee adds, changes and removes an entry in the open window", async ({ page }) => {
  await signIn(page, "employee");
  // Sign-in lands on the role's home; availability is under My work (007 contracts/ui-routes.md).
  await expect(page).toHaveURL(/\/home$/);
  await page.getByTestId("nav-availability").click();
  await expect(page).toHaveURL(/\/my\/availability/);
  await expect(page.getByTestId(`window-${windowId}`)).toContainText("Jun 2");

  await page.getByTestId("add-entry").click();
  await page.getByTestId("kind-blocked").click();
  await page.getByTestId("first-day").fill(DAY);
  await page.getByTestId("last-day").fill(DAY);
  await page.getByTestId("save-entry").click();
  await expect(page.getByTestId("entry-form")).toHaveCount(0);
  const [added] = await entriesOn(DAY);
  expect(added?.kind).toBe("blocked");
  await expect(page.getByTestId(`entry-${added!.id}`)).toContainText("Can't work");

  await page.getByTestId(`edit-${added!.id}`).click();
  await page.getByTestId("kind-close").click();
  await page.getByTestId("save-entry").click();
  await expect(page.getByTestId(`entry-${added!.id}`)).toContainText("Would rather not work");
  expect((await entriesOn(DAY))[0]?.kind).toBe("close");

  await page.getByTestId(`remove-${added!.id}`).click();
  await expect(page.getByTestId(`entry-${added!.id}`)).toHaveCount(0);
  expect(await entriesOn(DAY)).toEqual([]);
});

test("dates outside the open window are refused with the API's reason", async ({ page }) => {
  await signIn(page, "employee");
  await expect(page).toHaveURL(/\/home$/);
  await page.getByTestId("nav-availability").click();
  await expect(page).toHaveURL(/\/my\/availability/);
  await page.getByTestId("add-entry").click();
  await page.getByTestId("kind-blocked").click();
  await page.getByTestId("first-day").fill("2031-01-05");
  await page.getByTestId("last-day").fill("2031-01-05");
  await page.getByTestId("save-entry").click();
  await expect(page.getByTestId("error-message")).toContainText("open submission window");
  expect(await entriesOn("2031-01-05")).toEqual([]);
});
