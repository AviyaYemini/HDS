/** Helpers for the e2e specs: signing in through the page, and setting up data through the API. */
import { expect, request, type APIRequestContext, type Page } from "@playwright/test";

/** The e2e API; the web build is exported against it (`pnpm e2e`). */
export const API = "http://localhost:8100";
export const PASSWORD = "Dev1234!";

/** Sign in through the sign-in screen as the seeded account of `role`. */
export async function signIn(page: Page, role: string, query = ""): Promise<void> {
  await page.goto(`/sign-in${query}`);
  await page.getByTestId("email").fill(`${role}@hds.test`);
  await page.getByTestId("password").fill(PASSWORD);
  await page.getByTestId("sign-in-btn").click();
}

/** An API request context signed in as the seeded account of `role`. */
export async function apiAs(role: string): Promise<APIRequestContext> {
  const anonymous = await request.newContext({ baseURL: API });
  const login = await anonymous.post("/auth/login", {
    data: { email: `${role}@hds.test`, password: PASSWORD },
  });
  expect(login.status(), await login.text()).toBe(200);
  const { access_token } = (await login.json()) as { access_token: string };
  await anonymous.dispose();
  return request.newContext({ baseURL: API, extraHTTPHeaders: { Authorization: `Bearer ${access_token}` } });
}

/** The JSON body of a call that must answer `status`. */
export async function ok<T>(answer: Promise<{ status(): number; text(): Promise<string> }>, status = 200): Promise<T> {
  const response = await answer;
  const text = await response.text();
  expect(response.status(), text).toBe(status);
  return (text ? JSON.parse(text) : undefined) as T;
}

/** Remove every submission window, then open one for `first` to `last` closing a day from now. */
export async function onlyWindow(manager: APIRequestContext, first: string, last: string): Promise<{ id: string }> {
  const { items } = await ok<{ items: { id: string }[] }>(manager.get("/api/v1/availability-windows"));
  for (const window of items) await ok(manager.delete(`/api/v1/availability-windows/${window.id}`), 204);
  const deadline = new Date(Date.now() + 24 * 3600 * 1000).toISOString();
  return ok(manager.post("/api/v1/availability-windows", { data: { starts_on: first, ends_on: last, deadline } }), 201);
}
