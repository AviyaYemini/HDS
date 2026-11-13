/**
 * The upgraded cluster through its ingress (specs/006-deploy-operations T059, review C1): the first
 * Admin signs in on the web build, moves through the main screens with the app's own navigation,
 * and signs out; a fresh load then shows sign-in. The Admin is made by the rolling-update job, which
 * passes its address and password in `HDS_CLUSTER_EMAIL` and `HDS_CLUSTER_PASSWORD`; neither is
 * written down.
 *
 * The navigation is 007's staff shell (specs/007-ux-redesign FR-003): since the CEO demo an Admin
 * lands on the snapshot dashboard, /manage/overview, and the desktop's side navigation
 * (`nav-<destination>`) leads to My work's screens under /my.
 *
 * Screens are reached by clicking, as a person moves through the app; /me and /auth are the API's
 * paths on the same host (the ingress's apiPrefixes). The web keeps its tokens per tab (ADR 019), so
 * a reload in the same tab stays signed in, but signing out clears the tab, so the fresh load after
 * it shows sign-in.
 */
import { expect, test } from "@playwright/test";

const email = process.env.HDS_CLUSTER_EMAIL ?? "";
const password = process.env.HDS_CLUSTER_PASSWORD ?? "";

test("the first Admin signs in, opens the main screens and signs out", async ({ page }) => {
  expect(email, "HDS_CLUSTER_EMAIL is set").not.toBe("");
  expect(password, "HDS_CLUSTER_PASSWORD is set").not.toBe("");
  await page.goto("/sign-in");
  await page.fill('[data-testid="email"]', email);
  await page.fill('[data-testid="password"]', password);
  await page.click('[data-testid="sign-in-btn"]');
  await expect(page).toHaveURL(/\/manage\/overview$/);
  const nav = page.getByTestId("nav-side");
  for (const [destination, path] of [
    ["my-shifts", /\/my\/shifts$/],
    ["pay", /\/my\/pay$/],
    ["availability", /\/my\/availability$/],
  ] as const) {
    await nav.getByTestId(`nav-${destination}`).click();
    await expect(page).toHaveURL(path);
  }
  await page.getByTestId("action-sign-out").click();
  await expect(page).toHaveURL(/\/sign-in/);
  await page.goto("/my/availability");
  await expect(page).toHaveURL(/\/sign-in/);
});
