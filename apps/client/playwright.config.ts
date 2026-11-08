/**
 * Playwright on the exported web build, against a real API on a throwaway test database.
 *
 * `pnpm e2e` builds the web app against the e2e API and runs the specs. The API runs from `apps/api`
 * on port 8100 (8000 is left to the dev server) against `E2E_DATABASE_URL`, which must name a
 * database ending in `_test`; the app is served on port 3000, the origin the API allows. Neither
 * server is reused, so a running dev server is never written to.
 *
 * The 007 redesign's specs live in `tests/e2e/ux/` and run in two projects, a desktop and a
 * 390 × 844 phone viewport (`ux-desktop`, `ux-phone`). `E2E_EXTERNAL_API` set means the API is
 * already running (scripts/ops/e2e_ux.sh starts it on the host and runs Playwright in the pinned
 * Playwright image); then only the web build is served here, and the host did the migrations.
 */
import { defineConfig, devices } from "@playwright/test";

const DATABASE_URL =
  process.env.E2E_DATABASE_URL ?? "postgresql+asyncpg://hds:hds_dev@localhost:5432/hds_p3_test";
if (!/_test$/.test(DATABASE_URL)) {
  throw new Error("E2E_DATABASE_URL must name a test database (its name ends in _test)");
}

/** The settings the API runs with in these tests. */
const EXTERNAL_API = !!process.env.E2E_EXTERNAL_API;

export const apiEnv = {
  ENVIRONMENT: "test",
  DATABASE_URL,
  JWT_SECRET_KEY: "e2e-only-not-a-secret-0123456789-abcdefgh",
  // The lockout specs fail sign-ins on purpose; the per-network limit would then refuse the rest.
  LOGIN_RATE_LIMIT_PER_NETWORK: "0",
  CORS_ORIGINS: '["http://localhost:3000"]',
};

export default defineConfig({
  testDir: "tests/e2e",
  testMatch: "*.spec.ts",
  workers: 1,
  fullyParallel: false,
  forbidOnly: !!process.env.CI,
  retries: 0,
  reporter: process.env.CI ? [["list"], ["html", { open: "never" }]] : "list",
  globalSetup: EXTERNAL_API ? undefined : "./tests/e2e/global-setup.ts",
  use: { baseURL: "http://localhost:3000", trace: "retain-on-failure" },
  projects: [
    { name: "chromium", testIgnore: "ux/**", use: { ...devices["Desktop Chrome"] } },
    {
      name: "ux-desktop",
      testMatch: "ux/*.spec.ts",
      use: { ...devices["Desktop Chrome"], viewport: { width: 1280, height: 800 } },
    },
    {
      name: "ux-phone",
      testMatch: "ux/*.spec.ts",
      // Chromium with a phone's viewport and touch, so the specs need only the one browser.
      use: { ...devices["Desktop Chrome"], viewport: { width: 390, height: 844 }, hasTouch: true, isMobile: true },
    },
  ],
  webServer: [
    ...(EXTERNAL_API ? [] : [{
      command: "uv run uvicorn hds.main:app --port 8100",
      cwd: "../api",
      url: "http://localhost:8100/health",
      env: apiEnv,
      reuseExistingServer: false,
      timeout: 120_000,
    }]),
    {
      command: "node tests/e2e/serve.mjs",
      url: "http://localhost:3000",
      reuseExistingServer: false,
      timeout: 120_000,
    },
  ],
});
