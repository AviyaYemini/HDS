/**
 * Playwright against a running cluster's ingress (specs/006-deploy-operations T059, review C1):
 * no web server of its own, the base URL from `HDS_CLUSTER_URL`. Outside both other runners'
 * discovery (the e2e config reads tests/e2e, jest tests/unit), so neither picks it up.
 *
 * Traces are off: the journey types the Admin's password, and a trace records what is typed.
 * Screenshots on failure are kept; the password field renders masked.
 */
import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: ".",
  testMatch: "*.spec.ts",
  workers: 1,
  retries: 0,
  forbidOnly: !!process.env.CI,
  reporter: "list",
  use: {
    baseURL: process.env.HDS_CLUSTER_URL ?? "http://hds.localhost:8080",
    trace: "off",
    screenshot: "only-on-failure",
    // In a container, Chromium resolves *.localhost to its own loopback whatever /etc/hosts says:
    // HDS_CLUSTER_RESOLVE ("hds.localhost:8080 host.docker.internal:8080") maps it to the host.
    launchOptions: process.env.HDS_CLUSTER_RESOLVE
      ? {
          args: [
            `--host-resolver-rules=MAP ${process.env.HDS_CLUSTER_RESOLVE.split(" ")[0]} ${
              process.env.HDS_CLUSTER_RESOLVE.split(" ")[1]
            }`,
          ],
        }
      : {},
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
});
