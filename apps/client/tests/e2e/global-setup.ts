/**
 * Before the specs: bring the test database to the latest migration and seed the dev accounts
 * (`hds.scripts.seed_dev` is idempotent). Everything else is set up through the API by the specs.
 */
import { execFileSync } from "node:child_process";
import { join } from "node:path";

import { apiEnv } from "../../playwright.config";

export default function globalSetup(): void {
  const api = join(__dirname, "../../../api");
  const env = { ...process.env, ...apiEnv };
  execFileSync("uv", ["run", "alembic", "upgrade", "head"], { cwd: api, env, stdio: "inherit" });
  execFileSync("uv", ["run", "python", "-m", "hds.scripts.seed_dev"], { cwd: api, env, stdio: "inherit" });
}
