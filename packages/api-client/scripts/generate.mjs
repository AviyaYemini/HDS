// Generates src/schema.d.ts from the API's OpenAPI document (Constitution V).
//
// The document is exported from the FastAPI app itself (apps/api, `hds.scripts.export_openapi`),
// so the types always match the running code. With --check nothing is written: the script fails
// when the committed copy differs, which is what CI runs.
import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import openapiTS, { astToString } from "openapi-typescript";

const here = dirname(fileURLToPath(import.meta.url));
const api = resolve(here, "../../../apps/api");
const target = resolve(here, "../src/schema.d.ts");
const check = process.argv.includes("--check");

const document = execFileSync("uv", ["run", "python", "-m", "hds.scripts.export_openapi"], {
  cwd: api,
  encoding: "utf8",
  env: {
    ...process.env,
    // The export only builds the app's schema; it reads no secret and opens no connection.
    ENVIRONMENT: process.env.ENVIRONMENT ?? "test",
    JWT_SECRET_KEY:
      process.env.JWT_SECRET_KEY ?? "schema-export-only-not-a-secret-0123456789",
  },
});
const header =
  "// Generated from the HDS OpenAPI document by packages/api-client/scripts/generate.mjs.\n" +
  "// Do not edit: run `pnpm api-client:generate` after changing the API.\n\n";
const generated = header + astToString(await openapiTS(JSON.parse(document)));

if (check) {
  const committed = readFileSync(target, "utf8");
  if (committed !== generated) {
    console.error("packages/api-client/src/schema.d.ts is out of date: run `pnpm api-client:generate`.");
    process.exit(1);
  }
  console.log("Generated API client is up to date.");
} else {
  writeFileSync(target, generated);
  console.log(`Wrote ${target}`);
}
