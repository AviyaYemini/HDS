/**
 * Write (or check) `src/design/theme/theme.generated.ts` from the DTCG token files
 * (specs/007-ux-redesign task T016).
 *
 *   node scripts/build-theme.ts            write the file (pnpm theme:build)
 *   node scripts/build-theme.ts --check    exit 1 when the committed file differs (pnpm theme:check)
 *
 * Runs on Node 22+ with its built-in type stripping, so no build step is needed.
 */
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { generateTheme } from "../src/design/theme/generate.ts";

const here = dirname(fileURLToPath(import.meta.url));
const tokens = join(here, "../src/design/tokens");
const target = join(here, "../src/design/theme/theme.generated.ts");

const read = (name: string): Record<string, unknown> =>
  JSON.parse(readFileSync(join(tokens, `${name}.tokens.json`), "utf8")) as Record<string, unknown>;

const expected = generateTheme({ base: read("base"), semantic: read("semantic"), component: read("component") });

if (process.argv.includes("--check")) {
  let current = "";
  try {
    current = readFileSync(target, "utf8");
  } catch {
    // Missing counts as drift.
  }
  if (current !== expected) {
    console.error("theme.generated.ts is out of date: run `pnpm theme:build` and commit the result.");
    process.exit(1);
  }
  console.log("theme.generated.ts matches the tokens.");
} else {
  writeFileSync(target, expected);
  console.log(`wrote ${target}`);
}
