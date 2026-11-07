/**
 * Every capability the UI asks for exists in the API's contract (specs/007-ux-redesign FR-001,
 * T031): the names in the generated client's `Capability` schema equal the table in
 * `contracts/capabilities.md`, and every name the screens pass to `Gate`, `RequireCapability`,
 * `has` or `hasOnProject` is one of them. TypeScript already refuses an unknown name typed as a
 * `Capability`; this test also catches one hidden behind a cast or a plain string.
 */
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";

const ROOT = join(__dirname, "../..");

/** The names in the generated schema's `Capability` union. */
export function schemaCapabilities(schema: string): string[] {
  const match = /^\s*Capability: ((?:"[^"]+"(?: \| )?)+);$/m.exec(schema);
  if (!match) throw new Error("no Capability schema in the generated client");
  return [...match[1]!.matchAll(/"([^"]+)"/g)].map((m) => m[1]!);
}

/**
 * The names in the first column of contracts/capabilities.md's table; a cell may hold several, as
 * the API's own test reads them (`apps/api/tests/unit/test_capabilities.py`).
 */
export function tableCapabilities(markdown: string): string[] {
  return [...markdown.matchAll(/^\| (`[^|]+)\|/gm)].flatMap((row) =>
    [...row[1]!.matchAll(/`([a-z_.:]+)`/g)].map((m) => m[1]!),
  );
}

/** The capability names a source file asks for. */
export function usedCapabilities(source: string): string[] {
  const asked = /\b(?:need=\{?|has\(|hasOnProject\()\s*["'`]([a-z_]+\.[a-z_.:]+)["'`]/g;
  return [...source.matchAll(asked)].map((m) => m[1]!);
}

function sources(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) return sources(path);
    return /\.tsx?$/.test(name) ? [path] : [];
  });
}

const schema = schemaCapabilities(readFileSync(join(ROOT, "../../packages/api-client/src/schema.d.ts"), "utf8"));

test("the generated schema lists exactly the capabilities in the contract's table", () => {
  const table = tableCapabilities(readFileSync(join(ROOT, "../../specs/007-ux-redesign/contracts/capabilities.md"), "utf8"));
  expect(table.length).toBeGreaterThan(0);
  expect([...schema].sort()).toEqual([...table].sort());
});

test("every capability the UI asks for is in the schema", () => {
  const unknown: string[] = [];
  for (const file of [...sources(join(ROOT, "src")), ...sources(join(ROOT, "app"))]) {
    for (const name of usedCapabilities(readFileSync(file, "utf8"))) {
      if (!schema.includes(name)) unknown.push(`${file.slice(ROOT.length + 1)}: ${name}`);
    }
  }
  expect(unknown).toEqual([]);
});

test("the scan finds the names however they are asked for", () => {
  const source = `<Gate need="roster.read" /> <RequireCapability need={"staff.delete"} />
    caps.has("my_work.read"); caps.hasOnProject('roster.edit:project', id); has(\`pay.read_others\`)`;
  expect(usedCapabilities(source)).toEqual([
    "roster.read",
    "staff.delete",
    "my_work.read",
    "roster.edit:project",
    "pay.read_others",
  ]);
});
