/**
 * WCAG contrast over the design tokens (specs/007-ux-redesign FR-062, research R3).
 *
 * Reads the DTCG semantic tier, resolves each `{palette.…}` reference against the base tier, and
 * pairs every text and icon token with each background its `$extensions.hds.on` list names, in the
 * same theme. {@link contrastFailures} returns every pair under its theme's minimum, so a test can
 * require an empty list.
 */

type Json = { [key: string]: unknown };

/** One token pair that misses its theme's minimum ratio. */
export interface ContrastFailure {
  theme: string;
  token: string;
  on: string;
  ratio: number;
  minimum: number;
}

/** The relative luminance of `#RRGGBB` (WCAG 2.x). */
export function luminance(hex: string): number {
  const linear = (i: number): number => {
    const c = Number.parseInt(hex.slice(i, i + 2), 16) / 255;
    return c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;
  };
  return 0.2126 * linear(1) + 0.7152 * linear(3) + 0.0722 * linear(5);
}

/** The contrast ratio of two `#RRGGBB` colours, from 1 to 21. */
export function contrastRatio(a: string, b: string): number {
  const [x, y] = [luminance(a), luminance(b)];
  return (Math.max(x, y) + 0.05) / (Math.min(x, y) + 0.05);
}

function at(tree: Json, path: string): Json | undefined {
  let node: unknown = tree;
  for (const part of path.split(".")) {
    if (typeof node !== "object" || node === null) return undefined;
    node = (node as Json)[part];
  }
  return node as Json | undefined;
}

/** The hex value of a colour token, following `{…}` references into the base tier. */
export function resolveHex(token: Json, base: Json, theme: Json): string {
  let current: Json | undefined = token;
  for (let hops = 0; hops < 8 && current; hops += 1) {
    const value: unknown = current.$value;
    if (typeof value === "string" && value.startsWith("{")) {
      const path: string = value.slice(1, -1);
      current = at(base, path) ?? at(theme, path);
    } else if (value && typeof value === "object" && "hex" in value) {
      return String((value as Json).hex).toUpperCase();
    } else {
      break;
    }
  }
  throw new Error(`token does not resolve to a colour: ${JSON.stringify(token)}`);
}

function* tokensWithBackgrounds(tree: Json, prefix = ""): Generator<[string, Json]> {
  for (const [key, value] of Object.entries(tree)) {
    if (key.startsWith("$") || typeof value !== "object" || value === null) continue;
    const path = prefix ? `${prefix}.${key}` : key;
    const node = value as Json;
    const hds = (node.$extensions as Json | undefined)?.hds as Json | undefined;
    if ("$value" in node && Array.isArray(hds?.on)) yield [path, node];
    else yield* tokensWithBackgrounds(node, path);
  }
}

/** Every text and icon pair in `semantic` under its theme's minimum ratio. */
export function contrastFailures(semantic: Json, base: Json): ContrastFailure[] {
  const failures: ContrastFailure[] = [];
  const minimums = ((semantic.$extensions as Json).hds as Json).minimum as Json;
  for (const [themeName, themeValue] of Object.entries(semantic.theme as Json)) {
    const theme = themeValue as Json;
    const minimum = minimums[themeName] as { text: number; icon: number } | undefined;
    if (!minimum) throw new Error(`no contrast minimum for theme ${themeName}`);
    for (const [path, token] of tokensWithBackgrounds(theme)) {
      const hds = (token.$extensions as Json).hds as { on: string[]; kind: "text" | "icon" };
      const fg = resolveHex(token, base, theme);
      for (const on of hds.on) {
        const background = at(theme, on);
        if (!background) throw new Error(`${themeName}.${path} names a missing background ${on}`);
        const ratio = contrastRatio(fg, resolveHex(background, base, theme));
        const needed = minimum[hds.kind];
        if (ratio < needed) failures.push({ theme: themeName, token: path, on, ratio, minimum: needed });
      }
    }
  }
  return failures;
}
